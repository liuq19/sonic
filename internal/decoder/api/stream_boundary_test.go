/*
 * Copyright 2021 ByteDance Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/require"
)

type streamReadStep struct {
	data string
	err  error
}

type streamStepReader struct {
	steps []streamReadStep
	reads int
	tail  error
}

func (r *streamStepReader) Read(p []byte) (int, error) {
	r.reads++
	if len(r.steps) == 0 {
		return 0, r.tail
	}
	s := &r.steps[0]
	n := copy(p, s.data)
	s.data = s.data[n:]
	if len(s.data) != 0 {
		return n, nil
	}
	err := s.err
	r.steps = r.steps[1:]
	return n, err
}

func TestStreamNumberReadBoundaries(t *testing.T) {
	inputs := []string{
		"0", "42", "-7", "-0.25", "1.5", "1e3", "1e-3", "1234567890",
		" \t42 \n 43  ", "42 true", "42 {}", "42 [1,2]", `{"n":42} 1.5`,
		"01", "1-2", "42true", "42{}",
	}
	for _, size := range []int{16, 32, 64} {
		inputs = append(inputs, "42"+strings.Repeat(" ", size)+"43", strings.Repeat("1", size)+" 2")
	}
	for _, input := range inputs {
		check := func(t *testing.T, reader func() io.Reader) {
			want := json.NewDecoder(reader())
			got := NewStreamDecoder(reader())
			want.UseNumber()
			got.UseNumber()
			for i := 0; i <= len(input); i++ {
				var expected, actual interface{}
				err := want.Decode(&expected)
				if err == io.EOF {
					require.Equal(t, io.EOF, got.Decode(&actual))
					return
				}
				require.NoError(t, err)
				require.NoError(t, got.Decode(&actual))
				require.Equal(t, expected, actual)
				require.Equal(t, want.InputOffset(), got.InputOffset())
				require.NotPanics(t, func() {
					_, err := io.ReadAll(got.Buffered())
					require.NoError(t, err)
				})
			}
			t.Fatal("decoder did not reach EOF")
		}
		t.Run(input+"/one-byte", func(t *testing.T) {
			check(t, func() io.Reader { return iotest.OneByteReader(strings.NewReader(input)) })
		})
		for split := 1; split < len(input); split++ {
			t.Run(fmt.Sprintf("%s/split-%d", input, split), func(t *testing.T) {
				check(t, func() io.Reader {
					return io.MultiReader(strings.NewReader(input[:split]), strings.NewReader(input[split:]))
				})
			})
		}
	}
}

func TestStreamNumberStopsAtWhitespace(t *testing.T) {
	for _, space := range []string{" ", "\t", "\r", "\n", "  "} {
		t.Run(fmt.Sprintf("%q", space), func(t *testing.T) {
			readErr := errors.New("read after delimiter")
			r := &streamStepReader{steps: []streamReadStep{{data: "42"}, {data: space}}, tail: readErr}
			d := NewStreamDecoder(r)
			var got int
			require.NoError(t, d.Decode(&got))
			require.Equal(t, 42, got)
			require.Equal(t, 2, r.reads, "a delimiter must finish Decode without another read")
			require.Equal(t, int64(2), d.InputOffset())
			buffered, err := io.ReadAll(d.Buffered())
			require.NoError(t, err)
			require.Equal(t, space, string(buffered))
			require.Equal(t, readErr, d.Decode(&got))
		})
	}
}

func TestStreamNumberReadErrors(t *testing.T) {
	readErr := errors.New("number read failed")
	for _, tc := range []struct {
		name  string
		steps []streamReadStep
		reads int
	}{
		{"after-prefix", []streamReadStep{{data: "4"}, {data: "2"}}, 3},
		{"with-prefix", []streamReadStep{{data: "42", err: readErr}, {data: "3 "}}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &streamStepReader{steps: tc.steps, tail: readErr}
			d := NewStreamDecoder(r)
			got := 99
			require.Equal(t, readErr, d.Decode(&got))
			require.Equal(t, 99, got, "an unterminated prefix must not be committed")
			require.Equal(t, tc.reads, r.reads)
			require.Equal(t, readErr, d.Decode(&got))
			require.Equal(t, tc.reads, r.reads, "reader errors must remain sticky")
			require.NotPanics(t, func() { _ = d.Buffered() })
		})
	}
}

func TestStreamNumberDataAndError(t *testing.T) {
	for _, tc := range []struct {
		name string
		data string
		err  error
	}{
		{"eof", "42", io.EOF},
		{"delimiter-and-error", "42 ", errors.New("error after delimiter")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &streamStepReader{steps: []streamReadStep{{data: tc.data, err: tc.err}}, tail: io.EOF}
			d := NewStreamDecoder(r)
			var got int
			require.NoError(t, d.Decode(&got))
			require.Equal(t, 42, got)
			require.Equal(t, int64(2), d.InputOffset())
			require.Equal(t, 1, r.reads)
			require.NotPanics(t, func() {
				b, err := io.ReadAll(d.Buffered())
				require.NoError(t, err)
				require.Equal(t, tc.data[2:], string(b))
			})
			require.Equal(t, tc.err, d.Decode(&got))
			require.Equal(t, 1, r.reads, "do not read again after an error returned with data")
		})
	}
}

func TestStreamIncompleteNumber(t *testing.T) {
	for _, input := range []string{"-", "1.", "1e", "1e+", "1e-"} {
		t.Run(input, func(t *testing.T) {
			d := NewStreamDecoder(iotest.OneByteReader(strings.NewReader(input)))
			got := 99
			require.Error(t, d.Decode(&got))
			require.Equal(t, 99, got)
			require.NotPanics(t, func() { _ = d.Buffered() })
		})
	}
}

func FuzzStreamNumberFraming(f *testing.F) {
	for _, number := range []string{"0", "-0", "42", "1.5", "1e+3", "-1e-3", strings.Repeat("1", 64)} {
		f.Add(number, uint8(1))
	}
	f.Fuzz(func(t *testing.T, number string, boundary uint8) {
		if len(number) == 0 || len(number) > 64 || !isNumberStart(number[0]) {
			t.Skip()
		}
		var expected json.Number
		if err := json.Unmarshal([]byte(number), &expected); err != nil || expected.String() != number {
			t.Skip()
		}
		input := number + " 2"
		split := 1 + int(boundary)%len(number)
		r := io.MultiReader(strings.NewReader(input[:split]), strings.NewReader(input[split:]))
		d := NewStreamDecoder(r)
		d.UseNumber()
		var first, second interface{}
		require.NoError(t, d.Decode(&first))
		require.Equal(t, expected, first)
		require.Equal(t, int64(len(number)), d.InputOffset())
		require.NoError(t, d.Decode(&second))
		require.Equal(t, json.Number("2"), second)
		require.Equal(t, io.EOF, d.Decode(new(interface{})))
	})
}
