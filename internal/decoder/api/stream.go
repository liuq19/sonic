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
	"bytes"
	"io"
	"sync"

	"github.com/bytedance/sonic/internal/native"
	"github.com/bytedance/sonic/internal/native/types"
	"github.com/bytedance/sonic/internal/rt"
	"github.com/bytedance/sonic/internal/utils"
	"github.com/bytedance/sonic/option"
)

var (
	minLeftBufferShift uint = 1
)

// StreamDecoder is the decoder context object for streaming input.
type StreamDecoder struct {
	r       io.Reader
	buf     []byte
	scanp   int
	scanned int64
	err     error
	readErr error
	Decoder
}

var bufPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 0, option.DefaultDecoderBufferSize)
	},
}

func freeBytes(buf []byte) {
	if cap(buf) != 0 && rt.CanSizeResue(cap(buf)) {
		bufPool.Put(buf[:0])
	}
}

// NewStreamDecoder adapts to encoding/json.NewDecoder API.
//
// NewStreamDecoder returns a new decoder that reads from r.
func NewStreamDecoder(r io.Reader) *StreamDecoder {
	return &StreamDecoder{r: r}
}

// Decode decodes input stream into val with corresponding data.
// Redundantly bytes may be read and left in its buffer, and can be used at next call.
// Either io error from underlying io.Reader (except io.EOF)
// or syntax error from data will be recorded and stop subsequently decoding.
func (self *StreamDecoder) Decode(val interface{}) (err error) {
	if !self.More() {
		return self.err
	}

	s := self.scanp
	number := isNumberStart(self.buf[s])
	var e int
	for {
		src := rt.Mem2Str(self.buf[s:])
		x := 0
		y := 0
		if number {
			x = streamNumberEnd(self.buf[s:])
		} else {
			y = native.SkipOneFast(&src, &x)
		}
		if y >= 0 {
			e = s + x
			// At a read boundary, a valid number may still gain more digits.
			// A delimiter or EOF lets the decoder validate the complete token.
			if e < len(self.buf) || !number || self.readErr == io.EOF {
				s += y
				break
			}
		} else if types.ParsingError(-y) != types.ERR_EOF {
			err = SyntaxError{x, string(self.buf[s:]), types.ParsingError(-y), ""}
			self.setErr(err)
			return err
		}

		if self.readErr != nil {
			err = self.readErr
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			self.setErr(err)
			return err
		}
		self.readMore()
	}

	// Keep decoded strings independent of the pooled read buffer.
	self.Decoder.Reset(string(self.buf[s:e]))
	if err = self.Decoder.Decode(val); err != nil {
		self.setErr(err)
		return err
	}
	self.scanp = e
	if e == len(self.buf) {
		self.scanned += int64(e)
		self.scanp = 0
		mem := self.buf
		self.buf = nil
		freeBytes(mem)
	}
	return nil
}

// InputOffset returns the input stream byte offset of the current decoder position.
// The offset gives the location of the end of the most recently returned token and the beginning of the next token.
func (self *StreamDecoder) InputOffset() int64 {
	return self.scanned + int64(self.scanp)
}

// Buffered returns a reader of the data remaining in the Decoder's buffer.
// The reader is valid until the next call to Decode.
func (self *StreamDecoder) Buffered() io.Reader {
	return bytes.NewReader(self.buf[self.scanp:])
}

// More reports whether there is another element in the
// current array or object being parsed.
func (self *StreamDecoder) More() bool {
	if self.err != nil {
		return false
	}
	c, err := self.peek()
	return err == nil && c != ']' && c != '}'
}

// readMore appends bytes without consuming or releasing the current token.
// An error returned with data is saved until those bytes have been examined.
func (self *StreamDecoder) readMore() error {
	if self.readErr != nil {
		return self.readErr
	}
	l := len(self.buf)
	realloc(&self.buf)
	n, err := self.r.Read(self.buf[l:cap(self.buf)])
	self.buf = self.buf[:l+n]
	self.readErr = err
	return err
}

func (self *StreamDecoder) setErr(err error) {
	self.err = err
	self.scanned += int64(self.scanp)
	self.scanp = 0
	mem := self.buf[:0]
	self.buf = nil
	freeBytes(mem)
}

func (self *StreamDecoder) peek() (byte, error) {
	var err error
	for {
		c, empty := self.scan()
		if !empty {
			return byte(c), nil
		}
		// buffer has been scanned, now report any error
		if err != nil {
			self.setErr(err)
			return 0, err
		}
		err = self.refill()
	}
}

func (self *StreamDecoder) scan() (byte, bool) {
	for i := self.scanp; i < len(self.buf); i++ {
		c := self.buf[i]
		if utils.IsSpace(c) {
			continue
		}
		self.scanp = i
		return c, false
	}
	return 0, true
}

func (self *StreamDecoder) refill() error {
	// Make room to read more into the buffer.
	// First slide down data already consumed.
	if self.scanp > 0 {
		self.scanned += int64(self.scanp)
		n := copy(self.buf, self.buf[self.scanp:])
		self.buf = self.buf[:n]
		self.scanp = 0
	}

	return self.readMore()
}

func isNumberStart(c byte) bool {
	return c == '-' || (c >= '0' && c <= '9')
}

// streamNumberEnd locates a numeric token without reading into the next value.
// Syntax validation remains with Decoder.Decode, including incomplete fractions
// or exponents at EOF. src starts with a digit or '-'.
func streamNumberEnd(src []byte) int {
	i := 0
	if src[i] == '-' {
		i++
	}
	if i < len(src) && src[i] == '0' {
		i++
	} else {
		i = streamDigitsEnd(src, i)
	}
	if i < len(src) && src[i] == '.' {
		i = streamDigitsEnd(src, i+1)
	}
	if i < len(src) && (src[i] == 'e' || src[i] == 'E') {
		i++
		if i < len(src) && (src[i] == '+' || src[i] == '-') {
			i++
		}
		i = streamDigitsEnd(src, i)
	}
	return i
}

func streamDigitsEnd(src []byte, i int) int {
	for i < len(src) && src[i] >= '0' && src[i] <= '9' {
		i++
	}
	return i
}

func realloc(buf *[]byte) bool {
	l := uint(len(*buf))
	c := uint(cap(*buf))
	if c == 0 {
		*buf = bufPool.Get().([]byte)
		return true
	}
	if c-l <= c>>minLeftBufferShift {
		e := l + (l >> minLeftBufferShift)
		if e <= c {
			e = c * 2
		}
		tmp := make([]byte, l, e)
		copy(tmp, *buf)
		*buf = tmp
		return true
	}
	return false
}
