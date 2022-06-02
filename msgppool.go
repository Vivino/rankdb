package rankdb

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"bytes"
	"errors"
	"io"
	"sync"

	"github.com/tinylib/msgp/msgp"
)

type WriterMsgp struct {
	w *msgp.Writer
	b *bytes.Buffer
}

func NewWriterMsg() *WriterMsgp {
	v := writerMsgpPool.Get().(*WriterMsgp)
	v.b.Reset()
	v.w.Reset(v.b)
	return v
}

var writerMsgpPool = sync.Pool{
	New: func() interface{} {
		buf := bytes.NewBuffer(make([]byte, 0, 1024))
		return &WriterMsgp{
			w: msgp.NewWriter(buf),
			b: buf,
		}
	},
}

//nolint - versionExt is unused, but someone apparently thought it should be here anyway?
const (
	versionExt   = 0   // Reserved for future extensions.
	versionError = 255 // Error
)

// SetVersion will write a version number.
func (w WriterMsgp) SetVersion(v uint8) error {
	if v == versionError {
		return errors.New("attempted to save with version == versionError")
	}
	return w.w.WriteByte(v)
}

// Writer returns the msgpack writer.
func (w WriterMsgp) Writer() *msgp.Writer {
	return w.w
}

// ReplaceWriter replaces the writer
func (w WriterMsgp) ReplaceWriter(writer io.Writer) {
	w.w.Reset(writer)
}

// Buffer returns the buffer containing the encoded content.
// The encoder is flushed to buffer.
func (w WriterMsgp) Buffer() *bytes.Buffer {
	w.w.Flush()
	return w.b
}

// Flush the messagepack writer.
func (w WriterMsgp) Flush() {
	w.w.Flush()
}

// Call Close to signify you are done with serialization and you no longer
// need the data kept in the buffer. This will recycle the Writer.
// This may only be called once, otherwise races will occur.
func (w *WriterMsgp) Close() {
	w.w.Reset(nil)
	w.b.Reset()
	writerMsgpPool.Put(w)
}

// ReaderMsgp
type ReaderMsgp struct {
	r *msgp.Reader
}

func NewReaderMsgp(b []byte) *ReaderMsgp {
	v := readerMsgpPool.Get().(*ReaderMsgp)
	buf := bytes.NewReader(b)
	v.r.Reset(buf)
	return v
}

func NewReaderMsgpReader(r io.Reader) *ReaderMsgp {
	v := readerMsgpPool.Get().(*ReaderMsgp)
	v.r.Reset(r)
	return v
}

var readerMsgpPool = sync.Pool{
	New: func() interface{} {
		return &ReaderMsgp{
			r: msgp.NewReader(nil),
		}
	},
}

func (w ReaderMsgp) Reader() *msgp.Reader {
	return w.r
}

func (w ReaderMsgp) GetVersion() (v uint8) {
	b, err := w.r.ReadByte()
	if err != nil {
		return versionError
	}
	return b
}

// MsgpGetVersion reads the version and returns it from a byte slice.
func MsgpGetVersion(in []byte) (b []byte, v uint8) {
	v, in, err := msgp.ReadUint8Bytes(in)
	if err != nil {
		return in, versionError
	}
	return in, v
}

// Call Close to signify you are done with serialization.
// This will recycle the Reader.
// This may only be called once, otherwise races will occur.
func (w *ReaderMsgp) Close() {
	w.r.Reset(nil)
	readerMsgpPool.Put(w)
}
