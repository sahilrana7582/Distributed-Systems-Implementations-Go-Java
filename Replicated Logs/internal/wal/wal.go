package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// EntryType for future use (0 = user entry, 1 = config)
type EntryType uint8

const (
	EntryTypeNormal EntryType = 0
	EntryTypeConfig EntryType = 1
)

// Entry represents a WAL entry.
type Entry struct {
	Index uint64
	Term  uint64
	Type  EntryType
	Data  []byte
}

// Format on disk for each record:
//
// [len uint32][crc uint32][payload bytes]
//
// where payload = [index uint64][term uint64][type uint8][data bytes]
// len == len(payload)
//
// CRC is crc32.Checksum(payload) using IEEE polynomial.
// We write the full record (len+crc+payload) in a single Write call to reduce
// partial-write exposure, and we fsync after write to ensure durability.
//
// On startup we scan sequentially, validating CRC. If we encounter EOF or CRC
// mismatch we truncate the file to the last known-good offset.

var (
	ErrNotFound        = errors.New("wal: entry not found")
	ErrCorruptedRecord = errors.New("wal: corrupted record")
)

// WAL represents a simple file-backed WAL (single segment).
type WAL struct {
	dir      string
	filepath string
	f        *os.File

	mu sync.RWMutex
	// offsets stores file offset for entry with logical index (baseIndex+pos)
	// We'll store offsets in slice where offsets[0] corresponds to first entry scanned
	offsets []int64
	// firstIndex is the index of the first stored entry (if any). For simplicity we
	// expect monotonic increasing indexes starting at 1 normally.
	firstIndex uint64
	lastIndex  uint64
}

// New creates/opens WAL in dir with filename "log.wal".
func New(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	fp := filepath.Join(dir, "log.wal")
	f, err := os.OpenFile(fp, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	w := &WAL{
		dir:      dir,
		filepath: fp,
		f:        f,
		offsets:  make([]int64, 0),
	}
	if err := w.recover(); err != nil {
		f.Close()
		return nil, err
	}
	return w, nil
}

// Close closes the underlying file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	err := w.f.Close()
	w.f = nil
	return err
}

// LastIndex returns the last known index (0 if empty).
func (w *WAL) LastIndex() uint64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.lastIndex
}

// TermAt returns entry's term at index.
func (w *WAL) TermAt(index uint64) (uint64, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if index < w.firstIndex || index > w.lastIndex || w.firstIndex == 0 {
		return 0, false
	}
	pos := index - w.firstIndex
	off := w.offsets[pos]
	// read record at off to extract term quickly
	rec, err := w.readAtOffset(off)
	if err != nil {
		return 0, false
	}
	return rec.Term, true
}

// ReadAt returns the entry with given index.
func (w *WAL) ReadAt(index uint64) (Entry, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if index < w.firstIndex || index > w.lastIndex || w.firstIndex == 0 {
		return Entry{}, ErrNotFound
	}
	pos := index - w.firstIndex
	off := w.offsets[pos]
	return w.readAtOffset(off)
}

// Exists returns true if an entry with same index and term exists.
func (w *WAL) Exists(e Entry) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if e.Index < w.firstIndex || e.Index > w.lastIndex || w.firstIndex == 0 {
		return false
	}
	pos := e.Index - w.firstIndex
	recOff := w.offsets[pos]
	rec, err := w.readAtOffset(recOff)
	if err != nil {
		return false
	}
	return rec.Index == e.Index && rec.Term == e.Term
}

// Append writes an entry to the WAL, fsyncs the file, updates in-memory index and returns last index.
func (w *WAL) Append(e Entry) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	// ensure monotonic index
	if w.firstIndex == 0 {
		// empty WAL
		w.firstIndex = e.Index
	} else {
		if e.Index != w.lastIndex+1 {
			// caller should ensure monotonic index sequence; if not, we still allow appends
			// but this is unusual. For learning keep it strict.
			return w.lastIndex, errors.New("wal: non-monotonic append index")
		}
	}

	// serialize payload
	var buf bytes.Buffer
	// index
	if err := binary.Write(&buf, binary.LittleEndian, e.Index); err != nil {
		return w.lastIndex, err
	}
	// term
	if err := binary.Write(&buf, binary.LittleEndian, e.Term); err != nil {
		return w.lastIndex, err
	}
	// type
	if err := buf.WriteByte(byte(e.Type)); err != nil {
		return w.lastIndex, err
	}
	// data
	if len(e.Data) > 0 {
		if _, err := buf.Write(e.Data); err != nil {
			return w.lastIndex, err
		}
	}
	payload := buf.Bytes()
	crc := crc32.ChecksumIEEE(payload)

	// prepare record: [len uint32][crc uint32][payload]
	var record bytes.Buffer
	if err := binary.Write(&record, binary.LittleEndian, uint32(len(payload))); err != nil {
		return w.lastIndex, err
	}
	if err := binary.Write(&record, binary.LittleEndian, uint32(crc)); err != nil {
		return w.lastIndex, err
	}
	if _, err := record.Write(payload); err != nil {
		return w.lastIndex, err
	}

	// write atomically
	off, err := w.f.Seek(0, io.SeekEnd)
	if err != nil {
		return w.lastIndex, err
	}
	n, err := w.f.Write(record.Bytes())
	if err != nil {
		return w.lastIndex, err
	}
	// fsync to ensure durability
	if err := w.f.Sync(); err != nil {
		return w.lastIndex, err
	}

	// update in-memory index
	w.offsets = append(w.offsets, off)
	w.lastIndex = e.Index
	_ = n // consumed
	return w.lastIndex, nil
}

// TruncateFrom truncates file from given index (inclusive).
// If index is <= firstIndex, the WAL becomes empty (firstIndex/lastIndex reset).
func (w *WAL) TruncateFrom(index uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.firstIndex == 0 {
		// already empty
		return nil
	}
	if index > w.lastIndex {
		// nothing to do
		return nil
	}
	if index <= w.firstIndex {
		// truncate whole file
		if err := w.f.Truncate(0); err != nil {
			return err
		}
		if _, err := w.f.Seek(0, io.SeekStart); err != nil {
			return err
		}
		w.offsets = w.offsets[:0]
		w.firstIndex = 0
		w.lastIndex = 0
		return w.f.Sync()
	}
	// find offset to truncate to
	pos := index - w.firstIndex
	off := w.offsets[pos]
	if err := w.f.Truncate(off); err != nil {
		return err
	}
	if _, err := w.f.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	// trim offsets
	w.offsets = w.offsets[:pos]
	w.lastIndex = w.firstIndex + uint64(len(w.offsets)) - 1
	return w.f.Sync()
}

// Iterator returns a channel that yields entries from 'from' inclusive up to lastIndex.
func (w *WAL) Iterator(from uint64) (<-chan Entry, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	ch := make(chan Entry)
	if w.firstIndex == 0 || from > w.lastIndex {
		close(ch)
		return ch, nil
	}
	if from < w.firstIndex {
		from = w.firstIndex
	}
	go func() {
		for idx := from; idx <= w.lastIndex; idx++ {
			pos := idx - w.firstIndex
			off := w.offsets[pos]
			rec, err := w.readAtOffset(off)
			if err != nil {
				// stop on error
				break
			}
			ch <- rec
		}
		close(ch)
	}()
	return ch, nil
}

// internal: readAtOffset reads a single record starting at given offset and returns Entry.
func (w *WAL) readAtOffset(off int64) (Entry, error) {
	// read len(4) + crc(4)
	var header [8]byte
	if _, err := w.f.ReadAt(header[:], off); err != nil {
		return Entry{}, err
	}
	payloadLen := binary.LittleEndian.Uint32(header[0:4])
	crc := binary.LittleEndian.Uint32(header[4:8])

	payload := make([]byte, payloadLen)
	if _, err := w.f.ReadAt(payload, off+8); err != nil {
		return Entry{}, err
	}
	if crc != crc32.ChecksumIEEE(payload) {
		return Entry{}, ErrCorruptedRecord
	}
	// parse payload: index(uint64), term(uint64), type(uint8), data
	buf := bytes.NewReader(payload)
	var idx uint64
	var term uint64
	if err := binary.Read(buf, binary.LittleEndian, &idx); err != nil {
		return Entry{}, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &term); err != nil {
		return Entry{}, err
	}
	t, err := buf.ReadByte()
	if err != nil {
		return Entry{}, err
	}
	data, _ := io.ReadAll(buf) // remainder is data
	return Entry{Index: idx, Term: term, Type: EntryType(t), Data: data}, nil
}

// recover scans the file and builds offset index, truncating any partial/bad tail.
func (w *WAL) recover() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var offsets []int64
	var firstIdx uint64 = 0
	var lastIdx uint64 = 0

	f := w.f
	off := int64(0)
	for {
		// read header
		var header [8]byte
		n, err := f.ReadAt(header[:], off)
		if err != nil {
			if err == io.EOF || (err == io.ErrUnexpectedEOF && n == 0) {
				// clean EOF
				break
			}
			// partial read or error: truncate file to off and stop
			if err := f.Truncate(off); err != nil {
				return err
			}
			break
		}
		if n < 8 {
			// partial header: truncate and exit
			if err := f.Truncate(off); err != nil {
				return err
			}
			break
		}
		payloadLen := binary.LittleEndian.Uint32(header[0:4])
		crc := binary.LittleEndian.Uint32(header[4:8])

		// read payload
		payload := make([]byte, payloadLen)
		np, err := f.ReadAt(payload, off+8)
		if err != nil {
			// partial payload: truncate and stop
			if err := f.Truncate(off); err != nil {
				return err
			}
			break
		}
		if uint32(np) < payloadLen {
			if err := f.Truncate(off); err != nil {
				return err
			}
			break
		}
		// crc check
		if crc != crc32.ChecksumIEEE(payload) {
			// corrupted: truncate and stop
			if err := f.Truncate(off); err != nil {
				return err
			}
			break
		}
		// parse index to set lastIndex and firstIndex
		var idx uint64
		buf := bytes.NewReader(payload)
		if err := binary.Read(buf, binary.LittleEndian, &idx); err != nil {
			if err := f.Truncate(off); err != nil {
				return err
			}
			break
		}
		if firstIdx == 0 {
			firstIdx = idx
		}
		lastIdx = idx
		offsets = append(offsets, off)
		// advance offset: header(8) + payloadLen
		off += 8 + int64(payloadLen)
	}

	// if file was truncated above, ensure file.Sync
	if err := f.Sync(); err != nil {
		return err
	}

	// set struct fields
	w.offsets = offsets
	w.firstIndex = firstIdx
	w.lastIndex = lastIdx
	// ensure file pointer at end for appends
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	return nil
}
