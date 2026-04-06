package store

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
)

// FSDirectory is a file-system based Directory.
type FSDirectory struct {
	path string
}

func NewFSDirectory(path string) (*FSDirectory, error) {
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}
	return &FSDirectory{path: path}, nil
}

func (d *FSDirectory) CreateOutput(name string) (IndexOutput, error) {
	f, err := os.Create(filepath.Join(d.path, name))
	if err != nil {
		return nil, err
	}
	return &fsIndexOutput{file: f, buf: bufio.NewWriter(f)}, nil
}

func (d *FSDirectory) OpenInput(name string) (IndexInput, error) {
	f, err := os.Open(filepath.Join(d.path, name))
	if err != nil {
		return nil, err
	}
	return &fsIndexInput{file: f}, nil
}

func (d *FSDirectory) ListAll() ([]string, error) {
	entries, err := os.ReadDir(d.path)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names, nil
}

func (d *FSDirectory) DeleteFile(name string) error {
	return os.Remove(filepath.Join(d.path, name))
}

func (d *FSDirectory) FileExists(name string) bool {
	_, err := os.Stat(filepath.Join(d.path, name))
	return err == nil
}

func (d *FSDirectory) FilePath(name string) string {
	return filepath.Join(d.path, name)
}

func (d *FSDirectory) Sync(names []string) error {
	for _, name := range names {
		f, err := os.OpenFile(filepath.Join(d.path, name), os.O_RDONLY, 0)
		if err != nil {
			return err
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}
		f.Close()
	}
	return nil
}

func (d *FSDirectory) SyncMetaData() error {
	dir, err := os.Open(d.path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func (d *FSDirectory) Rename(source, dest string) error {
	return os.Rename(filepath.Join(d.path, source), filepath.Join(d.path, dest))
}

// --- IndexOutput ---

type fsIndexOutput struct {
	file *os.File
	buf  *bufio.Writer
}

func (o *fsIndexOutput) Write(p []byte) (int, error) {
	return o.buf.Write(p)
}

func (o *fsIndexOutput) WriteVInt(v int) error {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], uint64(v))
	_, err := o.buf.Write(buf[:n])
	return err
}

func (o *fsIndexOutput) WriteUint16(v uint16) error {
	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], v)
	_, err := o.buf.Write(buf[:])
	return err
}

func (o *fsIndexOutput) WriteUint32(v uint32) error {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], v)
	_, err := o.buf.Write(buf[:])
	return err
}

func (o *fsIndexOutput) WriteUint64(v uint64) error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], v)
	_, err := o.buf.Write(buf[:])
	return err
}

func (o *fsIndexOutput) Close() error {
	if err := o.buf.Flush(); err != nil {
		return err
	}
	return o.file.Close()
}

// --- IndexInput ---

type fsIndexInput struct {
	file *os.File
}

func (in *fsIndexInput) Read(p []byte) (int, error) {
	return in.file.Read(p)
}

func (in *fsIndexInput) ReadVInt() (int, error) {
	val, err := binary.ReadUvarint(newByteReader(in.file))
	return int(val), err
}

func (in *fsIndexInput) Close() error {
	return in.file.Close()
}

// byteReader adapts io.Reader to io.ByteReader.
type byteReader struct {
	r   io.Reader
	buf [1]byte
}

func newByteReader(r io.Reader) *byteReader {
	return &byteReader{r: r}
}

func (br *byteReader) ReadByte() (byte, error) {
	_, err := br.r.Read(br.buf[:])
	return br.buf[0], err
}
