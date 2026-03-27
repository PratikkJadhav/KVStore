package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

type BitCask struct {
	currentdatafile *os.File
	pastDataFiles   map[int64]*os.File
	keyDir          map[string]KeyDirEntry
	currentFileID   int64
}
type Entry struct {
	timestamp int64
	keySize   int64
	valueSize int64
	key       string
	value     []byte
}

type KeyDirEntry struct {
	fileID    int64
	offset    int64
	valueSize int64
}

var threshold = 10 * 1024 * 1024

func (bs *BitCask) createFile() (*os.File, error) {
	bs.currentFileID++
	filename := fmt.Sprintf("%d.db", bs.currentFileID)

	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)

	bs.currentdatafile = file

	return file, err
}

func (bs *BitCask) Set(key string, value []byte) error {

	offset, err := bs.currentdatafile.Seek(0, io.SeekEnd)
	valueOffset := offset + 8 + 8 + 8 + int64(len(key))
	if err != nil {
		return err
	}

	buf := new(bytes.Buffer)
	timestamp := time.Now().UnixMilli()
	binary.Write(buf, binary.BigEndian, timestamp)
	binary.Write(buf, binary.BigEndian, int64(len(key)))
	binary.Write(buf, binary.BigEndian, int64(len(value)))
	buf.Write([]byte(key))
	buf.Write(value)

	_, err = bs.currentdatafile.Write(buf.Bytes())

	if err != nil {
		return err
	}

	bs.keyDir[key] = KeyDirEntry{
		fileID:    bs.currentFileID,
		offset:    valueOffset,
		valueSize: int64(len(value)),
	}

	info, _ := bs.currentdatafile.Stat()
	if info.Size() > int64(threshold) {
		bs.pastDataFiles[bs.currentFileID] = bs.currentdatafile
		bs.createFile()
	}

	return nil

}

func (bs *BitCask) Get(key string) (value []byte, err error) {
	entry, ok := bs.keyDir[key]
	if !ok {
		return nil, fmt.Errorf("Key not found: %s", key)
	}

	fileID := entry.fileID
	offset := entry.offset
	size := entry.valueSize

	filename := fmt.Sprintf("%d.db", fileID)
	file, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	defer file.Close()

	if err != nil {
		return nil, err
	}

	file.Seek(offset, 0)

	buf := make([]byte, size)

	_, err = io.ReadFull(file, buf)
	if err != nil {
		return nil, err
	}

	return buf, nil

}

func (bs *BitCask) Delete(key string) error {
	file := bs.currentdatafile
	_, err := file.Seek(0, io.SeekEnd)

	if err != nil {
		return err
	}
	var valueSize = 0
	buf := new(bytes.Buffer)
	timestamp := time.Now().UnixMilli()
	binary.Write(buf, binary.BigEndian, timestamp)
	binary.Write(buf, binary.BigEndian, int64(len(key)))
	binary.Write(buf, binary.BigEndian, int64(valueSize))
	buf.Write([]byte(key))

	_, exists := bs.keyDir[key]
	if !exists {
		fmt.Println("Key does not exists")
		return nil
	}

	_, err = file.Write(buf.Bytes())

	if err != nil {
		return err
	}

	delete(bs.keyDir, key)

	info, _ := bs.currentdatafile.Stat()
	if info.Size() > int64(threshold) {
		bs.pastDataFiles[bs.currentFileID] = bs.currentdatafile
		bs.createFile()
	}

	return nil
}

func (bs *BitCask) rebuildKeyDir(fileID int64, file *os.File) error {
	file.Seek(0, io.SeekStart)

	for {
		offset, _ := file.Seek(0, io.SeekCurrent)

		var timestamp int64
		var valueSize int64
		var keySize int64
		err := binary.Read(file, binary.BigEndian, &timestamp)
		if err == io.EOF {
			break
		}
		err = binary.Read(file, binary.BigEndian, &keySize)
		if err == io.EOF {
			break
		}
		err = binary.Read(file, binary.BigEndian, &valueSize)
		if err == io.EOF {
			break
		}

		key := make([]byte, keySize)

		io.ReadFull(file, key)

		file.Seek(valueSize, io.SeekCurrent)

		valueOffset := offset + 8 + 8 + 8 + int64(keySize)

		if valueSize == 0 {
			delete(bs.keyDir, string(key))
		} else {
			bs.keyDir[string(key)] = KeyDirEntry{
				fileID:    fileID,
				offset:    valueOffset,
				valueSize: valueSize,
			}
		}

	}

	return nil

}

func Open() (*BitCask, error) {
	bc := &BitCask{
		pastDataFiles: make(map[int64]*os.File),
		keyDir:        make(map[string]KeyDirEntry),
	}

	files, _ := os.ReadDir("./")
	var lastID int64 = -1

	for _, f := range files {
		var id int64

		_, err := fmt.Sscanf(f.Name(), "%d.db", &id)
		if err == nil {
			file, _ := os.OpenFile(f.Name(), os.O_RDWR, 0644)

			bc.pastDataFiles[id] = file

			if id > lastID {
				lastID = id
			}

			bc.rebuildKeyDir(id, file)
		}

	}

	bc.currentFileID = lastID
	if lastID == -1 {
		bc.currentFileID = 0
		bc.createFile()
	} else {
		bc.currentFileID = lastID
		bc.currentdatafile = bc.pastDataFiles[lastID]

	}

	return bc, nil
}

func main() {
	bc, err := Open()
	if err != nil {
		panic(err)
	}

	for {
		reader := bufio.NewReader(os.Stdin)
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		parts := strings.Split(input, " ")
		cmd := parts[0]

		if cmd == "GET" {
			value, err := bc.Get(parts[1])
			if err != nil {
				fmt.Println(err)
			}

			fmt.Println(string(value))
		} else if cmd == "SET" {
			resp := bc.Set(parts[1], []byte(parts[2]))
			fmt.Println(resp)
		} else if cmd == "DELETE" {
			_ = bc.Delete(parts[1])
		}
	}
}
