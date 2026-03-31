package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"
)

type BitCask struct {
	currentdatafile *os.File
	pastDataFiles   map[int64]*os.File
	keyDir          map[string]KeyDirEntry
	currentFileID   int64
	currentFileSize int64
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
	bs.currentFileSize = 0

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

	bytesWritten := len(buf.Bytes())
	_, err = bs.currentdatafile.Write(buf.Bytes())
	if err != nil {
		return err
	}

	bs.currentFileSize += int64(bytesWritten)

	if bs.currentFileSize > int64(threshold) {
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

func (bs *BitCask) Merge() error {
	fileID := bs.currentFileID + 1
	filename := fmt.Sprintf("%d.db", fileID)

	mergefile, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)

	if err != nil {
		return err
	}

	for index, _ := range bs.keyDir {
		offset, _ := mergefile.Seek(0, io.SeekCurrent)

		value, err := bs.Get(index)
		if err != nil {
			return err
		}
		keySize := int64(len(index))
		valueSize := int64(len(value))

		buf := new(bytes.Buffer)
		timestamp := time.Now().UnixMilli()
		binary.Write(buf, binary.BigEndian, timestamp)
		binary.Write(buf, binary.BigEndian, keySize)
		binary.Write(buf, binary.BigEndian, valueSize)
		buf.Write([]byte(index))
		buf.Write(value)
		_, err = mergefile.Write(buf.Bytes())

		if err != nil {
			return err
		}

		valueOffset := offset + 24 + keySize
		bs.keyDir[string(index)] = KeyDirEntry{
			fileID:    fileID,
			offset:    valueOffset,
			valueSize: int64(valueSize),
		}

	}

	for id, file := range bs.pastDataFiles {
		file.Close()
		os.Remove(fmt.Sprintf("%d.db", id))
		delete(bs.pastDataFiles, id)
	}
	oldID := bs.currentFileID
	bs.currentdatafile.Close()
	os.Remove(fmt.Sprintf("%d.db", oldID))

	bs.currentFileID = fileID
	bs.currentdatafile = mergefile

	return nil
}

func Open() (*BitCask, error) {
	bc := &BitCask{
		pastDataFiles: make(map[int64]*os.File),
		keyDir:        make(map[string]KeyDirEntry),
	}

	files, _ := os.ReadDir("./")
	var fileIDs []int64

	for _, f := range files {
		var id int64
		if _, err := fmt.Sscanf(f.Name(), "%d.db", &id); err == nil {
			fileIDs = append(fileIDs, id)
		}
	}

	sort.Slice(fileIDs, func(i, j int) bool {
		return fileIDs[i] < fileIDs[j]
	})

	var lastID int64 = -1
	for _, id := range fileIDs {
		file, _ := os.OpenFile(fmt.Sprintf("%d.db", id), os.O_RDWR, 0644)
		lastID = id
		bc.rebuildKeyDir(id, file)

		if id == fileIDs[len(fileIDs)-1] {
			bc.currentdatafile = file
		} else {
			file.Close()
			bc.pastDataFiles[id] = file
		}
	}

	if lastID == -1 {
		bc.currentFileID = 0
		bc.createFile()
	} else {
		bc.currentFileID = lastID
	}

	return bc, nil
}

func main() {
	bc, err := Open()
	if err != nil {
		panic(err)
	}

	for {
		fmt.Print("> ")
		reader := bufio.NewReader(os.Stdin)
		input, _ := reader.ReadString('\n')

		input = strings.TrimSpace(input)

		if err == io.EOF && input == "" {
			fmt.Println("\nEOF")
			break
		}

		if input == "" {
			continue
		}

		parts := strings.Split(input, " ")
		cmd := parts[0]

		if cmd == "GET" {
			if len(parts) < 2 {
				fmt.Println("Need a key to fetch")
				continue
			}
			value, err := bc.Get(parts[1])
			if err != nil {
				fmt.Println(err)
			} else {
				fmt.Println(string(value))
			}

		} else if cmd == "SET" {
			if len(parts) < 3 {
				fmt.Println("Need a key and value to set")
				continue
			}
			resp := bc.Set(parts[1], []byte(parts[2]))
			if resp != nil {
				fmt.Println(resp)
			} else {
				fmt.Println("OK")
			}
		} else if cmd == "DELETE" {
			if len(parts) < 2 {
				fmt.Println("Invalid number of arguement")
				continue
			}
			_ = bc.Delete(parts[1])
			fmt.Println("OK")
		} else if cmd == "MERGE" {
			fmt.Println("Starting background merge")
			err := bc.Merge()
			if err != nil {
				fmt.Println("Merge failed:", err)
			} else {
				fmt.Println("Merge complete!")
			}
		}
	}
}
