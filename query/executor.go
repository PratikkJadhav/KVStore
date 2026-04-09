package query

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	main "github.com/PratikkJadhav/KVStore.git/bitcask"
)

type Executor struct {
	db *main.BitCask
}

func (e *Executor) executeInsert(stmt *InsertStmt) (string, error) {
	// 1. Find or generate ID
	id := ""
	for i, col := range stmt.Columns {
		if col == "id" {
			id = stmt.Values[i]
			break
		}
	}

	if id == "" {
		var err error
		id, err = e.nextID(stmt.Table)
		if err != nil {
			return "", err
		}
	}

	key := fmt.Sprintf("%s:%s", stmt.Table, id)

	// 2. Check if it already exists
	_, err := e.db.Get(key)
	if err == nil {
		return "", fmt.Errorf("row with id '%s' already exists in table '%s'. Use UPSERT to modify", id, stmt.Table)
	}

	// 3. Zip columns and values together
	data := make(map[string]string)
	for i, col := range stmt.Columns {
		data[col] = stmt.Values[i]
	}
	data["id"] = id // Ensure ID is always saved in the JSON

	// 4. Encode and save
	bytes, err := encode(data)
	if err != nil {
		return "", err
	}

	err = e.db.Set(key, bytes)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("INSERT OK. ID: %s", id), nil
}

func (e *Executor) executeUpsert(stmt *UpsertStmt) (string, error) {
	id := ""
	for i, col := range stmt.Columns {
		if col == "id" {
			id = stmt.Values[i]
			break
		}
	}

	if id == "" {
		// If they didn't provide an ID to update, generate one (acting like a normal insert)
		var err error
		id, err = e.nextID(stmt.Table)
		if err != nil {
			return "", err
		}
	}

	key := fmt.Sprintf("%s:%s", stmt.Table, id)

	// 1. Try to fetch existing data to preserve fields that aren't being updated
	data := make(map[string]string)
	existingBytes, err := e.db.Get(key)
	if err == nil {
		data, _ = decode(existingBytes)
	}

	// 2. Overwrite with the new values
	for i, col := range stmt.Columns {
		data[col] = stmt.Values[i]
	}
	data["id"] = id

	// 3. Encode and save
	bytes, err := encode(data)
	if err != nil {
		return "", err
	}

	err = e.db.Set(key, bytes)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("UPSERT OK. ID: %s", id), nil
}
func (e *Executor) executeSelect(stmt *SelectStmt) (string, error) {
	if stmt.ID != nil {
		// Point Read: Fast O(1) lookup
		key := fmt.Sprintf("%s:%s", stmt.Table, *stmt.ID)
		bytes, err := e.db.Get(key)
		if err != nil {
			return "", fmt.Errorf("no records found")
		}

		data, _ := decode(bytes)
		// Format single result nicely
		res, _ := json.MarshalIndent(data, "", "  ")
		return string(res), nil
	}

	// Full Table Scan
	keys := e.db.Keys()
	prefix := stmt.Table + ":"
	var results []map[string]string

	for _, k := range keys {
		// Only grab keys for this table, and ignore the sequence tracker
		if strings.HasPrefix(k, prefix) && !strings.HasSuffix(k, ":__seq") {
			bytes, err := e.db.Get(k)
			if err == nil {
				data, _ := decode(bytes)
				results = append(results, data)
			}
		}
	}

	if len(results) == 0 {
		return "[]", nil
	}

	// Format array of results
	resBytes, _ := json.MarshalIndent(results, "", "  ")
	return string(resBytes), nil
}

func (e *Executor) executeDelete(stmt *DeleteStmt) (string, error) {
	key := fmt.Sprintf("%s:%s", stmt.Table, stmt.ID)
	err := e.db.Delete(key)
	if err != nil {
		return "", fmt.Errorf("failed to delete or record not found: %v", err)
	}
	return "DELETE OK", nil
}

func (e *Executor) nextID(table string) (string, error) {
	seqKey := fmt.Sprintf("%s:__seq", table)
	bytes, err := e.db.Get(seqKey)

	seq := 1
	if err == nil {
		parsed, err := strconv.Atoi(string(bytes))
		if err == nil {
			seq = parsed + 1
		}
	}

	err = e.db.Set(seqKey, []byte(strconv.Itoa(seq)))
	return strconv.Itoa(seq), err
}

func (e *Executor) Execute(stmt Statement) (string, error) {
	switch s := stmt.(type) {
	case *InsertStmt:
		return e.executeInsert(s)
	case *SelectStmt:
		return e.executeSelect(s)
	case *DeleteStmt:
		return e.executeDelete(s)
	case *UpsertStmt:
		return e.executeUpsert(s)
	default:
		return "", fmt.Errorf("unknown statement type")
	}
}

func NewExecutor(db *main.BitCask) *Executor {
	return &Executor{db: db}
}

func encode(data map[string]string) ([]byte, error) {
	return json.Marshal(data)
}

func decode(b []byte) (map[string]string, error) {
	var data map[string]string
	err := json.Unmarshal(b, &data)
	return data, err
}
