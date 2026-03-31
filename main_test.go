package main

import (
	"bytes"
	"fmt"
	"testing"
)

func TestBitCaskLifecycle(t *testing.T) {
	// 1. Boot up the database
	bc, err := Open()
	if err != nil {
		t.Fatal(err)
	}

	// 2. Temporarily lower threshold to 1MB so we hit it instantly
	threshold = 1 * 1024 * 1024

	t.Log("Threshold set to 1MB. Flooding DB with keys...")

	// 3. Write 2000 records (Overwriting 100 unique keys 20 times)
	// Using a 1KB value to easily blow past the 1MB threshold
	val := bytes.Repeat([]byte("X"), 1024)
	for i := 0; i < 2000; i++ {
		key := fmt.Sprintf("user_%d", i%100)
		err := bc.Set(key, val)
		if err != nil {
			t.Fatal(err)
		}
	}

	// 4. Check if your file rotation logic worked
	if len(bc.pastDataFiles) == 0 {
		t.Fatal("FAIL: No past files were created! File rotation threshold logic is broken.")
	}
	t.Logf("SUCCESS: Crossed threshold. Created %d old .db files.", len(bc.pastDataFiles))

	// 5. Test the Tombstone / Delete logic before merging
	t.Log("Deleting user_50...")
	bc.Delete("user_50")

	// 6. Test the Merge compaction
	t.Log("Starting background merge...")
	err = bc.Merge()
	if err != nil {
		t.Fatal("FAIL: Merge crashed:", err)
	}

	// 7. Verify the data actually survived the merge
	t.Log("Merge complete. Verifying data integrity...")

	_, err = bc.Get("user_1")
	if err != nil {
		t.Fatal("FAIL: Lost user_1 after merge. Data corrupted!")
	}

	_, err = bc.Get("user_50")
	if err == nil {
		t.Fatal("FAIL: user_50 still exists! Tombstone deletion failed during merge.")
	}

	t.Log("SUCCESS: All tests passed! File rotation, compaction, and tombstones are working perfectly.")
}
