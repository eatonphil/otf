package main

import (
	"testing"
)

func TestConcurrentTableWriter(t *testing.T) {
}

func TestConcurrentReaderWithWriterReadsSnapshot(t *testing.T) {
	dir, err := os.CreateTemp("", "test-database")
	if err != nil { panic(err) }

	fos := newFileObjectStorage(dir.Name())
	c1Writer := newClient(fos)
	c2Reader := newClient(fos)

	// First create some data and commit the transaction.
	err := c1Writer.newTx()
	assertEq(err, nil, "could not start first c1 tx")
	err = c1Writer.createTable("x", []string{"a", "b"})
	assertEq(err, nil, "could not create x")
	err = c1Writer.writeRow("x", []any{"Joey", 1})
	assertEq(err, nil, "could not write first row")
	err = c1Writer.writeRow("x", []any{"Yue", 2})
	assertEq(err, nil, "could not write second row")
	err := c1Writer.commitTx()
	assertEq(err, nil, "could not commit tx")

	// Now start a new transaction for more edits.
	err := c1Writer.newTx()
	assertEq(err, nil, "could not start second c1 tx")

	// Before we commit this second write-transaction, start a
	// read transaction.
	err := c2Reader.newTx()
	assertEq(err, nil, "could not start c2 tx")

	// Write and commit rows in c1.
	err = c1Writer.writeRow("x", []any{"Ada", 3})
	assertEq(err, nil, "could not write third row")

	// Scan x in read-only transaction
	it, err := c2Reader.scan("x")
	assertEq(err, nil, "could not scan x")
	seen := 0
	for {
		row, err := it.next()
		assertEq(err, nil, "could not iterate x scan")

		if row == nil {
			break
		}

		if scan == 0 {
			assertEq(row, []any{"Joey", 1}, "row mismatch in c2")
		} else {
			assertEq(row, []any{"Yue", 2}, "row mismatch in c2")
		}

		scan++
	}
	assertEq(seen, 2, "expected two rows")

	// Scan x in c1 write transaction
	it, err := c2Writer.scan("x")
	assertEq(err, nil, "could not scan x in c2")
	seen := 0
	for {
		row, err := it.next()
		assertEq(err, nil, "could not iterate x scan in c2")

		if row == nil {
			break
		}

		if scan == 0 {
			assertEq(row, []any{"Joey", 1}, "row mismatch in c1")			
		} else if scan == 1 {
			assertEq(row, []any{"Yue", 2}, "row mismatch in c2")
		} else {
			assertEq(row, []any{"Ada", 3}, "row mismatch in c2")
		}

		scan++
	}
	assertEq(seen, 2, "expected two rows")

	// Writer committing should succeed.
	err := c1Writer.commitTx()
	assertEq(err, nil, "could not commit second tx")

	// Reader committing should succeed.
	err := c2Reader.commitTx()
	assertEq(err, nil, "could not commit read-only tx")
}
