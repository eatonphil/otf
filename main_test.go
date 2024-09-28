package main

import (
	"os"
	"testing"
)

func TestConcurrentTableWriters(t *testing.T) {
	dir, err := os.MkdirTemp("", "test-database")

	if err != nil {
		panic(err)
	}

	defer os.Remove(dir)

	fos := newFileObjectStorage(dir)
	c1Writer := newClient(fos)
	c2Writer := newClient(fos)

	// Have c2Writer start up a transaction.
	err = c2Writer.newTx()
	assertEq(err, nil, "could not start first c2 tx")
	debug("[c2] new tx")

	// But then have c1Writer start a transaction and commit it first.
	err = c1Writer.newTx()
	assertEq(err, nil, "could not start first c1 tx")
	debug("[c1] new tx")
	err = c1Writer.createTable("x", []string{"a", "b"})
	assertEq(err, nil, "could not create x")
	debug("[c1] Created table")
	err = c1Writer.writeRow("x", []any{"Joey", 1})
	assertEq(err, nil, "could not write first row")
	debug("[c1] Wrote row")
	err = c1Writer.writeRow("x", []any{"Yue", 2})
	assertEq(err, nil, "could not write second row")
	debug("[c1] Wrote row")
	err = c1Writer.commitTx()
	assertEq(err, nil, "could not commit tx")
	debug("[c1] Committed tx")

	// Now go back to c2 and write data.
	err = c2Writer.createTable("x", []string{"a", "b"})
	assertEq(err, nil, "could not create x")
	debug("[c2] Created table")
	err = c2Writer.writeRow("x", []any{"Holly", 1})
	assertEq(err, nil, "could not write first row")
	debug("[c2] Wrote row")

	err = c2Writer.commitTx()
	assert(err != nil, "concurrent commit must fail")
	debug("[c2] tx not committed")

}

func TestConcurrentReaderWithWriterReadsSnapshot(t *testing.T) {
	dir, err := os.MkdirTemp("", "test-database")

	if err != nil {
		panic(err)
	}

	defer os.Remove(dir)

	fos := newFileObjectStorage(dir)
	c1Writer := newClient(fos)
	c2Reader := newClient(fos)

	// First create some data and commit the transaction.
	err = c1Writer.newTx()
	assertEq(err, nil, "could not start first c1 tx")
	err = c1Writer.createTable("x", []string{"a", "b"})
	assertEq(err, nil, "could not create x")
	debug("Created table")
	err = c1Writer.writeRow("x", []any{"Joey", 1})
	assertEq(err, nil, "could not write first row")
	debug("Wrote row")
	err = c1Writer.writeRow("x", []any{"Yue", 2})
	assertEq(err, nil, "could not write second row")
	debug("Wrote row")
	err = c1Writer.commitTx()
	assertEq(err, nil, "could not commit tx")
	debug("Committed tx")

	// Now start a new transaction for more edits.
	err = c1Writer.newTx()
	assertEq(err, nil, "could not start second c1 tx")
	debug("Starting new write tx")

	// Before we commit this second write-transaction, start a
	// read transaction.
	err = c2Reader.newTx()
	assertEq(err, nil, "could not start c2 tx")
	debug("Starting new read tx")

	// Write and commit rows in c1.
	err = c1Writer.writeRow("x", []any{"Ada", 3})
	assertEq(err, nil, "could not write third row")
	debug("Wrote third row")

	// Scan x in read-only transaction
	it, err := c2Reader.scan("x")
	assertEq(err, nil, "could not scan x")
	debug("Started scanning in reader tx")
	seen := 0
	for {
		row, err := it.next()
		assertEq(err, nil, "could not iterate x scan")

		if row == nil {
			debug("Done scanning in reader tx")
			break
		}

		debug("Got row in reader tx")
		if seen == 0 {
			assertEq(row[0], "Joey", "row mismatch in c1")
			assertEq(row[1], 1.0, "row mismatch in c1")
		} else {
			assertEq(row[0], "Yue", "row mismatch in c1")
			assertEq(row[1], 2.0, "row mismatch in c1")
		}

		seen++
	}
	assertEq(seen, 2, "expected two rows")

	// Scan x in c1 write transaction
	it, err = c1Writer.scan("x")
	assertEq(err, nil, "could not scan x in c1")
	seen = 0
	for {
		row, err := it.next()
		assertEq(err, nil, "could not iterate x scan in c1")

		if row == nil {
			break
		}

		if seen == 0 {
			assertEq(row[0], "Ada", "row mismatch in c1")
			// Since this hasn't been serialized to JSON, it's still an int not a float.
			assertEq(row[1], 3, "row mismatch in c1")
		} else if seen == 1 {
			assertEq(row[0], "Joey", "row mismatch in c1")
			assertEq(row[1], 1.0, "row mismatch in c1")
		} else {
			assertEq(row[0], "Yue", "row mismatch in c1")
			assertEq(row[1], 2.0, "row mismatch in c1")
		}

		seen++
	}
	assertEq(seen, 3, "expected three rows")

	// Writer committing should succeed.
	err = c1Writer.commitTx()
	assertEq(err, nil, "could not commit second tx")

	// Reader committing should succeed.
	err = c2Reader.commitTx()
	assertEq(err, nil, "could not commit read-only tx")
}
