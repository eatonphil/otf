package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"
)

func assert(b bool, msg string) {
	if !b {
		panic(msg)
	}
}

func assertEq[C comparable](a C, b C, prefix string) {
	if a != b {
		panic(fmt.Sprintf("%s '%v' != '%v'", prefix, a, b))
	}
}

var DEBUG = slices.Contains(os.Args, "--debug")

func debug(a ...any) {
	if !DEBUG {
		return
	}

	args := append([]any{"[DEBUG]"}, a...)
	fmt.Println(args...)
}

// https://datatracker.ietf.org/doc/html/rfc4122#section-4.4
func uuidv4() string {
	f, err := os.Open("/dev/random")
	assert(err == nil, fmt.Sprintf("could not open /dev/random: %s", err))
	defer f.Close()

	buf := make([]byte, 16)
	n, err := f.Read(buf)
	assert(err == nil, fmt.Sprintf("could not read 16 bytes from /dev/random: %s", err))
	assert(n == len(buf), "expected 16 bytes from /dev/random")

	// Set bit 6 to 0
	buf[8] &= ^(byte(1) << 6)
	// Set bit 7 to 1
	buf[8] |= 1 << 7

	// Set version
	buf[6] &= ^(byte(1) << 4)
	buf[6] &= ^(byte(1) << 5)
	buf[6] |= 1 << 6
	buf[6] &= ^(byte(1) << 7)

	return fmt.Sprintf("%x-%x-%x-%x-%x",
		buf[:4],
		buf[4:6],
		buf[6:8],
		buf[8:10],
		buf[10:16])
}

type objectStorage interface {
	putIfAbsent(name string, bytes []byte) error
	listPrefix(prefix string) ([]string, error)
	read(name string) ([]byte, error)
}

type fileObjectStorage struct {
	basedir string
}

func newFileObjectStorage(basedir string) *fileObjectStorage {
	return &fileObjectStorage{basedir}
}

func (fos *fileObjectStorage) putIfAbsent(name string, bytes []byte) error {
	filename := path.Join(fos.basedir, name)
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_EXCL|os.O_CREATE, 0644)
	if err != nil {
		return err
	}

	written := 0
	bufSize := 1024 * 16
	for written < len(bytes) {
		toWrite := min(written+bufSize, len(bytes))
		n, err := f.Write(bytes[written:toWrite])
		if err != nil {
			removeErr := os.Remove(filename)
			assert(removeErr == nil, "could not remove")
			return err
		}

		written += n
	}

	err = f.Sync()
	if err != nil {
		removeErr := os.Remove(filename)
		assert(removeErr == nil, "could not remove")
		return err
	}

	err = f.Close()
	if err != nil {
		removeErr := os.Remove(filename)
		assert(removeErr == nil, "could not remove")
		return err
	}

	return nil
}

func (fos *fileObjectStorage) listPrefix(prefix string) ([]string, error) {
	dir := path.Join(fos.basedir)
	f, err := os.Open(dir)
	if err != nil {
		return nil, err
	}

	var files []string
	for err != io.EOF {
		var names []string
		names, err = f.Readdirnames(100)
		if err != nil && err != io.EOF {
			return nil, err
		}

		for _, n := range names {
			if prefix == "" || strings.HasPrefix(n, prefix) {
				files = append(files, n)
			}
		}
	}
	err = f.Close()
	return files, err
}

func (fos *fileObjectStorage) read(name string) ([]byte, error) {
	filename := path.Join(fos.basedir, name)
	return os.ReadFile(filename)
}

type DataobjectAction struct {
	Name  string
	Table string
}

type ChangeMetadataAction struct {
	Table   string
	Columns []string
}

// an enum, only one field will be non-nil
type Action struct {
	AddDataobject  *DataobjectAction
	ChangeMetadata *ChangeMetadataAction
}

const DATAOBJECT_SIZE int = 64 * 1024

type transaction struct {
	id int

	// Both are mapping table name to a list of actions on the table.
	previousActions map[string][]Action
	Actions         map[string][]Action

	// Mapping tables to column names.
	tables map[string][]string

	// Mapping table name to unflushed/in-memory rows. When rows
	// are flushed, the dataobject that contains them is added to
	// `tx.actions` above and `tx.unflushedDataPointer[table]` is
	// reset to `0`.
	unflushedData        map[string]*[DATAOBJECT_SIZE][]any
	unflushedDataPointer map[string]int
}

type client struct {
	os objectStorage
	tx *transaction
}

func newClient(os objectStorage) client {
	return client{os, nil}
}

func (d *client) getTxActions(txLogFilename string) (map[string][]Action, error) {
	bytes, err := d.os.read(txLogFilename)
	if err != nil {
		return nil, err
	}

	var tx transaction
	err = json.Unmarshal(bytes, &tx)
	return tx.Actions, err
}

var errExistingTx = fmt.Errorf("Existing transaction")

func (d *client) newTx() error {
	if d.tx != nil {
		return errExistingTx
	}

	logPrefix := "_log_"
	txLogs, err := d.os.listPrefix(logPrefix)
	if err != nil {
		return err
	}

	var lastTxId = 0
	if len(txLogs) > 0 {
		lastTxIdString := txLogs[len(txLogs)-1][len(logPrefix):]
		lastTxId, err = strconv.Atoi(lastTxIdString)
		if err != nil {
			return err
		}
	}

	tx := &transaction{}
	tx.id = lastTxId + 1
	tx.previousActions = map[string][]Action{}
	tx.Actions = map[string][]Action{}
	tx.tables = map[string][]string{}
	tx.unflushedData = map[string]*[DATAOBJECT_SIZE][]any{}
	tx.unflushedDataPointer = map[string]int{}

	for _, txLog := range txLogs {
		actions, err := d.getTxActions(txLog)
		if err != nil {
			return err
		}

		for table, actions := range actions {
			for _, action := range actions {
				if action.AddDataobject != nil {
					tx.previousActions[table] = append(tx.previousActions[table], action)
				} else if action.ChangeMetadata != nil {
					mtd := action.ChangeMetadata
					tx.tables[table] = mtd.Columns
				} else {
					panic(fmt.Sprintf("unsupported action: %v", action))
				}
			}
		}
	}

	d.tx = tx
	return nil
}

var errNoTx = fmt.Errorf("No transaction")
var errTableExists = fmt.Errorf("Table Exists")
var errNoTable = fmt.Errorf("No Such Table")

func (d *client) createTable(table string, columns []string) error {
	if d.tx == nil {
		return errNoTx
	}

	if _, exists := d.tx.tables[table]; exists {
		return errTableExists
	}

	// Store it in the in-memory mapping.
	d.tx.tables[table] = columns

	// And also add it to the action history for future transactions.
	d.tx.Actions[table] = append(d.tx.Actions[table], Action{
		ChangeMetadata: &ChangeMetadataAction{
			Table:   table,
			Columns: columns,
		},
	})

	return nil
}

type dataobject struct {
	Table string
	Name  string
	Data  [DATAOBJECT_SIZE][]any
	Len   int
}

func (d *client) flushRows(table string) error {
	if d.tx == nil {
		return errNoTx
	}

	// First write out dataobject if there is anything to write out.
	pointer, exists := d.tx.unflushedDataPointer[table]
	if !exists || pointer == 0 {
		return nil
	}

	df := dataobject{
		Table: table,
		Name:  uuidv4(),
		Data:  *d.tx.unflushedData[table],
		Len:   pointer,
	}
	bytes, err := json.Marshal(df)
	if err != nil {
		return err
	}

	err = d.os.putIfAbsent(fmt.Sprintf("_table_%s_%s", table, df.Name), bytes)
	if err != nil {
		return err
	}

	// Record the newly written data file.
	d.tx.Actions[table] = append(d.tx.Actions[table], Action{
		AddDataobject: &DataobjectAction{
			Table: table,
			Name:  df.Name,
		},
	})

	// Reset in-memory pointer.
	d.tx.unflushedDataPointer[table] = 0
	return nil
}

func (d *client) writeRow(table string, row []any) error {
	if d.tx == nil {
		return errNoTx
	}

	if _, ok := d.tx.tables[table]; !ok {
		return errNoTable
	}

	// Try to find an unflushed/in-memory dataobject for this table
	pointer, ok := d.tx.unflushedDataPointer[table]
	if !ok {
		d.tx.unflushedDataPointer[table] = 0
		d.tx.unflushedData[table] = &[DATAOBJECT_SIZE][]any{}
	}

	if pointer == DATAOBJECT_SIZE {
		d.flushRows(table)
		pointer = 0
	}

	d.tx.unflushedData[table][pointer] = row
	d.tx.unflushedDataPointer[table]++
	return nil
}

type scanIterator struct {
	d     *client
	table string

	// First we iterate through unflushed rows.
	unflushedRows       [DATAOBJECT_SIZE][]any
	unflushedRowsLen    int
	unflushedRowPointer int

	// Then we move through each dataobject.
	dataobjects        []string
	dataobjectsPointer int

	// And within each dataobject we iterate through rows.
	dataobject           *dataobject
	dataobjectRowPointer int
}

func (d *client) readDataobject(table, name string) (*dataobject, error) {
	bytes, err := d.os.read(fmt.Sprintf("_table_%s_%s", table, name))
	if err != nil {
		return nil, err
	}

	var do dataobject
	err = json.Unmarshal(bytes, &do)
	return &do, err
}

// returns (nil, nil) when done
func (si *scanIterator) next() ([]any, error) {
	// Iterate through in-memory rows first.
	if si.unflushedRowPointer < si.unflushedRowsLen {
		row := si.unflushedRows[si.unflushedRowPointer]
		si.unflushedRowPointer++
		return row, nil
	}

	// If we've gotten through all dataobjects on disk we're done.
	if si.dataobjectsPointer == len(si.dataobjects) {
		return nil, nil
	}

	if si.dataobject == nil {
		name := si.dataobjects[si.dataobjectsPointer]
		o, err := si.d.readDataobject(si.table, name)
		if err != nil {
			return nil, err
		}

		si.dataobject = o
	}

	if si.dataobjectRowPointer > si.dataobject.Len {
		si.dataobjectsPointer++
		si.dataobject = nil
		si.dataobjectRowPointer = 0
		return si.next()
	}

	row := si.dataobject.Data[si.dataobjectRowPointer]
	si.dataobjectRowPointer++
	return row, nil
}

func (d *client) scan(table string) (*scanIterator, error) {
	if d.tx == nil {
		return nil, errNoTx
	}

	var dataobjects []string
	allActions := append(d.tx.previousActions[table], d.tx.Actions[table]...)
	for _, action := range allActions {
		if action.AddDataobject != nil {
			dataobjects = append(dataobjects, action.AddDataobject.Name)
		}
	}

	var unflushedRows [DATAOBJECT_SIZE][]any
	if data, ok := d.tx.unflushedData[table]; ok {
		unflushedRows = *data
	}

	return &scanIterator{
		unflushedRows:    unflushedRows,
		unflushedRowsLen: d.tx.unflushedDataPointer[table],
		d:                d,
		table:            table,
		dataobjects:      dataobjects,
	}, nil
}

func (d *client) commitTx() error {
	if d.tx == nil {
		return errNoTx
	}

	// Flush any outstanding data
	for table := range d.tx.tables {
		err := d.flushRows(table)
		if err != nil {
			return err
		}
	}

	wrote := false
	for _, actions := range d.tx.Actions {
		if len(actions) > 0 {
			wrote = true
			break
		}
	}
	// Read-only transaction, no need to do a concurrency check.
	if !wrote {
		return nil
	}

	filename := fmt.Sprintf("_log_%020d", d.tx.id)
	// We won't store previous actions, they will be recovered on
	// new transactions. So unset them. Honestly not totally
	// clear why.
	d.tx.previousActions = nil
	bytes, err := json.Marshal(d.tx)
	if err != nil {
		return err
	}

	err = d.os.putIfAbsent(filename, bytes)
	d.tx = nil
	return err
}

func main() {
	panic("unimplemented")
}
