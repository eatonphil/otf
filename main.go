package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
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
		toWrite := min(written+bufSize, len(bytes)-1)
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

type dataobjectAction struct {
	name string
	table string
}

type changeMetadataAction struct {
	table string
	columns []string
}

// an enum, only one field will be non-nil
type action struct {
	addDataobject *dataobjectAction
	changeMetadata *changeMetadataAction
}

const DATAOBJECT_SIZE uint = 64 * 1024
type transaction struct {
	id uint

	// Both are mapping table name to a list of actions on the table.
	previousActions map[string][]action
	actions map[string][]action

	// Mapping tables to column names.
	tables map[string][]string

	// Mapping table name to unflushed/in-memory rows. When rows
	// are flushed, the dataobject that contains them is added to
	// `tx.actions` above and `tx.unflushedDataPointer[table]` is
	// reset to `0`.
	unflushedData map[string][DATAOBJECT_SIZE][]any
	unflushedDataPointer map[string]uint
}

type client struct {
	os objectStorage
	tx *transaction
}

func newClient(os objectStorage) client {
	return client{os, nil}
}

func (d *client) getTxActions(txLogFilename string) ([]action, error) {
	bytes, err := d.os.read(txLogFilename)
	if err != nil {
		return nil, err
	}

	var dataobjects []action
	err = json.Unmarshal(bytes, &dataobjects)
	return dataobjects, err
}

var errExistingTx = fmt.Errorf("Existing Transaction")

func (d *client) newTx() error {
	if d.tx != nil {
		return errExistingTx
	}

	logPrefix := "_log_"
	txLogs, err := d.os.listPrefix(logPrefix)
	if err != nil {
		return err
	}

	lastTxIdString := txLogs[len(txLogs)-1][len(logPrefix):]
	lastTxId, err := strconv.ParseUint(lastTxIdString, 10, 64)
	if err != nil {
		return err
	}

	tx := &transaction{}
	tx.id = uint(lastTxId) + 1
	tx.previousActions = map[string][]action{}
	tx.actions = map[string][]action{}

	for _, txLog := range txLogs {
		actions, err := d.getTxActions(txLog)
		if err != nil {
			return err
		}

		for _, action := range actions {
			if action.addDataobject != nil {
				table := action.addDataobject.table
				tx.previousActions[table] = append(tx.previousActions[table], action)
			} else if action.changeMetadata != nil {
				mtd := action.changeMetadata
				tx.tables[mtd.table] = mtd.columns
			} else {
				panic(fmt.Sprintf("unsupported action: %v", action))
			}
		}
	}

	d.tx = tx
	return nil
}

var errNoTx = fmt.Errorf("No Transaction")
var errTableExists = fmt.Errorf("Table Exists")
var errNoTable = fmt.Errorf("No Such Table")

func (d *client) createTable(table string, columns []string) error {
	if d.tx == nil {
		return errNoTx
	}

	if _, exists := d.tx.tables[table]; exists {
		return errTableExists
	}

	d.tx.tables[table] = columns
}

type dataobject struct {
	table string
	name string
	data [][]any
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
		table: table,
		name: uuidv4(),
		data: d.tx.unflushedData[table][:pointer],
	}
	bytes, err := json.Marshal(df)
	if err != nil {
		return err
	}

	err := d.os.putIfAbsent(df.name, bytes)
	if err != nil {
		return err
	}

	// Record the newly written data file.
	d.tx.actions = append(d.tx.actions, action{
		addDataobject: &dataobjectAction{
			table: table,
			name: df.name,
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

	var df *dataobject = nil
	// Try to find an unflushed/in-memory dataobject for this table
	pointer, ok := d.tx.unflushedDataPointer[table]
	if !ok {
		d.tx.unflushedDataPointer[table] = 0
		d.tx.unflushedData[table] = [DATAOBJECT_SIZE][]any{}
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
	d *client
	table string

	// First we iterate through unflushed rows.
	unflushedRows [][]any
	unflushedRowPointer uint

	// Then we move through each dataobject.
	dataobjects []string
	dataobjectsPointer uint

	// And within each dataobject we iterate through rows.
	dataobject *dataobject
	dataobjectRowPointer uint
}


func (d *client) readDataobject(df dataobject) ([][]any, error) {
	bytes, err := d.os.read(fmt.Sprintf("_table_%s_%s", table, previousDf))
	if err != nil {
		return nil, err
	}

	var do dataobject
	err = json.Unmarshal(bytes, &do)
	return do, err
}

// returns (nil, nil) when done
func (si *scanIterator) next() ([]any, error) {
	// Iterate through in-memory rows first.
	if si.unflushedRowPointer < len(si.unflushedRows) {
		row := si.unflushedRows[si.unflushedRowPointer]
		si.unflushedRowPointer++
		return row, nil
	}

	// If we've gotten through all dataobjects on disk we're done.
	if si.dataobjectsPointer == len(si.dataobjects) {
		return nil, nil
	}

	if si.dataobject == nil {
		o, err := s.d.readDataobject(si.dataobjects[si.dataobjectsPointer])
		if err != nil {
			return nil, err
		}

		si.dataobject = o
	}

	if si.dataobjectRowPointer == len(s.dataobject.rows) {
		si.dataobjectsPointer++
		si.dataobject = nil
		si.dataobjectRowPointer == 0
		return si.next()
	}

	row := si.dataobject.rows[s.dataobjectsPointer]
	s.dataobjectsPointer++
	return row, nil
}

func (d *client) scan(table string) (*scanIterator, error) {
	if d.tx == nil {
		return nil, errNoTx
	}

	var dataobjects []dataobject
	allActions := append(d.tx.previousActions, d.tx.actions)
	for _, action := range allActions {
		if action.addDataobject != nil {
			dataobjects = append(dataobjects, dataobject{
				table: table,
				name: action.addDataobject.name,
			})
		}
	}

	return &scanIterator{
		unflushedRows: unflushedRows,
		d: d,
		table: table,
		dataobjects: dataobjects,
	}, nil
}

func (d *client) commitTx() error {
	if d.tx == nil {
		return errNoTx
	}

	filename := fmt.Sprintf("_log_%020d", d.tx.id)
	tx.previousActions = nil
	bytes, err := json.Marshal(tx)
	if err != nil {
		return err
	}

	return d.os.putIfAbsent(filename, bytes)
}

func main() {}
