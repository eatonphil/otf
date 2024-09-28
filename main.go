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

// https://datatracker.ietf.org/doc/html/rfc4122#section-4.4
func uuidv4() {
	f, err := os.Open("/dev/random")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	buf := make([]byte, 16)
	n, err := f.Read(buf)
	if err != nil {
		panic(err)
	}

	if n != len(buf) {
		panic("Expected 16 bytes")
	}

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
	baseDir string
}

func (fos *fileObjectStorage) putIfAbsent(name string, bytes []byte) error {
	filename := path.Join(fos.baseDir, name)
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
			if removeErr != nil {
				panic(removeErr)
			}

			return err
		}

		written += n
	}

	err = f.Sync()
	if err != nil {
		removeErr := os.Remove(filename)
		if removeErr != nil {
			panic(removeErr)
		}

		return err
	}

	err = f.Close()
	if err != nil {
		removeErr := os.Remove(filename)
		if removeErr != nil {
			panic(removeErr)
		}

		return err
	}

	return nil
}

func (fos *fileObjectStorage) listPrefix(prefix string) ([]string, error) {
	dir := path.Join(fos.baseDir)
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
	filename := path.Join(fos.baseDir, name)
	return os.ReadFile(filename)
}

type datafileAction struct {
	datafile string
	table string
}

type changeMetadataAction struct {
	table string
	columns []string
}

// an enum, only one field will be non-nil
type action struct {
	addDatafile *datafileAction
	changeMetadata *changeMetadataAction
}

const DATAFILE_SIZE uint = 64 * 1024
type transaction struct {
	id uint

	// Both are mapping table name to a list of actions on the table.
	previousActions map[string][]action
	actions map[string][]action

	// Mapping tables to column names.
	tables map[string][]string

	// Mapping table name to unflushed/in-memory rows. When rows
	// are flushed, the datafile that contains them is added to
	// `tx.actions` above and `tx.unflushedDataPointer[table]` is
	// reset to `0`.
	unflushedData map[string][DATAFILE_SIZE][]any
	unflushedDataPointer map[string]uint
}

type database struct {
	os objectStorage
	tx *transaction
}

func newDatabase(os objectStorage) database {
	return database{os, nil}
}

func (d *database) getTxActions(txLogFilename string) ([]action, error) {
	bytes, err := d.os.read(txLogFilename)
	if err != nil {
		return nil, err
	}

	var datafiles []action
	err = json.Unmarshal(bytes, &datafiles)
	return datafiles, err
}

var errExistingTx = fmt.Errorf("Existing Transaction")

func (d *database) newTx() error {
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

	tx := &transaction{id: lastTxId + 1}

	for _, txLog := range txLogs {
		actions, err := d.getTxActions(txLog)
		if err != nil {
			return err
		}

		for _, action := range actions {
			if action.addDatafile != nil {
				table := action.addDatafile.table
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

func (d *database) readDatafile(df datafile) ([][]any, error) {
	actions := append(, d.tx.actions[table]...)

	var dfs []datafile
	for _, previousDf := range d.tx.previousDatafiles {
		dfBytes, err := d.os.read(fmt.Sprintf("_table_%s_%s", table, previousDf))
		if err != nil {
			return nil, err
		}

		var data [][]any
		err = json.Unmarshal(dfBytes, &data)
		if err != nil {
			return nil, err
		}

		dfs = append(dfs, datafile{
			name: previousDf,
			data: data,
		})
	}

	for _, df := range d.tx.previousDatafiles {

	}
}

var errTableExists = fmt.Errorf("Table Exists")
func (d *database) createTable(table string, columns []string) error {
	if d.tx == nil {
		return errNoTx
	}

	if _, exists := d.tx.tables[table]; exists {
		return errTableExists
	}

	d.tx.tables[table] = columns
}

type datafile struct {
	datafileAction
	data [][]any
}

func (d *database) flushRows(table string) error {
	if d.tx == nil {
		return errNoTx
	}

	// First write out datafile if there is anything to write out.
	pointer, exists := d.tx.unflushedDataPointer[table]
	if !exists || pointer == 0 {
		return nil
	}

	df := datafile{
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
		addDatafile: &datafileAction{
			table: table,
			name: df.name,
		},
	})

	// Reset in-memory pointer.
	d.tx.unflushedDataPointer[table] = 0
	return nil
}

func (d *database) writeRow(table string, row []any) error {
	if d.tx == nil {
		return errNoTx
	}

	var df *datafile = nil
	// Try to find an unflushed/in-memory datafile for this table
	pointer, ok := d.tx.unflushedDataPointer[table]
	if !ok {
		d.tx.unflushedDataPointer[table] = 0
		d.tx.unflushedData[table] = [DATAFILE_SIZE][]any{}
	}

	if pointer == DATAFILE_SIZE {
		d.flushRows(table)
		pointer = 0
	}

	d.tx.unflushedData[table][pointer] = row
	d.tx.unflushedDataPointer[table]++

	return nil
}

func (d *database) scan(table string) error {
	if d.tx == nil {
		return errNoTx
	}

	// First check through unwritten datafiles

	return nil
}

func (d *database) commitTx() error {
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
