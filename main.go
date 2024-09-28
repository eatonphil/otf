package main

import "os"
import "path"

type objectStorage interface {
	putIfAbsent(name string, bytes []byte) error
	listPrefix(prefix string) ([]string, error)
	read(name string) ([]byte, error)
}

type fileObjectStorage struct {
	baseDir string
}

func (fos *fileObjectStorage) putIfAbsent(name string, bytes []byte) error {
	filename = path.Join(fos.baseDir, name)
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_EXCL|os.O_CREATE, 0644)
	if err != nil {
		return err
	}

	written := 0
	bufSize := 1024 * 16
	for written < bytes {
		toWrite := min(written + bufSize, len(bytes)-1)
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

	err = fs.Close()
	if err != nil {
		removeErr = os.Remove(filename)
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
		return err
	}

	var files []string
	var err error
	for err != os.EOF {
		names, err := f.Readdirnames(100)
		if err != nil && err != os.EOF {
			return err
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
	filename = path.Join(fos.baseDir, name)
	return os.ReadFile(filename)
}

type transaction struct {
	id uint64
}

type database struct {
	os objectStorage
	tx *transaction
}

func newDatabase(os objectStorage) database {
	return database{os, nil}
}

var errExistingTransaction = fmt.Errorf("Existing Transaction")
func (d *database) newTransaction() error {
	if d.tx != nil {
		return errExistingTransaction
	}

	logPrefix := "_log_"
	transactionLogs, err := os.listPrefix(logPrefix)
	if err != nil {
		return err
	}

	lastTxIdString := transactionLogs[len(transactionLogs)-1][len(logPrefix):]
	lastTxId, err := strconv.ParseUint(lastTxIdString, 10, 64)
	if err != nil {
		return err
	}

	d.tx = &transaction{lastTxId+1}
}

var errNoTransaction = fmt.Errorf("No Transaction")
func (d *database) writeRow(table string, row []any) error {
	if d.tx == nil {
		return errNoTransaction
	}
}

func (d *database) commitTransaction() error {
	if d.tx == nil {
		return errNoTransaction
	}

	filename := fmt.Sprintf("_log_%020d", d.tx.id)
	return d.os.putIfAbsent(filename, txData)
}
