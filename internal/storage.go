package internal

import (
	"encoding/binary"

	"github.com/dgraph-io/badger/v3"
	"github.com/fxamacker/cbor/v2"
	"github.com/pkg/errors"
)

const (
	heightKeyPrefix byte = iota
	blockInfoKeyPrefix
)

var (
	heightKey = []byte{heightKeyPrefix}
)

type uint32Value uint32

func (v *uint32Value) marshal() ([]byte, error) {
	return cbor.Marshal(v)
}

func (v *uint32Value) unmarshal(data []byte) error {
	return cbor.Unmarshal(data, v)
}

func (v *uint32Value) toUint32() uint32 {
	return uint32(*v)
}

type blockInfo struct {
	Accounts     uint32  `cbor:"1,keyasint,omitempty"`
	Transactions uint32  `cbor:"2,keyasint,omitempty"`
	Groups       uint32  `cbor:"3,keyasint,omitempty"`
	Parallelism  float32 `cbor:"4,keyasint,omitempty"`
}

func (v *blockInfo) marshal() ([]byte, error) {
	return cbor.Marshal(v)
}

func (v *blockInfo) unmarshal(data []byte) error {
	return cbor.Unmarshal(data, v)
}

type Storage struct {
	db *badger.DB
}

func NewStorage(path string) (*Storage, error) {
	opts := badger.DefaultOptions(path)
	opts.Logger = nil
	if len(path) == 0 {
		opts = opts.WithInMemory(true)
	}
	db, err := badger.Open(opts)
	if err != nil {
		return nil, errors.Errorf("failed to open badger storage: %v", err)
	}
	return &Storage{db: db}, nil
}

func (s *Storage) Close() error {
	return s.db.Close()
}

func (s *Storage) Transaction() *badger.Txn {
	return s.db.NewTransaction(true)
}

func (s *Storage) ReadOnlyTransaction() *badger.Txn {
	return s.db.NewTransaction(false)
}

func (s *Storage) Height(txn *badger.Txn) (uint32, error) {
	if txn == nil {
		return 0, errors.New("empty badger transaction")
	}
	item, err := txn.Get(heightKey)
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return 0, nil
		}
		return 0, err
	}
	var h uint32Value
	if err := item.Value(h.unmarshal); err != nil {
		return 0, err
	}
	return h.toUint32(), nil
}

func (s *Storage) UpdateBlockInfo(txn *badger.Txn, height, accCount, txCount, groupCount uint32, parallelism float32) error {
	if txn == nil {
		return errors.New("empty badger transaction")
	}
	hd, err := cbor.Marshal(height)
	if err != nil {
		return err
	}
	if err = txn.Set(heightKey, hd); err != nil {
		return err
	}
	bi := blockInfo{Accounts: accCount, Transactions: txCount, Groups: groupCount, Parallelism: parallelism}
	b, err := bi.marshal()
	if err != nil {
		return err
	}
	tk := uint32Key(blockInfoKeyPrefix, height)
	if err = txn.Set(tk, b); err != nil {
		return err
	}
	return nil
}

func (s *Storage) BlockInfo(txn *badger.Txn, height uint32) (uint32, uint32, uint32, float32, error) {
	if txn == nil {
		return 0, 0, 0, 0, errors.New("empty badger transaction")
	}
	tk := uint32Key(blockInfoKeyPrefix, height)
	item, err := txn.Get(tk)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	var bi blockInfo
	if err := item.Value(bi.unmarshal); err != nil {
		return 0, 0, 0, 0, err
	}
	return bi.Accounts, bi.Transactions, bi.Groups, bi.Parallelism, nil
}

func uint32Key(prefix byte, n uint32) []byte {
	r := make([]byte, 5)
	r[0] = prefix
	binary.BigEndian.PutUint32(r[1:], n)
	return r
}
