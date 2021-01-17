package table_placement_rule

import (
	"fmt"
	"github.com/tikv/client-go/codec"
)

const (
	SchemaColTableID int64 = 12345
)

type TableKey struct {
	key     []byte
	tableID int64
	encoded []byte
}

type TableKeyHandle struct {
	TableKey
	handleID int64
}

func NewTableKey(tableID int64, key []byte) TableKey {
	res := TableKey{
		key:     key,
		tableID: tableID,
	}
	res.encoded = codec.EncodeBytes(append(codec.GenTableRecordPrefix(tableID), res.key...))
	return res
}

func NewTableKeyByHandle(tableID int64, handleID int64) TableKeyHandle {
	handleKey := make([]byte, 0, 8)
	handleKey = codec.EncodeInt(handleKey, handleID)

	res := TableKeyHandle{
		TableKey: NewTableKey(tableID, handleKey),
		handleID: handleID,
	}
	return res
}

func MakeTableRecordStartEndKey(tableId int64) ([]byte, []byte) {
	return NewTableKey(tableId, []byte("")).encoded, codec.EncodeBytes(codec.GenNextTablePrefix(tableId))
}

func (k *TableKey) EncodeBytes() []byte {
	return k.encoded
}

func (k *TableKey) String() string {
	return fmt.Sprintf("table_id: %d, key: %s", k.tableID, k.key)
}

func (k *TableKeyHandle) String() string {
	return fmt.Sprintf("table_id: %d, handle: %d", k.tableID, k.handleID)
}
