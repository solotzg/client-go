package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/locate"
	"github.com/tikv/client-go/rawkv"
	"github.com/tikv/client-go/retry"
	. "github.com/tikv/client-go/table-placement-rule"
	"os"
	"strconv"
	"strings"
	"time"
)

type EngineModifyType uint8

const (
	EngineDel EngineModifyType = 1 + iota
	EngineAdd
)

type TableModifyDel struct {
	key *TableKey
}

type TableModifyAdd struct {
	TableModifyDel
	colID int64
	val   int64
}

func (m *TableModifyAdd) String() string {
	return fmt.Sprintf("Add col_%d %d", m.colID, m.val)
}

func (m *TableModifyDel) Encode() (v []byte) {
	v = append(v, byte(EngineDel))
	return v
}

func (m *TableModifyAdd) Encode() (v []byte) {
	v = append(v, byte(EngineAdd))
	var data [8]byte
	binary.LittleEndian.PutUint64(data[:], uint64(m.colID))
	v = append(v, data[:]...)
	binary.LittleEndian.PutUint64(data[:], uint64(m.val))
	v = append(v, data[:]...)
	return v
}

func MakeTableModifyDel(key *TableKey) TableModifyDel {
	return TableModifyDel{
		key: key,
	}
}

func MakeTableModifyAdd(key *TableKey, colID int64, val int64) TableModifyAdd {
	return TableModifyAdd{
		TableModifyDel: TableModifyDel{
			key: key,
		},
		colID: colID,
		val:   val,
	}
}

var (
	pdAddressArgv       = flag.String("pd", os.Getenv("DEFAULT_TEST_PD_ADDRESS"), "pd address")
	columnSizeArgv      = flag.Int64("column-size", 200, "column size")
	modifyRoundArgv     = flag.Int64("modify-round", 5, "modify round")
	killLeaderStoreArgv = flag.Bool("kill", false, "send message to kill -9 one store which has leader peer")
	showRegions         = flag.Bool("show-regions", false, "show all regions information about test table")
	noModify            = flag.Bool("no-modify", false, "skip all modify operators")
	cleanUp             = flag.Bool("clean-up", false, "delete all data in table")
)

func Main2() {
	flag.Parse()

	writeCf := "write"

	tableID := SchemaColTableID
	pdAddress := *pdAddressArgv
	columnSize := *columnSizeArgv
	modifyRound := *modifyRoundArgv
	killLeaderStore := *killLeaderStoreArgv

	if columnSize <= 0 || columnSize >= 300 {
		panic("invalid columnSize, should be (0, 300)")
	}

	if modifyRound <= 0 {
		panic("invalid modifyRound, should be (0, inf)")
	}

	cli, err := rawkv.NewClient(context.TODO(), []string{pdAddress}, config.Default())
	if err != nil {
		panic(err)
	}
	defer cli.Close()
	startKey, endKey := MakeTableRecordStartEndKey(tableID)

	if *showRegions {
		regionCache := cli.RegionCache()
		bo := retry.NewBackoffer(context.TODO(), retry.RawkvMaxBackoff)
		res := make([]*locate.RPCContext, 0, 10)
		{
			for bytes.Compare(startKey, endKey) < 0 {
				loc, err := regionCache.LocateKey(bo, startKey)
				if err != nil {
					panic(err)
				}
				ctx, err := regionCache.GetRPCContext(bo, loc.Region)
				if err != nil {
					panic(err)
				}
				res = append(res, ctx)
				startKey = loc.EndKey
				if len(startKey) == 0 {
					break
				}
			}
		}
		regionIds := make([]string, 0, 64)
		for _, r := range res {
			regionIds = append(regionIds, strconv.FormatUint(r.Region.GetID(), 10))
		}
		fmt.Printf("There are %d regions in this placement rule: %s\n", len(res), strings.Join(regionIds, ","))
		for _, e := range res {
			fmt.Printf("\n%+v\n", *e)
		}
		return
	}

	if killLeaderStore {
		if err := cli.Kill(context.TODO(), startKey); err != nil {
			fmt.Printf("cluster ID: %d, fail to kill -9 store which has leader peer %s", cli.ClusterID(), err)
		}
		return
	}

	fmt.Printf("cluster ID: %d, start to test {table %d} with {column size %d}, modify round %d\n", cli.ClusterID(), tableID, columnSize, modifyRound)
	start := time.Now()
	defer func() {
		fmt.Printf("Whole test costs: %fs\n", time.Since(start).Seconds())
	}()

	if !*noModify {
		keys, err := cli.GetTableHandleIDs(context.TODO(), startKey, endKey)
		if err != nil {
			panic(err)
		}
		values := make([][]byte, len(keys))
		for i, _ := range keys {
			add := MakeTableModifyDel(nil)
			values[i] = add.Encode()
		}
		if err = cli.BatchPutCf(context.TODO(), keys, values, writeCf); err != nil {
			panic(err)
		}
		fmt.Printf("Successfully delete %d record of table %d\n", len(keys), tableID)
	}
	if *cleanUp {
		return
	}
	if !*noModify {
		keys := make([][]byte, modifyRound)
		values := make([][]byte, modifyRound)
		for handleID := int64(0); handleID < modifyRound; handleID += 1 {
			handleKey := NewTableKeyByHandle(tableID, handleID)
			key := &handleKey.TableKey
			modify := MakeTableModifyAdd(key, columnSize-1, 0)
			keys[handleID] = key.EncodeBytes()
			values[handleID] = modify.Encode()
		}
		if err = cli.BatchPutCf(context.TODO(), keys, values, writeCf); err != nil {
			panic(err)
		}
		fmt.Printf("Successfully init %d record of table %d\n", len(keys), tableID)
	}
	for colID := int64(0); colID < columnSize; colID += 1 {
		if !*noModify {
			keys, err := cli.GetTableHandleIDs(context.TODO(), startKey, endKey)
			if err != nil {
				panic(err)
			}
			values := make([][]byte, len(keys))
			for i, _ := range keys {
				add := MakeTableModifyAdd(nil, colID, 1)
				values[i] = add.Encode()
			}
			if err = cli.BatchPutCf(context.TODO(), keys, values, writeCf); err != nil {
				panic(err)
			}
			fmt.Printf("update table %d set col %d += %d \n", tableID, colID, 1)
		}
		sumRes, err := cli.SumTableCol(context.TODO(), startKey, endKey, colID)
		if err != nil {
			panic(err)
		}
		fmt.Printf("Successfully got sum of table %d col %d is %d \n", tableID, colID, sumRes)
	}
}

func main() {
	Main2()
}
