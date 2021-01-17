package main

import (
	"database/sql"
	"errors"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"os"
	"time"
)

type DBWorker struct {
	dsn string
	db  *sql.DB
}

type TestRow struct {
	names []string
	cols  []int64
	id    int64
}

func NewTestRow(names []string) (*TestRow, error) {
	siz := len(names)
	if siz < 1 {
		return nil, errors.New("illegal column len")
	}
	return &TestRow{names: names}, nil
}

func (n *TestRow) GenInterface() []interface{} {
	siz := len(n.names)
	res := make([]interface{}, siz)
	n.cols = make([]int64, siz-1)
	for i := 0; i < siz-1; i += 1 {
		res[i] = &n.cols[i]
	}
	res[siz-1] = &n.id
	return res
}

// mysql -h h81 -P 8008 -D test
const (
	USERNAME = "root"
	PASSWORD = ""
	NETWORK  = "tcp"
	DATABASE = "test"
)

var (
	tidbAddressArgv = flag.String("tidb-addr", os.Getenv("DEFAULT_TEST_TIDB_ADDRESS"), "tidb address")
	columnSizeArgv  = flag.Int64("column-size", 200, "column size")
	modifyRoundArgv = flag.Int64("modify-round", 5, "modify round")
)

func main() {
	flag.Parse()

	var err error

	columnSize := *columnSizeArgv
	modifyRound := *modifyRoundArgv
	tidbAddress := *tidbAddressArgv

	if columnSize <= 0 || columnSize >= 300 {
		panic("invalid columnSize, should be (0, 300)")
	}

	if modifyRound <= 0 {
		panic("invalid modifyRound, should be (0, inf)")
	}

	dbw := DBWorker{
		dsn: fmt.Sprintf("%s:%s@%s(%s)/%s", USERNAME, PASSWORD, NETWORK, tidbAddress, DATABASE),
	}
	dbw.db, err = sql.Open("mysql", dbw.dsn)
	if err != nil {
		panic(err)
	}
	defer dbw.db.Close()

	tableName := fmt.Sprintf("table_col_%d", columnSize)

	fmt.Printf("prepare {table %s} with column size %d\n", tableName, columnSize)

	err = dbw.dropTable(tableName)
	if err != nil {
		fmt.Printf("fail to drop tabel %s\n", tableName)
		panic(err)
	}
	err = dbw.createTable(tableName, columnSize)
	if err != nil {
		fmt.Printf("fail to create tabel %s\n", tableName)
		panic(err)
	}

	fmt.Printf("start to test {table %s} with {column size %d}, modify round %d\n", tableName, columnSize, modifyRound)
	start := time.Now()
	defer func() {
		fmt.Printf("Whole test costs: %fs\n", time.Since(start).Seconds())
	}()

	err = dbw.initTableData(tableName, modifyRound)
	if err != nil {
		fmt.Printf("fail to init table %s\n", tableName)
	}

	for colID := int64(0); colID < columnSize; colID += 1 {
		{
			err = dbw.updateColData2(tableName, colID, 1)
			if err != nil {
				fmt.Printf("fail to update table %s at col_%d\n", tableName, colID)
				return
			}
			fmt.Printf("update table %s set col %d += %d \n", tableName, colID, 1)
		}
		sumRes, err := dbw.querySum(tableName, colID)
		if err != nil {
			fmt.Printf("fail to update sum(col_%d) from table %s\n", colID, tableName)
			return
		}
		fmt.Printf("Successfully got sum(col_%d) of table %s is %f\n", colID, tableName, sumRes)
	}
}

func (dbw *DBWorker) insertData(tableName string, id int64) {
	stmt, err := dbw.db.Prepare(fmt.Sprintf(`insert into %s (id) values (?)`, tableName))
	defer stmt.Close()
	ret, err := stmt.Exec(id)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("insert data success")
	if LastInsertId, err := ret.LastInsertId(); err == nil {
		fmt.Println("LastInsertId:", LastInsertId)
	}
	if RowsAffected, err := ret.RowsAffected(); err == nil {
		fmt.Println("RowsAffected:", RowsAffected)
	}
}

func (dbw *DBWorker) initTableData(tableName string, modifyRound int64) error {
	base := []byte(fmt.Sprintf("insert into %s (id) values ", tableName))
	for i := int64(0); i < modifyRound-1; i += 1 {
		base = append(base, fmt.Sprintf("(%d),", i)...)
	}
	base = append(base, fmt.Sprintf("(%d);", modifyRound-1)...)
	stmt, err := dbw.db.Prepare(string(base))
	defer stmt.Close()
	_, err = stmt.Exec()
	if err != nil {
		fmt.Println(err)
		return err
	}
	fmt.Printf("init table %s with %d records\n", tableName, modifyRound)
	return nil
}

func (dbw *DBWorker) updateColData(tableName string, id int64, cid int64, num int64) error {
	colName := fmt.Sprintf("col_%d", cid)
	stmt, err := dbw.db.Prepare(fmt.Sprintf(`update %s set %s=%s+%d where id=?`, tableName, colName, colName, num))
	defer stmt.Close()
	_, err = stmt.Exec(id)
	if err != nil {
		fmt.Println(err)
		return err
	}
	return nil
}

func (dbw *DBWorker) updateColData2(tableName string, cid int64, num int64) error {
	colName := fmt.Sprintf("col_%d", cid)
	stmt, err := dbw.db.Prepare(fmt.Sprintf(`update %s set %s=%s+%d`, tableName, colName, colName, num))
	defer stmt.Close()
	_, err = stmt.Exec()
	if err != nil {
		fmt.Println(err)
		return err
	}
	return nil
}

func (dbw *DBWorker) createTable(tableName string, colNum int64) error {
	base := []byte(fmt.Sprintf("create table %s (", tableName))
	for i := int64(0); i < colNum; i += 1 {
		base = append(base, fmt.Sprintf("col_%d bigint not null default 0,", i)...)
	}
	base = append(base, "id bigint, primary key (id)"...)
	base = append(base, " ) "...)
	stmt, _ := dbw.db.Prepare(string(base))
	defer stmt.Close()
	_, err := stmt.Exec()
	if err != nil {
		fmt.Println(err)
		return err
	}
	fmt.Printf("success create table %s with column size %d\n", tableName, colNum)
	return nil
}

func (dbw *DBWorker) dropTable(tableName string) error {
	base := []byte(fmt.Sprintf("drop table if exists %s ", tableName))
	stmt, err := dbw.db.Prepare(string(base))
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer stmt.Close()
	_, err = stmt.Exec()
	if err != nil {
		fmt.Println(err)
		return err
	}
	fmt.Printf("success exec %s\n", base)
	return nil
}

func (dbw *DBWorker) queryAll(tableName string) {
	stmt, _ := dbw.db.Prepare(fmt.Sprintf(`select * from %s`, tableName))
	defer stmt.Close()
	rows, err := stmt.Query()
	defer rows.Close()
	if err != nil {
		fmt.Println(err)
		return
	}
	colNames, err := rows.Columns()
	if err != nil {
		fmt.Printf("fail to get col names, %s", err.Error())
		return
	}

	row, err := NewTestRow(colNames)
	if err != nil {
		fmt.Printf("fail to gen test row, %s", err.Error())
		return
	}

	for rows.Next() {
		err := rows.Scan(row.GenInterface()...)
		if err != nil {
			fmt.Printf("fail to scan test rows, %s", err.Error())
			return
		}
	}
	fmt.Printf("%+v\n", row)
	err = rows.Err()
	if err != nil {
		fmt.Printf(err.Error())
	}
}

func (dbw *DBWorker) querySum(tableName string, cid int64) (float64, error) {
	stmt, _ := dbw.db.Prepare(fmt.Sprintf(`select sum(col_%d) from %s`, cid, tableName))
	defer stmt.Close()
	rows, err := stmt.Query()
	defer rows.Close()
	if err != nil {
		fmt.Println(err)
		return 0, err
	}
	_, err = rows.Columns()
	if err != nil {
		fmt.Printf("fail to fetch col names, %s", err.Error())
		return 0, nil
	}

	var sumNum float64

	for rows.Next() {
		err := rows.Scan(&sumNum)
		if err != nil {
			fmt.Printf("fail to scan test rows, %s", err.Error())
			return 0, err
		}
	}
	err = rows.Err()
	if err != nil {
		fmt.Printf(err.Error())
		return 0, err
	}
	return sumNum, nil
}
