package clickhouse

import (
	"errors"
	"strings"
	"fmt"
	"net/url"
)

type (
	Column  string
	Columns []string
	Row     []interface{}
	Rows    []Row
	Array   []interface{}
	VisitParamsString map[string]interface{}
	StringArray []string
)

func NewHttpTransport() HttpTransport {
	return HttpTransport{}
}

func NewConn(host string, t Transport) *Conn {
	if strings.Index(host, "http://") < 0 && strings.Index(host, "https://") < 0 {
		host = "http://" + host
	}
	host = strings.TrimRight(host, "/") + "/"

	return &Conn{
		Host:      host,
		transport: t,
		params: url.Values{},
	}
}

func NewQuery(stmt string, args ...interface{}) Query {
	return Query{
		Stmt: stmt,
		args: args,
		params:url.Values{},
	}
}

func OptimizeTable(table string) Query {
	return NewQuery("OPTIMIZE TABLE " + table)
}

func OptimizePartition(table string, partition string) Query {
	return NewQuery("OPTIMIZE TABLE " + table + " PARTITION " + partition + " FINAL")
}

func IsLeader(table string, conn *Conn) bool {

	var leader uint8

	settings := strings.Split(table, ".")

	database := "default"

	if len(settings) > 1 {

		database = settings[0]

		table = settings[1]

	}

	query := NewQuery("SELECT is_leader FROM system.replicas WHERE database = '" + database + "' table = '" + table + "'")

	iter := query.Iter(conn)

	if iter.Error() != nil {
		return false
	}

	for iter.Scan(&leader) {
		return leader == 1
	}

	return false
}

func BuildInsert(tbl string, cols Columns, row Row) (Query, error) {
	return BuildMultiInsert(tbl, cols, Rows{row})
}

func BuildMultiInsert(tbl string, cols Columns, rows Rows) (Query, error) {
	var (
		stmt string
		args []interface{}
	)

	if len(cols) == 0 || len(rows) == 0 {
		return Query{}, errors.New("rows and cols cannot be empty")
	}

	colCount := len(cols)
	rowCount := len(rows)
	args = make([]interface{}, colCount*rowCount)
	argi := 0

	for _, row := range rows {
		if len(row) != colCount {
			return Query{}, errors.New("Amount of row items does not match column count")
		}
		for _, val := range row {
			args[argi] = val
			argi++
		}
	}

	binds := strings.Repeat("?,", colCount)
	binds = "(" + binds[:len(binds)-1] + "),"
	batch := strings.Repeat(binds, rowCount)
	batch = batch[:len(batch)-1]

	stmt = fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", tbl, strings.Join(cols, ","), batch)

	return NewQuery(stmt, args...), nil
}
