package clickhouse

import (
	"fmt"

	"github.com/stretchr/testify/assert"

	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

type TestHandler struct {
	Result string
}

func (h *TestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Content-Type", "text/tab-separated-values; charset=UTF-8")
	fmt.Fprint(w, h.Result)
}

func TestExec(t *testing.T) {
	handler := &TestHandler{Result: "1  2.5 clickid68235\n2 -0.14   clickidsdkjhj44"}
	server := httptest.NewServer(handler)
	defer server.Close()

	transport := NewHttpTransport()
	conn := Conn{Host: server.URL, transport: transport}
	q := NewQuery("SELECT * FROM testdata")
	resp, err := conn.Exec(q, false)
	assert.Equal(t, nil, err)
	assert.Equal(t, handler.Result, resp)

}

func TestExecReadOnly(t *testing.T) {
	handler := &TestHandler{Result: "1  2.5 clickid68235\n2 -0.14   clickidsdkjhj44"}
	server := httptest.NewServer(handler)
	defer server.Close()

	transport := NewHttpTransport()
	conn := Conn{Host: server.URL, transport: transport}
	q := NewQuery(url.QueryEscape("SELECT * FROM testdata"))
	query := prepareHttp(q.Stmt, q.args)
	query = "?query=" + url.QueryEscape(query)
	resp, err := conn.Exec(q, true)
	assert.Equal(t, nil, err)
	assert.Equal(t, handler.Result, resp)

}

func TestPrepareHttp(t *testing.T) {
	p := prepareHttp("SELECT * FROM table WHERE key = :value:", []interface{}{"test"})
	assert.Equal(t, "SELECT * FROM table WHERE key = 'test'", p)
}

func TestPrepareHttpArray(t *testing.T) {
	p := prepareHttp("INSERT INTO table (arr) VALUES (:value:)", Row{Array{"val1", "val2"}})
	assert.Equal(t, "INSERT INTO table (arr) VALUES (['val1','val2'])", p)
}

func BenchmarkPrepareHttp(b *testing.B) {
	params := strings.Repeat("(?,?,?,?,?,?,?,?)", 1000)
	args := make([]interface{}, 8000)
	for i := 0; i < 8000; i++ {
		args[i] = "test"
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		prepareHttp("INSERT INTO t VALUES "+params, args)
	}
}
