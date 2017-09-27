package clickhouse

import (
	"bytes"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	httpTransportBodyType = "text/plain"
)

type Transport interface {
	Exec(conn *Conn, q Query, readOnly bool) (res string, err error)
}

type HttpTransport struct {
	Timeout time.Duration
}

func (t HttpTransport) Exec(conn *Conn, q Query, readOnly bool) (res string, err error) {
	var resp *http.Response
	query := prepareHttp(q.Stmt, q.args)
	client := &http.Client{Timeout: t.Timeout}

	if readOnly {
		if len(query) > 0 {
			query = "?query=" + query
		}

		params := conn.params.Encode()

		if len(params) > 0 {
			if len(query) > 0 {
				query += "&" + params
			} else {
				query += "?" + params
			}
		}

		resp, err = client.Get(conn.Host + query)
	} else {
		var req *http.Request

		// Set global parameters for query, like: user, password, max_memory_limit, etc.
		// But it skips already defined params.
		req, err = prepareExecPostRequest(conn, q)

		if err != nil {
			return "", err
		}
		resp, err = client.Do(req)
	}

	if err != nil {
		return "", err
	}

	defer resp.Body.Close()
	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(resp.Body)

	return buf.String(), err
}

func prepareExecPostRequest(conn *Conn, q Query) (*http.Request, error) {
	query := prepareHttp(q.Stmt, q.args)
	var req *http.Request
	var err error = nil
	if len(q.externals) > 0 {
		if len(query) > 0 {
			query = "?query=" + url.QueryEscape(query)
		}

		body := &bytes.Buffer{}
		writer := multipart.NewWriter(body)

		for _, ext := range q.externals {
			query = query + "&" + ext.Name + "_structure=" + url.QueryEscape(ext.Structure)
			part, err := writer.CreateFormFile(ext.Name, ext.Name)
			if err != nil {
				return nil, err
			}
			_, err = part.Write(ext.Data)
			if err != nil {
				return nil, err
			}
		}

		params := q.params.Encode()

		if len(params) > 0 {
			query += "&" + params
		}

		paramsCon := conn.params.Encode()

		if len(paramsCon) > 0 {
			query += "&" + paramsCon
		}

		err = writer.Close()
		if err != nil {
			return nil, err
		}

		req, err = http.NewRequest("POST", conn.Host + query, body)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", writer.FormDataContentType())
	} else {
		req, err = http.NewRequest("POST", conn.Host + "?" + conn.params.Encode(), strings.NewReader(query))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", httpTransportBodyType)
	}
	return req, err
}

func prepareHttp(stmt string, args []interface{}) string {

	var res []byte

	buf := []byte(stmt)

	res = make([]byte, 0)

	k := 0

	skip_to := -1

	for key, ch := range buf {

		if skip_to != -1 && key < skip_to {
			continue
		} else {
			skip_to = -1
		}

		if ch == ':' && stmt[key:key + 7] == ":value:"  {
			res = append(res, []byte(marshal(args[k]))...)
			k++
			skip_to = key + 7
		} else {
			res = append(res, ch)
		}
	}

	return string(res)
}
