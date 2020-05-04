// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package dorisloader

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"runtime"
	"strings"
)

type Request http.Request

// NewRequest is a http.Request and adds features such as encoding the body.
func NewRequest(method, url string, body io.Reader) (*Request, error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}

	req.Header.Add("User-Agent", "DorisLoader/"+Version+" ("+runtime.GOOS+"-"+runtime.GOARCH+")")
	return (*Request)(req), nil
}

func (r *Request) SetBasicAuth(username, password string) {
	((*http.Request)(r)).SetBasicAuth(username, password)
}

func handleGetBodyReader(header http.Header, body interface{}, gzipCompress bool) (io.Reader, error) {
	switch b := body.(type) {
	case string:
		if gzipCompress {
			return getBodyGzipReader(header, b)
		}
		return getBodyString(b)
	default:
		if gzipCompress {
			return getBodyGzipReader(header, body)
		}
		return getBodyJsonReader(header, body)
	}
}

func getBodyJsonReader(header http.Header, data interface{}) (io.Reader, error) {
	body, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	header.Set("Content-Type", "application/json")
	return bytes.NewReader(body), nil
}

func getBodyString(body string) (io.Reader, error) {
	return strings.NewReader(body), nil
}

func getBodyGzipReader(header http.Header, body interface{}) (io.Reader, error) {
	switch b := body.(type) {
	case string:
		buf := new(bytes.Buffer)
		w := gzip.NewWriter(buf)
		if _, err := w.Write([]byte(b)); err != nil {
			return nil, err
		}
		if err := w.Close(); err != nil {
			return nil, err
		}
		header.Add("Content-Encoding", "gzip")
		header.Add("Vary", "Accept-Encoding")
		return bytes.NewReader(buf.Bytes()), nil
	default:
		data, err := json.Marshal(b)
		if err != nil {
			return nil, err
		}
		buf := new(bytes.Buffer)
		w := gzip.NewWriter(buf)
		if _, err := w.Write(data); err != nil {
			return nil, err
		}
		if err := w.Close(); err != nil {
			return nil, err
		}
		header.Add("Content-Encoding", "gzip")
		header.Add("Vary", "Accept-Encoding")
		header.Set("Content-Type", "application/json")
		return bytes.NewReader(buf.Bytes()), nil
	}
}
