package dorisloader

import (
	"context"
	"errors"
	"net/http"
	"strings"
)

const (
	BULK_HEADER_LABEL_KEY = "label"
)

type BulkService struct {
	c     *Client
	rows  [][]byte
	db    string
	table string

	// load option
	// 导入任务的标识
	label string
	// 导入任务的最大容忍率，默认为0容忍，取值范围是0~1
	maxFilterRatio float64
	// 导入任务指定的过滤条件
	where string
	// 待导入表的 Partition 信息
	partition string
	// 待导入数据的函数变换配置
	columns string
	// 导入内存限制
	execMemLimit int64
	// Stream load 导入可以开启 strict mode 模式
	strictMode bool

	headers http.Header // custom request-level HTTP headers

	// estimated bulk size in bytes, up to the request index sizeInBytesCursor
	sizeInBytes       int64
	sizeInBytesCursor int
}

func NewBulkService(c *Client) *BulkService {

	b := &BulkService{c: c}
	b.Header("Expect", "100-continue")

	return b
}

type BulkResponse struct {
	TxnID                int    `json:"TxnId"`
	Label                string `json:"Label"`
	Status               string `json:"Status"`
	ExistingJobStatus    string `json:"ExistingJobStatus"`
	Message              string `json:"Message"`
	NumberTotalRows      int    `json:"NumberTotalRows"`
	NumberLoadedRows     int    `json:"NumberLoadedRows"`
	NumberFilteredRows   int    `json:"NumberFilteredRows"`
	NumberUnselectedRows int    `json:"NumberUnselectedRows"`
	LoadBytes            int    `json:"LoadBytes"`
	LoadTimeMs           int    `json:"LoadTimeMs"`
	ErrorURL             string `json:"ErrorURL"`
}

func (s *BulkService) DB(db string) *BulkService {
	s.db = db
	return s
}

func (s *BulkService) Table(table string) *BulkService {
	s.table = table
	return s
}

func (s *BulkService) MaxFilterRatio(maxFilterRatio float64) *BulkService {
	s.maxFilterRatio = maxFilterRatio
	return s
}

func (s *BulkService) Label(label string) *BulkService {
	s.label = label
	s.Header(BULK_HEADER_LABEL_KEY, label)
	return s
}

func (s *BulkService) Where(where string) *BulkService {
	s.where = where
	return s
}

func (s *BulkService) Partition(partition string) *BulkService {
	s.partition = partition
	return s
}

func (s *BulkService) Columns(columns string) *BulkService {
	s.columns = columns
	return s
}

func (s *BulkService) ExecMemLimit(execMemLimit int64) *BulkService {
	s.execMemLimit = execMemLimit
	return s
}

func (s *BulkService) StrictMode(strictMode bool) *BulkService {
	s.strictMode = strictMode
	return s
}

func (s *BulkService) Header(name string, value string) *BulkService {
	if s.headers == nil {
		s.headers = http.Header{}
	}
	s.headers.Add(name, value)
	return s
}

func (s *BulkService) Headers(headers http.Header) *BulkService {
	s.headers = headers
	return s
}

func (s *BulkService) EstimatedSizeInBytes() int64 {
	if s.sizeInBytesCursor == len(s.rows) {
		return s.sizeInBytes
	}
	for _, r := range s.rows[s.sizeInBytesCursor:] {
		s.sizeInBytes += s.estimateSizeInBytes(r)
		s.sizeInBytesCursor++
	}
	return s.sizeInBytes
}

func (s *BulkService) estimateSizeInBytes(r []byte) int64 {
	return int64(len(r))
}

func (s *BulkService) NumberOfRows() int {
	return len(s.rows)
}

func (s *BulkService) bodyAsString() (string, error) {
	// Pre-allocate to reduce allocs
	var buf strings.Builder
	buf.Grow(int(s.EstimatedSizeInBytes()))

	for _, row := range s.rows {

		buf.Write(row)
		buf.WriteByte('\n')

	}

	return buf.String(), nil
}

func (s *BulkService) buildUrlPath() string {
	path := "/api/"
	path = path + s.db + "/"
	path = path + s.table + "/_stream_load"
	return path
}

func (s *BulkService) Reset() {
	s.rows = make([][]byte, 0)
	s.sizeInBytes = 0
	s.sizeInBytesCursor = 0
}

func (s *BulkService) Add(rows ...[]byte) *BulkService {
	s.rows = append(s.rows, rows...)
	return s
}

func (s *BulkService) Do(ctx context.Context) (*BulkResponse, error) {

	if s.NumberOfRows() == 0 {
		return nil, errors.New("No bulk rows to commit")
	}

	// Get body
	body, err := s.bodyAsString()
	if err != nil {
		return nil, err
	}

	// Build url
	path := s.buildUrlPath()

	// Get response
	res, err := s.c.PerformRequest(ctx, PerformRequestOptions{
		Method:  "PUT",
		Path:    path,
		Body:    body,
		Headers: s.headers,
	})
	if err != nil {
		return nil, err
	}

	// Return results
	ret := new(BulkResponse)
	if err := s.c.decoder.Decode(res.Body, ret); err != nil {
		return nil, err
	}

	// Reset so the request can be reused
	s.Reset()

	return ret, nil
}
