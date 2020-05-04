package dorisloader

import "encoding/json"

// Decoder is used to decode responses from Elasticsearch.
// Users of elastic can implement their own marshaler for advanced purposes
// and set them per Client (see SetDecoder). If none is specified,
// DefaultDecoder is used.
type Decoder interface {
	Decode(data []byte, v interface{}) error
}

// DefaultDecoder uses json.Unmarshal from the Go standard library
// to decode JSON data.
type DefaultDecoder struct{}

// Decode decodes with json.Unmarshal from the Go standard library.
func (u *DefaultDecoder) Decode(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}
