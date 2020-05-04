package dorisloader

import (
	"encoding/json"
	"net/http"
)

// Response represents a response from Elasticsearch.
type Response struct {
	// StatusCode is the HTTP status code, e.g. 200.
	StatusCode int
	// Header is the HTTP header from the HTTP response.
	// Keys in the map are canonicalized (see http.CanonicalHeaderKey).
	Header http.Header
	// Body is the deserialized response body.
	Body json.RawMessage
	// DeprecationWarnings lists all deprecation warnings returned from
	DeprecationWarnings []string
}
