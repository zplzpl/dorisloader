package dorisloader

import (
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
)

var (
	Version = "1.0.0"
)

type Client struct {
	c                 Doer         // e.g. a net/*http.Client to use for requests
	mu                sync.RWMutex // guards the next block
	feUrl             string       // fe node url info http://fehost:feport/
	basicAuth         bool         // indicates whether to send HTTP Basic Auth credentials
	basicAuthUsername string       // username for HTTP Basic Auth
	basicAuthPassword string       // password for HTTP Basic Auth
	headers           http.Header  // a list of default headers to add to each request
	decoder           Decoder
	debug             bool
}

func NewClient(feUrl string, options ...ClientOptionFunc) (*Client, error) {

	// Set up the client
	c := &Client{
		c:       http.DefaultClient,
		feUrl:   feUrl,
		decoder: &DefaultDecoder{},
	}

	// Run the options on it
	for _, option := range options {
		if err := option(c); err != nil {
			return nil, err
		}
	}

	return c, nil
}

// Doer is an interface to perform HTTP requests.
// It can be used for mocking.
type Doer interface {
	Do(*http.Request) (*http.Response, error)
}

// ClientOptionFunc is a function that configures a Client.
// It is used in NewClient.
type ClientOptionFunc func(*Client) error

// SetHttpClient can be used to specify the http.Client to use when making
func SetHttpClient(httpClient Doer) ClientOptionFunc {
	return func(c *Client) error {
		if httpClient != nil {
			c.c = httpClient
		} else {
			c.c = http.DefaultClient
		}
		return nil
	}
}

// SetHttpClient can be used to specify the http.Client to use when making
func SetDebug(debug bool) ClientOptionFunc {
	return func(c *Client) error {
		c.debug = debug
		return nil
	}
}

// SetBasicAuth can be used to specify the HTTP Basic Auth credentials to
func SetBasicAuth(username, password string) ClientOptionFunc {
	return func(c *Client) error {
		c.basicAuthUsername = username
		c.basicAuthPassword = password
		c.basicAuth = c.basicAuthUsername != "" || c.basicAuthPassword != ""
		return nil
	}
}

// SetHeaders adds a list of default HTTP headers that will be added to
// each requests executed by PerformRequest.
func SetHeaders(headers http.Header) ClientOptionFunc {
	return func(c *Client) error {
		c.headers = headers
		return nil
	}
}

// PerformRequestOptions must be passed into PerformRequest.
type PerformRequestOptions struct {
	Method       string
	Path         string
	Params       url.Values
	Body         interface{}
	ContentType  string
	IgnoreErrors []int
	//Retrier         Retrier
	Headers         http.Header
	MaxResponseSize int64
}

// PerformRequest does a HTTP request.
// It returns a response (which might be nil) and an error on failure.
//
// Optionally, a list of HTTP error codes to ignore can be passed.
// This is necessary for services that expect e.g. HTTP status 404 as a
// valid outcome (Exists, IndicesExists, IndicesTypeExists).
func (c *Client) PerformRequest(ctx context.Context, opt PerformRequestOptions) (*Response, error) {

	c.mu.RLock()
	basicAuth := c.basicAuth
	basicAuthUsername := c.basicAuthUsername
	basicAuthPassword := c.basicAuthPassword
	defaultHeaders := c.headers
	c.mu.RUnlock()

	var err error
	var req *Request
	var resp *Response

	pathWithParams := opt.Path

	bodyReader, err := handleGetBodyReader(opt.Headers, opt.Body, false)
	if err != nil {
		return nil, err
	}

	req, err = NewRequest(opt.Method, c.feUrl+pathWithParams, bodyReader)
	if err != nil {
		return nil, err
	}

	if basicAuth {
		req.SetBasicAuth(basicAuthUsername, basicAuthPassword)
	}

	if opt.ContentType != "" {
		req.Header.Set("Content-Type", opt.ContentType)
	}

	if len(opt.Headers) > 0 {
		for key, value := range opt.Headers {
			for _, v := range value {
				req.Header.Add(key, v)
			}
		}
	}

	if len(defaultHeaders) > 0 {
		for key, value := range defaultHeaders {
			for _, v := range value {
				req.Header.Add(key, v)
			}
		}
	}

	// Tracing
	c.dumpRequest((*http.Request)(req))

	// Get response
	res, err := c.c.Do((*http.Request)(req).WithContext(ctx))
	if res != nil && res.Body != nil {
		defer res.Body.Close()
	}
	if IsContextErr(err) {
		// Proceed, but don't mark the node as dead
		return nil, err
	}
	if err != nil {
		return nil, err
	}

	resp, err = c.newResponse(res)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

// IsContextErr returns true if the error is from a context that was canceled or deadline exceeded
func IsContextErr(err error) bool {
	if err == context.Canceled || err == context.DeadlineExceeded {
		return true
	}
	// This happens e.g. on redirect errors, see https://golang.org/src/net/http/client_test.go#L329
	if ue, ok := err.(*url.Error); ok {
		if ue.Temporary() {
			return true
		}
		// Use of an AWS Signing Transport can result in a wrapped url.Error
		return IsContextErr(ue.Err)
	}
	return false
}

// newResponse creates a new response from the HTTP response.
func (c *Client) newResponse(res *http.Response) (*Response, error) {
	r := &Response{
		StatusCode:          res.StatusCode,
		Header:              res.Header,
		DeprecationWarnings: res.Header["Warning"],
	}
	if res.Body != nil {
		body := io.Reader(res.Body)
		slurp, err := ioutil.ReadAll(body)
		if err != nil {
			return nil, err
		}
		// HEAD requests return a body but no content
		if len(slurp) > 0 {
			r.Body = json.RawMessage(slurp)
		}
	}
	return r, nil
}

// dumpRequest dumps the given HTTP request to the trace log.
func (c *Client) dumpRequest(r *http.Request) {
	if !c.debug {
		return
	}

	out, err := httputil.DumpRequestOut(r, true)
	if err == nil {
		log.Println(string(out))
	}
}
