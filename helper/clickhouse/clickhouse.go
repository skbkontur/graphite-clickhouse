package clickhouse

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/lomik/graphite-clickhouse/pkg/scope"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type ErrDataParse struct {
	err  string
	data string
}

func NewErrDataParse(err string, data string) error {
	return &ErrDataParse{err, data}
}

func (e *ErrDataParse) Error() string {
	return fmt.Sprintf("%s: %s", e.err, e.data)
}

func (e *ErrDataParse) PrependDescription(test string) {
	e.data = test + e.data
}

type ErrorWithCode struct {
	err  string
	Code int // error code
}

func NewErrorWithCode(err string, code int) error {
	return &ErrorWithCode{err, code}
}

func (e *ErrorWithCode) Error() string { return e.err }

var ErrUvarintRead = errors.New("ReadUvarint: Malformed array")
var ErrUvarintOverflow = errors.New("ReadUvarint: varint overflows a 64-bit integer")
var ErrClickHouseResponse = errors.New("Malformed response from clickhouse")

func HandleError(w http.ResponseWriter, err error) {
	netErr, ok := err.(net.Error)
	if ok {
		if netErr.Timeout() {
			http.Error(w, "Storage read timeout", http.StatusGatewayTimeout)
		} else if strings.HasSuffix(err.Error(), "connect: no route to host") ||
			strings.HasSuffix(err.Error(), "connect: connection refused") ||
			strings.HasSuffix(err.Error(), ": connection reset by peer") ||
			strings.HasPrefix(err.Error(), "dial tcp: lookup ") { // DNS lookup
			http.Error(w, "Storage error", http.StatusServiceUnavailable)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	errCode, ok := err.(*ErrorWithCode)
	if ok {
		if (errCode.Code > 500 && errCode.Code < 512) ||
			errCode.Code == http.StatusBadRequest || errCode.Code == http.StatusForbidden {
			http.Error(w, html.EscapeString(errCode.Error()), errCode.Code)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	_, ok = err.(*ErrDataParse)
	if ok || strings.HasPrefix(err.Error(), "clickhouse response status 500: Code:") {
		if strings.Contains(err.Error(), ": Limit for ") {
			//logger.Info("limit", zap.Error(err))
			http.Error(w, "Storage read limit", http.StatusForbidden)
		} else if !ok && strings.HasPrefix(err.Error(), "clickhouse response status 500: Code: 170,") {
			// distributed table configuration error
			// clickhouse response status 500: Code: 170, e.displayText() = DB::Exception: Requested cluster 'cluster' not found
			http.Error(w, "Storage configuration error", http.StatusServiceUnavailable)
		}
	}
	// DISABLED for prevent retry on balancers: check for context.Canceled - some strange errors when limit is reached
	// if errors.Is(err, context.Canceled) {
	// 	http.Error(w, "Storage read context canceled", http.StatusGatewayTimeout)
	// }
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

type Options struct {
	Timeout        time.Duration
	ConnectTimeout time.Duration
}

type loggedReader struct {
	reader   io.ReadCloser
	logger   *zap.Logger
	start    time.Time
	finished bool
	queryID  string
}

func (r *loggedReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if err != nil && !r.finished {
		r.finished = true
		r.logger.Info("query", zap.String("query_id", r.queryID), zap.Duration("time", time.Since(r.start)))
	}
	return n, err
}

func (r *loggedReader) Close() error {
	err := r.reader.Close()
	if !r.finished {
		r.finished = true
		r.logger.Info("query", zap.String("query_id", r.queryID), zap.Duration("time", time.Since(r.start)))
	}
	return err
}

func formatSQL(q string) string {
	s := strings.Split(q, "\n")
	for i := 0; i < len(s); i++ {
		s[i] = strings.TrimSpace(s[i])
	}

	return strings.Join(s, " ")
}

func Query(ctx context.Context, dsn string, query string, opts Options, extData *ExternalData) ([]byte, error) {
	return Post(ctx, dsn, query, nil, opts, extData)
}

func Post(ctx context.Context, dsn string, query string, postBody io.Reader, opts Options, extData *ExternalData) ([]byte, error) {
	return do(ctx, dsn, query, postBody, false, opts, extData)
}

func PostGzip(ctx context.Context, dsn string, query string, postBody io.Reader, opts Options, extData *ExternalData) ([]byte, error) {
	return do(ctx, dsn, query, postBody, true, opts, extData)
}

func Reader(ctx context.Context, dsn string, query string, opts Options, extData *ExternalData) (io.ReadCloser, error) {
	return reader(ctx, dsn, query, nil, false, opts, extData)
}

func reader(ctx context.Context, dsn string, query string, postBody io.Reader, gzip bool, opts Options, extData *ExternalData) (bodyReader io.ReadCloser, err error) {
	if postBody != nil && extData != nil {
		err = fmt.Errorf("postBody and extData could not be passed in one request")
		return
	}

	var chQueryID string

	start := time.Now()

	requestID := scope.RequestID(ctx)

	queryForLogger := query
	if len(queryForLogger) > 500 {
		queryForLogger = queryForLogger[:395] + "<...>" + queryForLogger[len(queryForLogger)-100:]
	}
	logger := scope.Logger(ctx).With(zap.String("query", formatSQL(queryForLogger)))

	defer func() {
		// fmt.Println(time.Since(start), formatSQL(queryForLogger))
		if err != nil {
			logger.Error("query", zap.Error(err), zap.Duration("time", time.Since(start)))
		}
	}()

	p, err := url.Parse(dsn)
	if err != nil {
		return
	}

	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], rand.Uint64())
	queryID := fmt.Sprintf("%x", b)

	q := p.Query()
	q.Set("query_id", fmt.Sprintf("%s::%s", requestID, queryID))
	// Get X-Clickhouse-Summary header
	// TODO: remove when https://github.com/ClickHouse/ClickHouse/issues/16207 is done
	q.Set("send_progress_in_http_headers", "1")
	q.Set("http_headers_progress_interval_ms", "10000")
	p.RawQuery = q.Encode()

	var contentHeader string
	if postBody != nil {
		q := p.Query()
		q.Set("query", query)
		p.RawQuery = q.Encode()
	} else if extData != nil {
		q := p.Query()
		q.Set("query", query)
		p.RawQuery = q.Encode()
		postBody, contentHeader, err = extData.buildBody(ctx, p)
		if err != nil {
			return
		}
	} else {
		postBody = strings.NewReader(query)
	}

	url := p.String()

	req, err := http.NewRequest("POST", url, postBody)
	if err != nil {
		return
	}

	req.Header.Add("User-Agent", scope.ClickhouseUserAgent(ctx))
	if contentHeader != "" {
		req.Header.Add("Content-Type", contentHeader)
	}

	if gzip {
		req.Header.Add("Content-Encoding", "gzip")
	}

	client := &http.Client{
		Timeout: opts.Timeout,
		Transport: &http.Transport{
			Dial: (&net.Dialer{
				Timeout: opts.ConnectTimeout,
			}).Dial,
			DisableKeepAlives: true,
		},
	}
	resp, err := client.Do(req)
	if err != nil {
		return
	}

	// chproxy overwrite our query id. So read it again
	chQueryID = resp.Header.Get("X-ClickHouse-Query-Id")

	summaryHeader := resp.Header.Get("X-Clickhouse-Summary")
	if len(summaryHeader) > 0 {
		summary := make(map[string]string)
		err = json.Unmarshal([]byte(summaryHeader), &summary)
		if err == nil {
			// TODO: use in carbon metrics sender when it will be implemented
			fields := make([]zapcore.Field, 0, len(summary))
			for k, v := range summary {
				fields = append(fields, zap.String(k, v))
			}
			logger = logger.With(fields...)
		} else {
			logger.Warn("query", zap.Error(err), zap.String("clickhouse-summary", summaryHeader))
			err = nil
		}
	}

	// check for return 5xx error, may be 502 code if clickhouse accesed via reverse proxy
	if resp.StatusCode > 500 && resp.StatusCode < 512 {
		body, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		err = NewErrorWithCode(string(body), resp.StatusCode)
		return
	} else if resp.StatusCode != 200 {
		body, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		err = fmt.Errorf("clickhouse response status %d: %s", resp.StatusCode, string(body))
		return
	}

	bodyReader = &loggedReader{
		reader:  resp.Body,
		logger:  logger,
		start:   start,
		queryID: chQueryID,
	}

	return
}

func do(ctx context.Context, dsn string, query string, postBody io.Reader, gzip bool, opts Options, extData *ExternalData) ([]byte, error) {
	bodyReader, err := reader(ctx, dsn, query, postBody, gzip, opts, extData)
	if err != nil {
		return nil, err
	}

	body, err := ioutil.ReadAll(bodyReader)
	bodyReader.Close()
	if err != nil {
		return nil, err
	}

	return body, nil
}

func ReadUvarint(array []byte) (uint64, int, error) {
	var x uint64
	var s uint
	l := len(array) - 1
	for i := 0; ; i++ {
		if i > l {
			return x, i + 1, ErrUvarintRead
		}
		if array[i] < 0x80 {
			if i > 9 || i == 9 && array[i] > 1 {
				return x, i + 1, ErrUvarintOverflow
			}
			return x | uint64(array[i])<<s, i + 1, nil
		}
		x |= uint64(array[i]&0x7f) << s
		s += 7
	}
}
