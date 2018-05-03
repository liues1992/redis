package proto

import (
	"bufio"
	"fmt"
	"io"
	"strconv"

	"github.com/liues1992/redis-proxy/internal/util"
	"bytes"
)

const bytesAllocLimit = 1024 * 1024 // 1mb

const (
	ErrorReply  = '-'
	StatusReply = '+'
	IntReply    = ':'
	StringReply = '$'
	ArrayReply  = '*'

	ReqArgMark  = '*'
	ReqSizeMark  = '$'
)

//------------------------------------------------------------------------------

const Nil = RedisError("redis: nil")

type RedisError string

func (e RedisError) Error() string { return string(e) }

//------------------------------------------------------------------------------

type MultiBulkParse func(*Reader, int64) (interface{}, error)

type Reader struct {
	src      *bufio.Reader
	buf      []byte
	chainBuf [][]byte
	cacheBuf bool
}

func (r *Reader) ChainBuf() [][]byte {
	return r.chainBuf
}

func NewReader(rd io.Reader) *Reader {
	return &Reader{
		src: bufio.NewReader(rd),
		buf: make([]byte, 4096),
		cacheBuf: true,
	}
}

func (r *Reader) Reset(rd io.Reader) {
	r.src.Reset(rd)
	r.chainBuf = nil
}

func (r *Reader) ResetChainBuf() {
	r.chainBuf = nil
}

func (r *Reader) PeekBuffered() []byte {
	if n := r.src.Buffered(); n != 0 {
		b, _ := r.src.Peek(n)
		return b
	}
	return nil
}

func (r *Reader) ReadN(n int) ([]byte, error) {
	b, err := readN(r.src, r.buf, n)
	if r.cacheBuf {
		tmpBuf := make([]byte, n)
		copy(tmpBuf, b)
		r.chainBuf = append(r.chainBuf, tmpBuf)
	}

	if err != nil {
		return nil, err
	}
	r.buf = b
	return b, nil
}

func (r *Reader) ReadLine() ([]byte, error) {
	line, isPrefix, err := r.src.ReadLine()
	if r.cacheBuf {
		tmpBuf := make([]byte, len(line) + 2)
		tmpBuf[len(line)] = '\r'
		tmpBuf[len(line) + 1] = '\n'
		copy(tmpBuf, line)
		r.chainBuf = append(r.chainBuf, tmpBuf)
	}

	if err != nil {
		return nil, err
	}
	if isPrefix {
		return nil, bufio.ErrBufferFull
	}
	if len(line) == 0 {
		return nil, fmt.Errorf("redis: reply is empty")
	}
	if isNilReply(line) {
		return nil, Nil
	}
	return line, nil
}

func (r *Reader) ReadReply(m MultiBulkParse) (interface{}, error) {
	line, err := r.ReadLine()
	if err != nil {
		return nil, err
	}

	switch line[0] {
	case ErrorReply:
		return nil, ParseErrorReply(line)
	case StatusReply:
		return parseStatusValue(line), nil
	case IntReply:
		return util.ParseInt(line[1:], 10, 64)
	case StringReply:
		return r.readTmpBytesValue(line)
	case ArrayReply:
		n, err := parseArrayLen(line)
		if err != nil {
			return nil, err
		}
		return m(r, n)
	}
	return nil, fmt.Errorf("redis: can't parse %.100q", line)
}

func (r *Reader) ReadIntReply() (int64, error) {
	line, err := r.ReadLine()
	if err != nil {
		return 0, err
	}
	switch line[0] {
	case ErrorReply:
		return 0, ParseErrorReply(line)
	case IntReply:
		return util.ParseInt(line[1:], 10, 64)
	default:
		return 0, fmt.Errorf("redis: can't parse int reply: %.100q", line)
	}
}

func (r *Reader) ReadTmpBytesReply() ([]byte, error) {
	line, err := r.ReadLine()
	if err != nil {
		return nil, err
	}
	switch line[0] {
	case ErrorReply:
		return nil, ParseErrorReply(line)
	case StringReply:
		return r.readTmpBytesValue(line)
	case StatusReply:
		return parseStatusValue(line), nil
	default:
		return nil, fmt.Errorf("redis: can't parse string reply: %.100q", line)
	}
}

func (r *Reader) ReadBytesReply() ([]byte, error) {
	b, err := r.ReadTmpBytesReply()
	if err != nil {
		return nil, err
	}
	cp := make([]byte, len(b))
	copy(cp, b)
	return cp, nil
}

func (r *Reader) ReadStringReply() (string, error) {
	b, err := r.ReadTmpBytesReply()
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (r *Reader) ReadFloatReply() (float64, error) {
	b, err := r.ReadTmpBytesReply()
	if err != nil {
		return 0, err
	}
	return util.ParseFloat(b, 64)
}

func (r *Reader) ReadArrayReply(m MultiBulkParse) (interface{}, error) {
	line, err := r.ReadLine()
	if err != nil {
		return nil, err
	}
	switch line[0] {
	case ErrorReply:
		return nil, ParseErrorReply(line)
	case ArrayReply:
		n, err := parseArrayLen(line)
		if err != nil {
			return nil, err
		}
		return m(r, n)
	default:
		return nil, fmt.Errorf("redis: can't parse array reply: %.100q", line)
	}
}

func (r *Reader) ReadArrayLen() (int64, error) {
	line, err := r.ReadLine()
	if err != nil {
		return 0, err
	}
	switch line[0] {
	case ErrorReply:
		return 0, ParseErrorReply(line)
	case ArrayReply:
		return parseArrayLen(line)
	default:
		return 0, fmt.Errorf("redis: can't parse array reply: %.100q", line)
	}
}

func (r *Reader) ReadScanReply() ([]string, uint64, error) {
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, 0, err
	}
	if n != 2 {
		return nil, 0, fmt.Errorf("redis: got %d elements in scan reply, expected 2", n)
	}

	cursor, err := r.ReadUint()
	if err != nil {
		return nil, 0, err
	}

	n, err = r.ReadArrayLen()
	if err != nil {
		return nil, 0, err
	}

	keys := make([]string, n)
	for i := int64(0); i < n; i++ {
		key, err := r.ReadStringReply()
		if err != nil {
			return nil, 0, err
		}
		keys[i] = key
	}

	return keys, cursor, err
}

func (r *Reader) readTmpBytesValue(line []byte) ([]byte, error) {
	if isNilReply(line) {
		return nil, Nil
	}

	replyLen, err := strconv.Atoi(string(line[1:]))
	if err != nil {
		return nil, err
	}

	b, err := r.ReadN(replyLen + 2)
	if err != nil {
		return nil, err
	}
	return b[:replyLen], nil
}

func (r *Reader) ReadInt() (int64, error) {
	b, err := r.ReadTmpBytesReply()
	if err != nil {
		return 0, err
	}
	return util.ParseInt(b, 10, 64)
}

func (r *Reader) ReadUint() (uint64, error) {
	b, err := r.ReadTmpBytesReply()
	if err != nil {
		return 0, err
	}
	return util.ParseUint(b, 10, 64)
}

// --------------------------------------------------------------------

func readN(r io.Reader, b []byte, n int) ([]byte, error) {
	if n == 0 && b == nil {
		return make([]byte, 0), nil
	}

	if cap(b) >= n {
		b = b[:n]
		_, err := io.ReadFull(r, b)
		return b, err
	}
	b = b[:cap(b)]

	pos := 0
	for pos < n {
		diff := n - len(b)
		if diff > bytesAllocLimit {
			diff = bytesAllocLimit
		}
		b = append(b, make([]byte, diff)...)

		nn, err := io.ReadFull(r, b[pos:])
		if err != nil {
			return nil, err
		}
		pos += nn
	}

	return b, nil
}

func formatInt(n int64) string {
	return strconv.FormatInt(n, 10)
}

func formatUint(u uint64) string {
	return strconv.FormatUint(u, 10)
}

func formatFloat(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}

func isNilReply(b []byte) bool {
	return len(b) == 3 &&
		(b[0] == StringReply || b[0] == ArrayReply) &&
		b[1] == '-' && b[2] == '1'
}

func ParseErrorReply(line []byte) error {
	return RedisError(string(line[1:]))
}

func parseStatusValue(line []byte) []byte {
	return line[1:]
}

func parseArrayLen(line []byte) (int64, error) {
	if isNilReply(line) {
		return 0, Nil
	}
	return util.ParseInt(line[1:], 10, 64)
}

/** below is reply parser*/



func (r *Reader) ReadReq() ([]string, error) {
	r.cacheBuf = false
	line, err := r.ReadLine()
	if err != nil {
		return nil, err
	}
	switch line[0] {
	case ReqArgMark:
		argLen, err := parseArrayLen(line)
		if err != nil {
			return nil, fmt.Errorf("redis: can't parse req arg len: %.100q", line)
		}
		return r.ReadReqMultiBulk(argLen)
	default:
		return r.ReadReqInline(line)
	}
}

func (r *Reader) ReadReqInline(line []byte) ([]string, error) {
	var tmp = make([]byte, len(line))
	copy(tmp, line)
	var ret []string
	var sliced = bytes.Split(tmp, []byte{' '})

	for _, sub := range sliced {
		ret = append(ret, string(sub))
	}
	return ret, nil
}

func (r *Reader) ReadReqMultiBulk(argLen int64) ([]string, error) {
	if argLen <= 0 {
		return nil, fmt.Errorf("redis: read req arg len zero")
	}

	var i int64
	var ret []string
	for i = 0; i < argLen; i++ {
		item, err := r.ReadReqArg()
		if err != nil {
			return nil, err
		}
		ret = append(ret, string(item))
	}
	return ret, nil
}


func (r *Reader) ReadReqArgLen() (int64, error) {
	line, err := r.ReadLine()
	if err != nil {
		return 0, err
	}
	switch line[0] {
	case ReqArgMark:
		return parseArrayLen(line)
	default:
		return 0, fmt.Errorf("redis: can't parse req arg len: %.100q", line)
	}
}

func (r *Reader) ReadReqArg() ([]byte, error) {
	line, err := r.ReadLine()
	if err != nil {
		return nil, err
	}
	switch line[0] {
	case ReqSizeMark:
		return r.readTmpBytesValue(line)
	}
	return nil, fmt.Errorf("redis: can't parse req arg %.100q", line)
}

func (r *Reader) HasMore() bool {
	return r.src.Buffered() > 0
}