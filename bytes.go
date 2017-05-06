package goavro

import (
	"encoding/hex"
	"fmt"
	"io"
	"strconv"
	"unicode/utf8"
)

////////////////////////////////////////
// Binary Decode
////////////////////////////////////////

func bytesBinaryDecoder(buf []byte) (interface{}, []byte, error) {
	if len(buf) < 1 {
		return nil, nil, io.ErrShortBuffer
	}
	var decoded interface{}
	var err error
	if decoded, buf, err = longBinaryDecoder(buf); err != nil {
		return nil, buf, fmt.Errorf("bytes: %s", err)
	}
	size := decoded.(int64) // longDecoder always returns int64
	if size < 0 {
		return nil, buf, fmt.Errorf("bytes: negative length: %d", size)
	}
	if size > int64(len(buf)) {
		return nil, buf, io.ErrShortBuffer
	}
	return buf[:size], buf[size:], nil
}

func stringBinaryDecoder(buf []byte) (interface{}, []byte, error) {
	d, b, err := bytesBinaryDecoder(buf)
	if err != nil {
		return nil, buf, err
	}
	return string(d.([]byte)), b, nil
}

////////////////////////////////////////
// Binary Encode
////////////////////////////////////////

func bytesBinaryEncoder(buf []byte, datum interface{}) ([]byte, error) {
	var value []byte
	switch v := datum.(type) {
	case []byte:
		value = v
	case string:
		value = []byte(v)
	default:
		return buf, fmt.Errorf("bytes: expected: Go string or []byte; received: %T", v)
	}
	// longEncoder only fails when given non int, so elide error checking
	buf, _ = longBinaryEncoder(buf, len(value))
	// append datum bytes
	return append(buf, value...), nil
}

func stringBinaryEncoder(buf []byte, datum interface{}) ([]byte, error) {
	return bytesBinaryEncoder(buf, datum)
}

////////////////////////////////////////
// Text Decode
////////////////////////////////////////

func bytesTextDecoder(buf []byte) (interface{}, []byte, error) {
	// var tmp [utf8.UTFMax]byte
	var tmp [1]byte
	// scan each character, being mindful of escape sequence. once find unescaped quote, we're done
	buflen := len(buf)
	if buflen < 2 {
		return nil, buf, io.ErrShortBuffer
	}
	if buf[0] != '"' {
		return nil, buf, fmt.Errorf("expected initial \"; found: %c", buf[0])
	}

	var newBytes []byte
	var escaped bool

	// Loop through all remaining bytes, but note we will terminate early when find unescaped double quote.
	var i, l int

	for i, l = 1, buflen-1; i < l; i++ {
		b := buf[i]
		if escaped {
			switch b {
			case '"':
				newBytes = append(newBytes, '"')
			case '\\':
				newBytes = append(newBytes, '\\')
			case '/':
				newBytes = append(newBytes, '/')
			case 'b':
				newBytes = append(newBytes, '\b')
			case 'f':
				newBytes = append(newBytes, '\f')
			case 'n':
				newBytes = append(newBytes, '\n')
			case 'r':
				newBytes = append(newBytes, '\r')
			case 't':
				newBytes = append(newBytes, '\t')
			case 'u':
				if i > buflen-6 {
					return nil, buf, io.ErrShortBuffer
				}
				_, err := hex.Decode(tmp[:], buf[i+3:i+5])
				if err != nil {
					return nil, buf, err
				}
				newBytes = append(newBytes, tmp[:]...)
				i += 4 // absorb 4 hexidecimal characters
			default:
				newBytes = append(newBytes, b)
			}
			escaped = false
			continue
		}
		if b == '\\' {
			escaped = true
			continue
		}
		if b == '"' {
			break
		}
		newBytes = append(newBytes, buf[i])
	}
	if b := buf[buflen-1]; b != '"' {
		return nil, buf, fmt.Errorf("expected final \"; found: %c", b)
	}
	return newBytes, buf[i+1:], nil
}

func stringTextDecoder(buf []byte) (interface{}, []byte, error) {
	var tmp [utf8.UTFMax]byte
	// scan each character, being mindful of escape sequence. once find unescaped quote, we're done
	buflen := len(buf)
	if buflen < 2 {
		return nil, buf, io.ErrShortBuffer
	}
	if buf[0] != '"' {
		return nil, buf, fmt.Errorf("expected initial \"; found: %c", buf[0])
	}

	var newBytes []byte
	var escaped bool

	// Loop through all remaining bytes, but note we will terminate early when find unescaped double quote.
	var i, l int

	for i, l = 1, buflen-1; i < l; i++ {
		b := buf[i]
		if escaped {
			switch b {
			case '"':
				newBytes = append(newBytes, '"')
			case '\\':
				newBytes = append(newBytes, '\\')
			case '/':
				newBytes = append(newBytes, '/')
			case 'b':
				newBytes = append(newBytes, '\b')
			case 'f':
				newBytes = append(newBytes, '\f')
			case 'n':
				newBytes = append(newBytes, '\n')
			case 'r':
				newBytes = append(newBytes, '\r')
			case 't':
				newBytes = append(newBytes, '\t')
			case 'u':
				if i > buflen-6 {
					return nil, buf, io.ErrShortBuffer
				}
				blob := buf[i+1 : i+5]
				v, err := strconv.ParseUint(string(blob), 16, 64)
				if err != nil {
					return nil, buf, err
				}
				width := utf8.EncodeRune(tmp[:], rune(v))
				newBytes = append(newBytes, tmp[:width]...)
				i += 4 // absorb 4 hexidecimal characters
			default:
				newBytes = append(newBytes, b)
			}
			escaped = false
			continue
		}
		if b == '\\' {
			escaped = true
			continue
		}
		if b == '"' {
			break
		}
		newBytes = append(newBytes, buf[i])
	}
	if b := buf[buflen-1]; b != '"' {
		return nil, buf, fmt.Errorf("expected final \"; found: %c", b)
	}
	return string(newBytes), buf[i+1:], nil
}

////////////////////////////////////////
// Text Encode
////////////////////////////////////////

const hexDigits = "0123456789ABCDEF"

func bytesTextEncoder(buf []byte, datum interface{}) ([]byte, error) {
	someBytes, ok := datum.([]byte)
	if !ok {
		// panic("string rather than []byte")
		return buf, fmt.Errorf("bytes: expected: []byte; received: %T", datum)
	}
	buf = append(buf, '"')
	for _, b := range someBytes {
		buf = appendMaybeEscapedByte(buf, b)
	}
	return append(buf, '"'), nil
}

func appendMaybeEscapedByte(buf []byte, b byte) []byte {
	if b < utf8.RuneSelf {
		// NOTE: The following 6 special JSON characters must be escaped:
		switch b {
		case '"', '\\', '/':
			return append(buf, []byte{'\\', b}...)
		case '\b':
			return append(buf, []byte("\\b")...)
		case '\f':
			return append(buf, []byte("\\f")...)
		case '\n':
			return append(buf, []byte("\\n")...)
		case '\r':
			return append(buf, []byte("\\r")...)
		case '\t':
			return append(buf, []byte("\\t")...)
		default:
			return append(buf, b) // typical characters
		}
	}
	return append(append(append(buf, []byte("\\u00")...), hexDigits[b>>4]), hexDigits[b&0x0f])
}

func stringTextEncoder(buf []byte, datum interface{}) ([]byte, error) {
	someString, ok := datum.(string)
	if !ok {
		// panic("[]byte rather than string")
		return buf, fmt.Errorf("bytes: expected: string; received: %T", datum)
	}
	buf = append(buf, '"')
	for _, r := range someString {
		buf = appendMaybeEscapedRune(buf, r)
	}
	return append(buf, '"'), nil
}

// While slices in Go are never constants, we can initialize them once and reuse
// them many times.
var (
	sliceQuote          = []byte("\\\"")
	sliceBackslash      = []byte("\\\\")
	sliceSlash          = []byte("\\/")
	sliceBackspace      = []byte("\\b")
	sliceFormfeed       = []byte("\\f")
	sliceNewline        = []byte("\\n")
	sliceCarriageReturn = []byte("\\r")
	sliceTab            = []byte("\\t")
)

func appendMaybeEscapedRune(buf []byte, r rune) []byte {
	if r < utf8.RuneSelf {
		switch r {
		case '"':
			return append(buf, sliceQuote...)
		case '\\':
			return append(buf, sliceBackslash...)
		case '/':
			return append(buf, sliceSlash...)
		case '\b':
			return append(buf, sliceBackspace...)
		case '\f':
			return append(buf, sliceFormfeed...)
		case '\n':
			return append(buf, sliceNewline...)
		case '\r':
			return append(buf, sliceCarriageReturn...)
		case '\t':
			return append(buf, sliceTab...)
		default:
			return append(buf, uint8(r))
		}
	}
	return strconv.AppendInt(append(append(buf, []byte("\\u")...)), int64(r), 16)
}
