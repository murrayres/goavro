package goavro_test

import (
	"testing"

	"github.com/karrick/goavro"
)

func TestSchemaFixed(t *testing.T) {
	testSchemaValid(t, `{"type": "fixed", "size": 16, "name": "md5"}`)
}

func TestFixedName(t *testing.T) {
	testSchemaInvalid(t, `{"type":"fixed","size":16}`, "Fixed ought to have valid name: schema ought to have name key")
	testSchemaInvalid(t, `{"type":"fixed","name":3}`, "Fixed ought to have valid name: schema name ought to be non-empty string")
	testSchemaInvalid(t, `{"type":"fixed","name":""}`, "Fixed ought to have valid name: schema name ought to be non-empty string")
	testSchemaInvalid(t, `{"type":"fixed","name":"&foo","size":16}`, "Fixed ought to have valid name: schema name ought to start with")
	testSchemaInvalid(t, `{"type":"fixed","name":"foo&","size":16}`, "Fixed ought to have valid name: schema name ought to have second and remaining")
}

func TestFixedSize(t *testing.T) {
	testSchemaInvalid(t, `{"type":"fixed","name":"f1"}`, `Fixed "f1" ought to have size key`)
	testSchemaInvalid(t, `{"type":"fixed","name":"f1","size":"16"}`, `Fixed "f1" size ought to be number greater than zero`)
	testSchemaInvalid(t, `{"type":"fixed","name":"f1","size":-1}`, `Fixed "f1" size ought to be number greater than zero`)
	testSchemaInvalid(t, `{"type":"fixed","name":"f1","size":0}`, `Fixed "f1" size ought to be number greater than zero`)
}

func TestFixedDecodeBufferUnderflow(t *testing.T) {
	testBinaryDecodeFail(t, `{"type":"fixed","name":"md5","size":16}`, nil, "short buffer")
}

func TestFixedDecodeWithExtra(t *testing.T) {
	c, err := goavro.NewCodec(`{"type":"fixed","name":"foo","size":4}`)
	if err != nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, nil)
	}
	val, buf, err := c.BinaryDecode([]byte("abcdefgh"))
	if actual, expected := string(val.([]byte)), "abcd"; actual != expected {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}
	if actual, expected := string(buf), "efgh"; actual != expected {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}
	if err != nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, nil)
	}
}

func TestFixedEncodeUnsupportedType(t *testing.T) {
	testBinaryEncodeFailBadDatumType(t, `{"type":"fixed","name":"foo","size":4}`, 13)
}

func TestFixedEncodeWrongSize(t *testing.T) {
	testBinaryEncodeFail(t, `{"type":"fixed","name":"foo","size":4}`, []byte("abcde"), "datum size ought to equal schema size")
	testBinaryEncodeFail(t, `{"type":"fixed","name":"foo","size":4}`, []byte("abc"), "datum size ought to equal schema size")
}

func TestFixedEncode(t *testing.T) {
	testBinaryCodecPass(t, `{"type":"fixed","name":"foo","size":4}`, []byte("abcd"), []byte("abcd"))
}
