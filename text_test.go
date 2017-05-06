package goavro_test

import (
	"bytes"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/karrick/goavro"
)

func testTextDecodeFail(t *testing.T, schema string, buf []byte, errorMessage string) {
	c, err := goavro.NewCodec(schema)
	if err != nil {
		t.Fatal(err)
	}
	value, newBuffer, err := c.TextDecode(buf)
	if err == nil || !strings.Contains(err.Error(), errorMessage) {
		t.Errorf("Actual: %v; Expected: %s", err, errorMessage)
	}
	if value != nil {
		t.Errorf("Actual: %v; Expected: %v", value, nil)
	}
	if !bytes.Equal(buf, newBuffer) {
		t.Errorf("Actual: %v; Expected: %v", newBuffer, buf)
	}
}

func testTextEncodeFail(t *testing.T, schema string, datum interface{}, errorMessage string) {
	c, err := goavro.NewCodec(schema)
	if err != nil {
		t.Fatal(err)
	}
	buf, err := c.TextEncode(nil, datum)
	if err == nil || !strings.Contains(err.Error(), errorMessage) {
		t.Errorf("Actual: %v; Expected: %s", err, errorMessage)
	}
	if buf != nil {
		t.Errorf("Actual: %v; Expected: %v", buf, nil)
	}
}

func testTextEncodeFailBadDatumType(t *testing.T, schema string, datum interface{}) {
	testTextEncodeFail(t, schema, datum, "received: ")
}

func testTextDecodeFailShortBuffer(t *testing.T, schema string, buf []byte) {
	testTextDecodeFail(t, schema, buf, "short buffer")
}

func testTextDecodePass(t *testing.T, schema string, datum interface{}, encoded []byte) {
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		t.Fatalf("schema: %s; %s", schema, err)
	}

	decoded, remaining, err := codec.TextDecode(encoded)
	if err != nil {
		t.Fatalf("schema: %s; %s", schema, err)
	}

	// remaining ought to be empty because there is nothing remaining to be
	// decoded
	if actual, expected := len(remaining), 0; actual != expected {
		t.Errorf("schema: %s; Datum: %#v; Actual: %v; Expected: %v", schema, datum, actual, expected)
	}

	var datumIsNumerical, datumIsString bool
	var datumFloat float64
	var datumString string
	switch v := datum.(type) {
	case float64:
		datumFloat = v
		datumIsNumerical = true
	case float32:
		datumFloat = float64(v)
		datumIsNumerical = true
	case int:
		datumFloat = float64(v)
		datumIsNumerical = true
	case int32:
		datumFloat = float64(v)
		datumIsNumerical = true
	case int64:
		datumFloat = float64(v)
		datumIsNumerical = true
	case string:
		datumString = v
		datumIsString = true
	}

	var decodedIsNumerical, decodedIsString bool
	var decodedFloat float64
	var decodedString string
	switch v := decoded.(type) {
	case float64:
		decodedFloat = v
		decodedIsNumerical = true
	case float32:
		decodedFloat = float64(v)
		decodedIsNumerical = true
	case int:
		decodedFloat = float64(v)
		decodedIsNumerical = true
	case int32:
		decodedFloat = float64(v)
		decodedIsNumerical = true
	case int64:
		decodedFloat = float64(v)
		decodedIsNumerical = true
	case string:
		decodedString = v
		decodedIsString = true
	}

	// NOTE: Special handling when both datum and decoded values are floating
	// point to test whether both are NaN, -Inf, or +Inf.
	if datumIsNumerical && decodedIsNumerical {
		if (math.IsNaN(datumFloat) != math.IsNaN(decodedFloat)) &&
			(math.IsInf(datumFloat, 1) != math.IsInf(decodedFloat, 1)) &&
			(math.IsInf(datumFloat, -1) != math.IsInf(decodedFloat, -1)) &&
			datumFloat != decodedFloat {
			t.Errorf("schema: %s; Datum: %v; Actual: %f; Expected: %f", schema, datum, decodedFloat, datumFloat)
		}
	} else if datumIsString && decodedIsString {
		if actual, expected := decodedString, datumString; actual != expected {
			t.Errorf("schema: %s; Datum: %v; Actual: %s; Expected: %s", schema, datum, actual, expected)
		}
	} else if actual, expected := fmt.Sprintf("%v", decoded), fmt.Sprintf("%v", datum); actual != expected {
		t.Errorf("schema: %s; Datum: %v; Actual: %s; Expected: %s", schema, datum, actual, expected)
	}
}

func testTextEncodePass(t *testing.T, schema string, datum interface{}, expected []byte) {
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		t.Fatalf("Schma: %q %s", schema, err)
	}

	actual, err := codec.TextEncode(nil, datum)
	if err != nil {
		t.Fatalf("schema: %s; Datum: %v; %s", schema, datum, err)
	}
	if !bytes.Equal(actual, expected) {
		t.Errorf("schema: %s; Datum: %v; Actual: %+q; Expected: %+q", schema, datum, actual, expected)
	}
}

// testTextCodecPass does a bi-directional codec check, by encoding datum to
// bytes, then decoding bytes back to datum.
func testTextCodecPass(t *testing.T, schema string, datum interface{}, buf []byte) {
	testTextDecodePass(t, schema, datum, buf)
	testTextEncodePass(t, schema, datum, buf)
}
