package goavro_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/karrick/goavro"
)

func TestSchemaUnion(t *testing.T) {
	testSchemaInvalid(t, `[{"type":"enum","name":"e1","symbols":["alpha","bravo"]},"e1"]`, "Union item 2 ought to be unique type")
	testSchemaInvalid(t, `[{"type":"enum","name":"com.example.one","symbols":["red","green","blue"]},{"type":"enum","name":"one","namespace":"com.example","symbols":["dog","cat"]}]`, "Union item 2 ought to be unique type")
}

func TestUnion(t *testing.T) {
	testBinaryCodecPass(t, `["null"]`, goavro.Union("null", nil), []byte("\x00"))
	testBinaryCodecPass(t, `["null","int"]`, goavro.Union("null", nil), []byte("\x00"))
	testBinaryCodecPass(t, `["int","null"]`, goavro.Union("null", nil), []byte("\x02"))

	testBinaryCodecPass(t, `["null","int"]`, goavro.Union("int", 3), []byte("\x02\x06"))
	testBinaryCodecPass(t, `["null","long"]`, goavro.Union("long", 3), []byte("\x02\x06"))

	testBinaryCodecPass(t, `["int","null"]`, goavro.Union("int", 3), []byte("\x00\x06"))
	testBinaryEncodePass(t, `["int","null"]`, goavro.Union("int", 3), []byte("\x00\x06")) // can encode a bare 3

	testBinaryEncodeFail(t, `[{"type":"enum","name":"colors","symbols":["red","green","blue"]},{"type":"enum","name":"animals","symbols":["dog","cat"]}]`, goavro.Union("colors", "bravo"), "value ought to be member of symbols")
	testBinaryEncodeFail(t, `[{"type":"enum","name":"colors","symbols":["red","green","blue"]},{"type":"enum","name":"animals","symbols":["dog","cat"]}]`, goavro.Union("animals", "bravo"), "value ought to be member of symbols")
	testBinaryCodecPass(t, `[{"type":"enum","name":"colors","symbols":["red","green","blue"]},{"type":"enum","name":"animals","symbols":["dog","cat"]}]`, goavro.Union("colors", "green"), []byte{0, 2})
	testBinaryCodecPass(t, `[{"type":"enum","name":"colors","symbols":["red","green","blue"]},{"type":"enum","name":"animals","symbols":["dog","cat"]}]`, goavro.Union("animals", "cat"), []byte{2, 2})
}

func TestUnionRejectInvalidType(t *testing.T) {
	testBinaryEncodeFailBadDatumType(t, `["null","long"]`, 3)
	testBinaryEncodeFailBadDatumType(t, `["null","int","long","float"]`, float64(3.5))
	testBinaryEncodeFailBadDatumType(t, `["null","long"]`, goavro.Union("int", 3))
	testBinaryEncodeFailBadDatumType(t, `["null","int","long","float"]`, goavro.Union("double", float64(3.5)))
}

func TestUnionWillCoerceTypeIfPossible(t *testing.T) {
	testBinaryCodecPass(t, `["null","long","float","double"]`, goavro.Union("long", int32(3)), []byte("\x02\x06"))
	testBinaryCodecPass(t, `["null","int","float","double"]`, goavro.Union("int", int64(3)), []byte("\x02\x06"))
	testBinaryCodecPass(t, `["null","int","long","double"]`, goavro.Union("double", float32(3.5)), []byte("\x06\x00\x00\x00\x00\x00\x00\f@"))
	testBinaryCodecPass(t, `["null","int","long","float"]`, goavro.Union("float", float64(3.5)), []byte("\x06\x00\x00\x60\x40"))
}

func TestUnionNumericCoercionGuardsPrecision(t *testing.T) {
	testBinaryEncodeFail(t, `["null","int","long","double"]`, goavro.Union("int", float32(3.5)), "lose precision")
}

func TestUnionWithArray(t *testing.T) {
	testBinaryCodecPass(t, `["null",{"type":"array","items":"int"}]`, goavro.Union("null", nil), []byte("\x00"))

	testBinaryCodecPass(t, `["null",{"type":"array","items":"int"}]`, goavro.Union("array", []interface{}{}), []byte("\x02\x00"))
	testBinaryCodecPass(t, `["null",{"type":"array","items":"int"}]`, goavro.Union("array", []interface{}{1}), []byte("\x02\x02\x02\x00"))
	testBinaryCodecPass(t, `["null",{"type":"array","items":"int"}]`, goavro.Union("array", []interface{}{1, 2}), []byte("\x02\x04\x02\x04\x00"))

	testBinaryCodecPass(t, `[{"type": "array", "items": "string"}, "null"]`, goavro.Union("null", nil), []byte{2})
	testBinaryCodecPass(t, `[{"type": "array", "items": "string"}, "null"]`, goavro.Union("array", []string{"foo"}), []byte("\x00\x02\x06foo\x00"))
	testBinaryCodecPass(t, `[{"type": "array", "items": "string"}, "null"]`, goavro.Union("array", []string{"foo", "bar"}), []byte("\x00\x04\x06foo\x06bar\x00"))
}

func TestUnionWithMap(t *testing.T) {
	testBinaryCodecPass(t, `["null",{"type":"map","values":"string"}]`, goavro.Union("null", nil), []byte("\x00"))
	testBinaryCodecPass(t, `["string",{"type":"map","values":"string"}]`, goavro.Union("map", map[string]interface{}{"He": "Helium"}), []byte("\x02\x02\x04He\x0cHelium\x00"))
	testBinaryCodecPass(t, `["string",{"type":"array","items":"string"}]`, goavro.Union("string", "Helium"), []byte("\x00\x0cHelium"))
}

func TestUnionMapRecordFitsInRecord(t *testing.T) {
	// union value may be either map or a record
	codec, err := goavro.NewCodec(`["null",{"type":"map","values":"double"},{"type":"record","name":"com.example.record","fields":[{"name":"field1","type":"int"},{"name":"field2","type":"float"}]}]`)
	if err != nil {
		t.Fatal(err)
	}

	// the provided datum value could be encoded by either the map or the record schemas above
	datum := map[string]interface{}{
		"field1": 3,
		"field2": 3.5,
	}
	datumIn := goavro.Union("com.example.record", datum)

	buf, err := codec.BinaryEncode(nil, datumIn)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf, []byte{
		0x04,                   // prefer record (union item 2) over map (union item 1)
		0x06,                   // field1 == 3
		0x00, 0x00, 0x60, 0x40, // field2 == 3.5
	}) {
		t.Errorf("Actual: %#v; Expected: %#v", buf, []byte{byte(2)})
	}

	// round trip
	datumOut, buf, err := codec.BinaryDecode(buf)
	if err != nil {
		t.Fatal(err)
	}
	if actual, expected := len(buf), 0; actual != expected {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}

	datumOutMap, ok := datumOut.(map[string]interface{})
	if !ok {
		t.Fatalf("Actual: %#v; Expected: %#v", ok, false)
	}
	if actual, expected := len(datumOutMap), 1; actual != expected {
		t.Fatalf("Actual: %#v; Expected: %#v", actual, expected)
	}
	datumValue, ok := datumOutMap["com.example.record"]
	if !ok {
		t.Fatalf("Actual: %#v; Expected: %#v", datumOutMap, "have `com.example.record` key")
	}
	datumValueMap, ok := datumValue.(map[string]interface{})
	if !ok {
		t.Errorf("Actual: %#v; Expected: %#v", ok, true)
	}
	if actual, expected := len(datumValueMap), len(datum); actual != expected {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}
	for k, v := range datum {
		if actual, expected := fmt.Sprintf("%v", datumValueMap[k]), fmt.Sprintf("%v", v); actual != expected {
			t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
		}
	}
}

func TestUnionRecordFieldWhenNull(t *testing.T) {
	schema := `{
  "type": "record",
  "name": "r1",
  "fields": [
    {"name": "f1", "type": [{"type": "array", "items": "string"}, "null"]}
  ]
}`

	testBinaryCodecPass(t, schema, map[string]interface{}{"f1": goavro.Union("array", []string{"bar"})}, []byte("\x00\x02\x06bar\x00"))

	// decoded blob will include "f2" so decode test will be more involved
	testBinaryEncodePass(t, schema, map[string]interface{}{"f1": nil}, []byte("\x02"))
	testBinaryEncodePass(t, schema, map[string]interface{}{}, []byte("\x02"))
}

func TestUnionText(t *testing.T) {
	testTextEncodeFail(t, `["null","int"]`, goavro.Union("null", 3), "expected")
	testTextCodecPass(t, `["null","int"]`, goavro.Union("null", nil), []byte("null"))
	testTextCodecPass(t, `["null","int"]`, goavro.Union("int", 3), []byte(`{"int":3}`))
	testTextCodecPass(t, `["null","int","string"]`, goavro.Union("string", "😂 "), []byte(`{"string":"\u0001\uD83D\uDE02 "}`))
}
