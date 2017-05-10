package goavro_test

import (
	"testing"

	"github.com/karrick/goavro"
)

func TestMapSchema(t *testing.T) {
	// NOTE: This schema also used to read and write files in OCF format
	testSchemaValid(t, `{"type":"map","values":"bytes"}`)

	testSchemaInvalid(t, `{"type":"map","value":"int"}`, "Map ought to have values key")
	testSchemaInvalid(t, `{"type":"map","values":"integer"}`, "Map values ought to be valid Avro type")
	testSchemaInvalid(t, `{"type":"map","values":3}`, "Map values ought to be valid Avro type")
	testSchemaInvalid(t, `{"type":"map","values":int}`, "invalid character") // type name must be quoted
}

func TestMapDecodeInitialBlockCountCannotDecode(t *testing.T) {
	testBinaryDecodeFail(t, `{"type":"map","values":"int"}`, nil, "block count")
}

func TestMapDecodeInitialBlockCountZero(t *testing.T) {
	testBinaryDecodePass(t, `{"type":"map","values":"int"}`, map[string]interface{}{}, []byte{0})
}

func TestMapDecodeInitialBlockCountNegative(t *testing.T) {
	testBinaryDecodePass(t, `{"type":"map","values":"int"}`, map[string]interface{}{"k1": 3}, []byte{1, 2, 4, 'k', '1', 6, 0})
}

func TestMapDecodeInitialBlockCountTooLarge(t *testing.T) {
	testBinaryDecodeFail(t, `{"type":"map","values":"int"}`, morePositiveThanMaxBlockCount, "block count")
}

func TestMapDecodeInitialBlockCountNegativeTooLarge(t *testing.T) {
	testBinaryDecodeFail(t, `{"type":"map","values":"int"}`, append(moreNegativeThanMaxBlockCount, byte(0)), "block count")
}

func TestMapDecodeNextBlockCountCannotDecode(t *testing.T) {
	testBinaryDecodeFail(t, `{"type":"map","values":"int"}`, []byte{1, 2, 4, 'k', '1', 6}, "block count")
}

func TestMapDecodeNextBlockCountNegative(t *testing.T) {
	c, err := goavro.NewCodec(`{"type":"map","values":"int"}`)
	if err != nil {
		t.Fatal(err)
	}

	decoded, _, err := c.BinaryDecode([]byte{1, 2, 4, 'k', '1', 6, 1, 8, 4, 'k', '2', 0x1a, 0})
	if err != nil {
		t.Fatal(err)
	}

	decodedMap, ok := decoded.(map[string]interface{})
	if !ok {
		t.Fatalf("Actual: %v; Expected: %v", ok, true)
	}

	value, ok := decodedMap["k1"]
	if !ok {
		t.Errorf("Actual: %v; Expected: %v", ok, true)
	}
	if actual, expected := value.(int32), int32(3); actual != expected {
		t.Errorf("Actual: %v; Expected: %v", actual, expected)
	}

	value, ok = decodedMap["k2"]
	if !ok {
		t.Errorf("Actual: %v; Expected: %v", ok, true)
	}
	if actual, expected := value.(int32), int32(13); actual != expected {
		t.Errorf("Actual: %v; Expected: %v", actual, expected)
	}
}

func TestMapDecodeNextBlockCountTooLarge(t *testing.T) {
	testBinaryDecodeFail(t, `{"type":"map","values":"int"}`, append([]byte{1, 2, 4, 'k', '1', 6}, morePositiveThanMaxBlockCount...), "block count")
}

func TestMapDecodeNextBlockCountNegativeTooLarge(t *testing.T) {
	testBinaryDecodeFail(t, `{"type":"map","values":"int"}`, append(append([]byte{1, 2, 4, 'k', '1', 6}, moreNegativeThanMaxBlockCount...), 2), "block count")
}

// TODO: key repeated in encoded map

func TestMapDecodeFail(t *testing.T) {
	testBinaryDecodeFail(t, `{"type":"map","values":"boolean"}`, nil, "cannot decode Map block count")           // leading block count
	testBinaryDecodeFail(t, `{"type":"map","values":"boolean"}`, []byte("\x01"), "cannot decode Map block size") // when block count < 0
	testBinaryDecodeFail(t, `{"type":"map","values":"boolean"}`, []byte("\x02\x04"), "cannot decode Map key")
	testBinaryDecodeFail(t, `{"type":"map","values":"boolean"}`, []byte("\x02\x04"), "cannot decode Map key")
	testBinaryDecodeFail(t, `{"type":"map","values":"boolean"}`, []byte("\x02\x04a"), "cannot decode Map key")
	testBinaryDecodeFail(t, `{"type":"map","values":"boolean"}`, []byte("\x02\x04ab"), "cannot decode Map value")
	testBinaryDecodeFail(t, `{"type":"map","values":"boolean"}`, []byte("\x02\x04ab\x02"), "boolean: expected")
	testBinaryDecodeFail(t, `{"type":"map","values":"boolean"}`, []byte("\x02\x04ab\x01"), "cannot decode Map block count") // trailing block count
}

func TestMap(t *testing.T) {
	testBinaryCodecPass(t, `{"type":"map","values":"null"}`, map[string]interface{}{"ab": nil}, []byte("\x02\x04ab\x00"))
	testBinaryCodecPass(t, `{"type":"map","values":"boolean"}`, map[string]interface{}{"ab": true}, []byte("\x02\x04ab\x01\x00"))
}

func TestMapTextDecodeFail(t *testing.T) {
	schema := `{"type":"map","values":"string"}`
	testTextDecodeFail(t, schema, []byte(`    "string"  :  "silly"  ,   "bytes"  : "silly" } `), "expected: '{'")
	testTextDecodeFail(t, schema, []byte(`  {  16  :  "silly"  ,   "bytes"  : "silly" } `), "expected initial \"")
	testTextDecodeFail(t, schema, []byte(`  {  "badName"  :  "silly"  ,   "bytes"  : "silly" } `), "invalid record field name")
	testTextDecodeFail(t, schema, []byte(`  {  "string"  ,  "silly"  ,   "bytes"  : "silly" } `), "expected: ':'")
	testTextDecodeFail(t, schema, []byte(`  {  "string"  :  13  ,   "bytes"  : "silly" } `), "expected initial \"")
	testTextDecodeFail(t, schema, []byte(`  {  "string"  :  "silly" :   "bytes"  : "silly" } `), "expected ',' or '}'")
	testTextDecodeFail(t, schema, []byte(`  {  "string"  :  "silly" ,   "bytes"  : "silly"  `), "short buffer")
	testTextDecodeFail(t, schema, []byte(`  {  "string"  :  "silly"  `), "short buffer")
	testTextDecodeFail(t, schema, []byte(`  {  "string"  :  "silly" } `), "only found 1 of 2 fields")
}

func TestMapTextCodecPass(t *testing.T) {
	datum := map[string]interface{}{"key1": "⌘ ", "key2": "value2"}
	testTextEncodePass(t, `{"type":"map","values":"string"}`, datum, []byte(`{"key1":"\u0001\u2318 ","key2":"value2"}`))
	testTextDecodePass(t, `{"type":"map","values":"string"}`, datum, []byte(` { "key1" : "\u0001\u2318 " , "key2" : "value2" }`))
}
