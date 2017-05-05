package goavro_test

import (
	"math"
	"testing"
)

func TestSchemaPrimitiveCodecDouble(t *testing.T) {
	testSchemaPrimativeCodec(t, "double")
}

func TestPrimitiveDoubleBinary(t *testing.T) {
	testBinaryEncodeFailBadDatumType(t, "double", "some string")
	testBinaryDecodeFailShortBuffer(t, "double", []byte("\x00\x00\x00\x00\x00\x00\xf0"))
	testBinaryCodecPass(t, "double", 3.5, []byte("\x00\x00\x00\x00\x00\x00\f@"))
	testBinaryCodecPass(t, "double", math.Inf(-1), []byte("\x00\x00\x00\x00\x00\x00\xf0\xff"))
	testBinaryCodecPass(t, "double", math.Inf(1), []byte("\x00\x00\x00\x00\x00\x00\xf0\u007f"))
	testBinaryCodecPass(t, "double", math.NaN(), []byte("\x01\x00\x00\x00\x00\x00\xf8\u007f"))
}

func TestPrimitiveDoubleText(t *testing.T) {
	testTextDecodeFailShortBuffer(t, "double", []byte(""))
	testTextDecodeFailShortBuffer(t, "double", []byte("-"))

	testTextCodecPass(t, "double", -12.3, []byte("-12.3"))
	testTextCodecPass(t, "double", -0.5, []byte("-0.5"))
	testTextCodecPass(t, "double", -3.5, []byte("-3.5"))
	testTextCodecPass(t, "double", 0, []byte("0"))
	testTextCodecPass(t, "double", 0.5, []byte("0.5"))
	testTextCodecPass(t, "double", 1, []byte("1"))
	testTextCodecPass(t, "double", 19.7, []byte("19.7"))
	testTextCodecPass(t, "double", math.Inf(-1), []byte("-1e999"))
	testTextCodecPass(t, "double", math.Inf(1), []byte("1e999"))
	testTextCodecPass(t, "double", math.NaN(), []byte("null"))
	testTextDecodeFail(t, "double", []byte("42a"), "unexpected byte")
	testTextDecodePass(t, "double", -0, []byte("-0"))
	testTextEncodePass(t, "double", -0, []byte("0")) // NOTE: -0 encodes as "0"
}

func TestSchemaPrimitiveCodecFloat(t *testing.T) {
	testSchemaPrimativeCodec(t, "float")
}

func TestPrimitiveFloatBinary(t *testing.T) {
	testBinaryEncodeFailBadDatumType(t, "float", "some string")
	testBinaryDecodeFailShortBuffer(t, "float", []byte("\x00\x00\x80"))
	testBinaryCodecPass(t, "float", 3.5, []byte("\x00\x00\x60\x40"))
	testBinaryCodecPass(t, "float", math.Inf(-1), []byte("\x00\x00\x80\xff"))
	testBinaryCodecPass(t, "float", math.Inf(1), []byte("\x00\x00\x80\u007f"))
	testBinaryCodecPass(t, "float", math.NaN(), []byte("\x00\x00\xc0\u007f"))
}

func TestPrimitiveFloatText(t *testing.T) {
	testTextDecodeFailShortBuffer(t, "float", []byte(""))
	testTextDecodeFailShortBuffer(t, "float", []byte("-"))

	testTextCodecPass(t, "float", -12.3, []byte("-12.3"))
	testTextCodecPass(t, "float", -0.5, []byte("-0.5"))
	testTextCodecPass(t, "float", -3.5, []byte("-3.5"))
	testTextCodecPass(t, "float", 0, []byte("0"))
	testTextCodecPass(t, "float", 0.5, []byte("0.5"))
	testTextCodecPass(t, "float", 1, []byte("1"))
	testTextCodecPass(t, "float", 19.7, []byte("19.7"))
	testTextCodecPass(t, "float", math.Inf(-1), []byte("-1e999"))
	testTextCodecPass(t, "float", math.Inf(1), []byte("1e999"))
	testTextCodecPass(t, "float", math.NaN(), []byte("null"))
	testTextDecodeFail(t, "float", []byte("42a"), "unexpected byte")
	testTextDecodePass(t, "float", -0, []byte("-0"))
	testTextEncodePass(t, "float", -0, []byte("0")) // NOTE: -0 encodes as "0"
}
