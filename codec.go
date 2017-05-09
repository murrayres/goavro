package goavro

import (
	"encoding/json"
	"fmt"
)

// BinaryDecoder interface describes types that expose the BinaryDecode method.
type BinaryDecoder interface {
	BinaryDecode([]byte) (interface{}, []byte, error)
}

// BinaryEncoder interface describes types that expose the BinaryEncode method.
type BinaryEncoder interface {
	BinaryEncode([]byte, interface{}) ([]byte, error)
}

// BinaryCoder interface describes types that expose both the BinaryDecode and
// the BinaryEncode methods.
type BinaryCoder interface {
	BinaryDecoder
	BinaryEncoder
}

// TextDecoder interface describes types that expose the TextDecode method.
type TextDecoder interface {
	TextDecode([]byte) (interface{}, []byte, error)
}

// TextEncoder interface describes types that expose the TextEncode method.
type TextEncoder interface {
	TextEncode([]byte, interface{}) ([]byte, error)
}

// TextCoder interface describes types that expose both the TextDecode and the
// TextEncode methods.
type TextCoder interface {
	TextDecoder
	TextEncoder
}

// Codec stores function pointers for encoding and decoding Avro blobs according
// to their defined specification.  Their state is created during
// initialization, but then never modified, so the same Codec may be safely used
// in multiple go routines to encode and or decode different Avro streams
// concurrently.
type Codec struct {
	typeName    *name
	symbolTable map[string]*Codec

	decoder func([]byte) (interface{}, []byte, error)
	encoder func([]byte, interface{}) ([]byte, error)

	textDecoder func([]byte) (interface{}, []byte, error)
	textEncoder func([]byte, interface{}) ([]byte, error)
}

func newSymbolTable() map[string]*Codec {
	return map[string]*Codec{
		"boolean": &Codec{
			typeName:    &name{"boolean", nullNamespace},
			decoder:     booleanDecoder,
			encoder:     booleanEncoder,
			textDecoder: booleanTextDecoder,
			textEncoder: booleanTextEncoder,
		},
		"bytes": &Codec{typeName: &name{"bytes", nullNamespace},
			decoder:     bytesDecoder,
			encoder:     bytesEncoder,
			textDecoder: bytesTextDecoder,
			textEncoder: bytesTextEncoder,
		},
		"double": &Codec{typeName: &name{"double", nullNamespace},
			decoder:     doubleDecoder,
			encoder:     doubleEncoder,
			textDecoder: doubleTextDecoder,
			textEncoder: doubleTextEncoder,
		},
		"float": &Codec{typeName: &name{"float", nullNamespace},
			decoder:     floatDecoder,
			encoder:     floatEncoder,
			textDecoder: floatTextDecoder,
			textEncoder: floatTextEncoder,
		},
		"int": &Codec{typeName: &name{"int", nullNamespace},
			decoder:     intDecoder,
			encoder:     intEncoder,
			textDecoder: intTextDecoder,
			textEncoder: intTextEncoder,
		},
		"long": &Codec{typeName: &name{"long", nullNamespace},
			decoder:     longDecoder,
			encoder:     longEncoder,
			textDecoder: longTextDecoder,
			textEncoder: longTextEncoder,
		},
		"null": &Codec{typeName: &name{"null", nullNamespace},
			decoder:     nullDecoder,
			encoder:     nullEncoder,
			textDecoder: nullTextDecoder,
			textEncoder: nullTextEncoder,
		},
		"string": &Codec{typeName: &name{"string", nullNamespace},
			decoder:     stringDecoder,
			encoder:     stringEncoder,
			textDecoder: stringTextDecoder,
			textEncoder: stringTextEncoder,
		},
	}
}

// NewCodec returns a Codec that can encode and decode the specified Avro
// schema.
func NewCodec(schemaSpecification string) (*Codec, error) {
	// bootstrap a symbol table with primitive type codecs for the new codec
	st := newSymbolTable()

	// NOTE: Some clients might give us unadorned primitive type name for the
	// schema, e.g., "long".  While it is not valid JSON, it is a valid schema.
	// Provide special handling for primitive type names.
	if c, ok := st[schemaSpecification]; ok {
		c.symbolTable = st
		return c, nil
	}

	// NOTE: At this point, schema ought to be valid JSON
	var schema interface{}
	if err := json.Unmarshal([]byte(schemaSpecification), &schema); err != nil {
		return nil, fmt.Errorf("cannot unmarshal JSON: %s", err)
	}

	c, err := buildCodec(st, nullNamespace, schema)
	if err == nil {
		c.symbolTable = st
	}
	return c, err
}

// BinaryDecode decodes the provided byte slice in accordance with the Codec's
// Avro schema.  On success, it returns the decoded value, along with a new byte
// slice with the decoded bytes consumed.  In other words, when decoding an Avro
// int that happens to take 3 bytes, the returned byte slice will be like the
// original byte slice, but with the first three bytes removed.  On error, it
// returns the original byte slice without any bytes consumed and the error.
func (c Codec) BinaryDecode(buf []byte) (interface{}, []byte, error) {
	value, newBuf, err := c.decoder(buf)
	if err != nil {
		return nil, buf, err // if error, return original byte slice
	}
	return value, newBuf, nil
}

// BinaryEncode encodes the provided datum value in accordance with the Codec's
// Avro schema.  It takes a byte slice to which to append the encoded bytes.  On
// success, it returns the new byte slice with the appended byte slice.  On
// error, it returns the original byte slice without any encoded bytes.
func (c Codec) BinaryEncode(buf []byte, datum interface{}) ([]byte, error) {
	newBuf, err := c.encoder(buf, datum)
	if err != nil {
		return buf, err // if error, return original byte slice
	}
	return newBuf, nil
}

// TextDecode decodes the provided byte slice in accordance with the Codec's
// Avro schema.  On success, it returns the decoded value, along with a new byte
// slice with the decoded bytes consumed.  In other words, when decoding an Avro
// int that happens to take 3 bytes, the returned byte slice will be like the
// original byte slice, but with the first three bytes removed.  On error, it
// returns the original byte slice without any bytes consumed and the error.
func (c Codec) TextDecode(buf []byte) (interface{}, []byte, error) {
	value, newBuf, err := c.textDecoder(buf)
	if err != nil {
		return nil, buf, err // if error, return original byte slice
	}
	return value, newBuf, nil
}

// TextEncode encodes the provided datum value in accordance with the Codec's
// Avro schema.  It takes a byte slice to which to append the encoded bytes.  On
// success, it returns the new byte slice with the appended byte slice.  On
// error, it returns the original byte slice without any encoded bytes.
func (c Codec) TextEncode(buf []byte, datum interface{}) ([]byte, error) {
	newBuf, err := c.textEncoder(buf, datum)
	if err != nil {
		return buf, err // if error, return original byte slice
	}
	return newBuf, nil
}

// convert a schema data structure to a codec, prefixing with specified
// namespace
func buildCodec(st map[string]*Codec, enclosingNamespace string, schema interface{}) (*Codec, error) {
	switch schemaType := schema.(type) {
	case map[string]interface{}:
		return buildCodecForTypeDescribedByMap(st, enclosingNamespace, schemaType)
	case string:
		return buildCodecForTypeDescribedByString(st, enclosingNamespace, schemaType, nil)
	case []interface{}:
		return buildCodecForTypeDescribedBySlice(st, enclosingNamespace, schemaType)
	default:
		return nil, fmt.Errorf("unknown schema type: %T", schema)
	}
}

// Reach into the map, grabbing its "type".  Use that to create the codec.
func buildCodecForTypeDescribedByMap(st map[string]*Codec, enclosingNamespace string, schemaMap map[string]interface{}) (*Codec, error) {
	t, ok := schemaMap["type"]
	if !ok {
		return nil, fmt.Errorf("missing type: %v", schemaMap)
	}
	switch v := t.(type) {
	case string:
		// Already defined types may be abbreviated with its string name.
		// EXAMPLE: "type":"array"
		// EXAMPLE: "type":"enum"
		// EXAMPLE: "type":"fixed"
		// EXAMPLE: "type":"int"
		// EXAMPLE: "type":"record"
		// EXAMPLE: "type":"somePreviouslyDefinedCustomTypeString"
		return buildCodecForTypeDescribedByString(st, enclosingNamespace, v, schemaMap)
	case map[string]interface{}:
		return buildCodecForTypeDescribedByMap(st, enclosingNamespace, v)
	case []interface{}:
		return buildCodecForTypeDescribedBySlice(st, enclosingNamespace, v)
	default:
		return nil, fmt.Errorf("type ought to be either string, map[string]interface{}, or []interface{}; received: %T", t)
	}
}

func buildCodecForTypeDescribedByString(st map[string]*Codec, enclosingNamespace string, typeName string, schemaMap map[string]interface{}) (*Codec, error) {
	// NOTE: When codec already exists, return it.  This includes both primitive
	// type codecs added in NewCodec, and user-defined types, added while
	// building the codec.
	if cd, ok := st[typeName]; ok {
		return cd, nil
	}
	// NOTE: Sometimes schema may abbreviate type name inside a namespace.
	if enclosingNamespace != "" {
		if cd, ok := st[enclosingNamespace+"."+typeName]; ok {
			return cd, nil
		}
	}
	// There are only a small handful of complex Avro data types.
	switch typeName {
	case "array":
		return makeArrayCodec(st, enclosingNamespace, schemaMap)
	case "enum":
		return makeEnumCodec(st, enclosingNamespace, schemaMap)
	case "fixed":
		return makeFixedCodec(st, enclosingNamespace, schemaMap)
	case "map":
		return makeMapCodec(st, enclosingNamespace, schemaMap)
	case "record":
		return makeRecordCodec(st, enclosingNamespace, schemaMap)
	default:
		return nil, fmt.Errorf("unknown type name: %q", typeName)
	}
}

// notion of enclosing namespace changes when record, enum, or fixed create a
// new namespace, for child objects.
func registerNewCodec(st map[string]*Codec, schemaMap map[string]interface{}, enclosingNamespace string) (*Codec, error) {
	n, err := newNameFromSchemaMap(enclosingNamespace, schemaMap)
	if err != nil {
		return nil, err
	}
	c := &Codec{typeName: n}
	st[n.fullName] = c
	return c, nil
}

func typeNames(st map[string]*Codec) []string {
	var keys []string
	for k := range st {
		keys = append(keys, k)
	}
	return keys
}
