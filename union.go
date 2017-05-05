package goavro

import (
	"errors"
	"fmt"
)

// Union wraps a datum value in a map for encoding as a Union, as required by Union encoder.
func Union(name string, datum interface{}) interface{} {
	if datum == nil && name == "null" {
		return nil
	}
	return map[string]interface{}{name: datum}
}

func buildCodecForTypeDescribedBySlice(st map[string]*Codec, enclosingNamespace string, schemaArray []interface{}) (*Codec, error) {
	if len(schemaArray) == 0 {
		return nil, errors.New("Union ought to have one or more members")
	}

	allowedTypes := make([]string, len(schemaArray)) // used for error reporting when encoder receives invalid datum type
	codecFromIndex := make([]*Codec, len(schemaArray))
	indexFromName := make(map[string]int, len(schemaArray))

	for i, unionMemberSchema := range schemaArray {
		unionMemberCodec, err := buildCodec(st, enclosingNamespace, unionMemberSchema)
		if err != nil {
			return nil, fmt.Errorf("Union item %d ought to be valid Avro type: %s", i+1, err)
		}
		fullName := unionMemberCodec.typeName.fullName
		if _, ok := indexFromName[fullName]; ok {
			return nil, fmt.Errorf("Union item %d ought to be unique type: %s", i+1, unionMemberCodec.typeName)
		}
		indexFromName[fullName] = i
		allowedTypes[i] = fullName
		codecFromIndex[i] = unionMemberCodec
	}

	return &Codec{
		typeName: &name{"union", nullNamespace},
		binaryDecoder: func(buf []byte) (interface{}, []byte, error) {
			var decoded interface{}
			var err error

			decoded, buf, err = longBinaryDecoder(buf)
			if err != nil {
				return nil, buf, err
			}
			index := decoded.(int64) // longDecoder always returns int64, so elide error checking
			if index < 0 || index >= int64(len(codecFromIndex)) {
				return nil, buf, fmt.Errorf("cannot decode Union: index ought to be between 0 and %d; read index: %d", len(codecFromIndex)-1, index)
			}
			c := codecFromIndex[index]
			decoded, buf, err = c.binaryDecoder(buf)
			if err != nil {
				return nil, buf, fmt.Errorf("cannot decode Union item %d: %s", index+1, err)
			}
			if decoded == nil {
				return nil, buf, nil
			}
			return map[string]interface{}{allowedTypes[index]: decoded}, buf, nil
		},
		binaryEncoder: func(buf []byte, datum interface{}) ([]byte, error) {
			switch v := datum.(type) {
			case nil:
				index, ok := indexFromName["null"]
				if !ok {
					return buf, fmt.Errorf("cannot encode Union: no member schema types support datum: allowed types: %v; received: %T", allowedTypes, datum)
				}
				return longBinaryEncoder(buf, index)
			case map[string]interface{}:
				if len(v) != 1 {
					return buf, fmt.Errorf("cannot encode Union: non-nil values ought to be specified with Go map[string]interface{}, with single key equal to type name, and value equal to datum value: %v; received: %T", allowedTypes, datum)
				}
				// will execute exactly once
				for key, value := range v {
					index, ok := indexFromName[key]
					if !ok {
						return buf, fmt.Errorf("cannot encode Union: no member schema types support datum: allowed types: %v; received: %T", allowedTypes, datum)
					}
					c := codecFromIndex[index]
					buf, _ = longBinaryEncoder(buf, index)
					return c.binaryEncoder(buf, value)
				}
			}
			return buf, fmt.Errorf("cannot encode Union: non-nil values ought to be specified with Go map[string]interface{}, with single key equal to type name, and value equal to datum value: %v; received: %T", allowedTypes, datum)
		},
	}, nil
}
