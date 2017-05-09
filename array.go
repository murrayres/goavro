package goavro

import (
	"fmt"
	"reflect"
)

func makeArrayCodec(st map[string]*Codec, enclosingNamespace string, schemaMap map[string]interface{}) (*Codec, error) {
	// array type must have items
	itemSchema, ok := schemaMap["items"]
	if !ok {
		return nil, fmt.Errorf("Array ought to have items key")
	}
	itemCodec, err := buildCodec(st, enclosingNamespace, itemSchema)
	if err != nil {
		return nil, fmt.Errorf("Array items ought to be valid Avro type: %s", err)
	}

	return &Codec{
		typeName: &name{"array", nullNamespace},
		decoder: func(buf []byte) (interface{}, []byte, error) {
			var value interface{}
			var err error

			// block count and block size
			if value, buf, err = longDecoder(buf); err != nil {
				return nil, buf, fmt.Errorf("cannot decode Array block count: %s", err)
			}
			blockCount := value.(int64)
			if blockCount < 0 {
				// NOTE: A negative block count implies there is a long encoded
				// block size following the negative block count. We have no use
				// for the block size in this decoder, so we read and discard
				// the value.
				blockCount = -blockCount // convert to its positive equivalent
				if _, buf, err = longDecoder(buf); err != nil {
					return nil, buf, fmt.Errorf("cannot decode Array block size: %s", err)
				}
			}
			// Ensure block count does not exceed some sane value.
			if blockCount > MaxBlockCount {
				return nil, buf, fmt.Errorf("cannot decode Array when block count exceeds MaxBlockCount: %d > %d", blockCount, MaxBlockCount)
			}
			// NOTE: While the attempt of a RAM optimization shown below is not
			// necessary, many encoders will encode all items in a single block.
			// We can optimize amount of RAM allocated by runtime for the array
			// by initializing the array for that number of items.
			arrayValues := make([]interface{}, 0, blockCount)

			for blockCount != 0 {
				// Decode `blockCount` datum values from buffer
				for i := int64(0); i < blockCount; i++ {
					if value, buf, err = itemCodec.decoder(buf); err != nil {
						return nil, buf, fmt.Errorf("cannot decode Array item %d: %s", i+1, err)
					}
					arrayValues = append(arrayValues, value)
				}
				// Decode next blockCount from buffer, because there may be more blocks
				if value, buf, err = longDecoder(buf); err != nil {
					return nil, buf, fmt.Errorf("cannot decode Array block count: %s", err)
				}
				blockCount = value.(int64)
				if blockCount < 0 {
					// NOTE: A negative block count implies there is a long
					// encoded block size following the negative block count. We
					// have no use for the block size in this decoder, so we
					// read and discard the value.
					blockCount = -blockCount // convert to its positive equivalent
					if _, buf, err = longDecoder(buf); err != nil {
						return nil, buf, fmt.Errorf("cannot decode Array block size: %s", err)
					}
				}
				// Ensure block count does not exceed some sane value.
				if blockCount > MaxBlockCount {
					return nil, buf, fmt.Errorf("cannot decode Array when block count exceeds MaxBlockCount: %d > %d", blockCount, MaxBlockCount)
				}
			}
			return arrayValues, buf, nil
		},
		encoder: func(buf []byte, datum interface{}) ([]byte, error) {
			var arrayValues []interface{}
			switch i := datum.(type) {
			case []interface{}:
				arrayValues = i
			default:
				// NOTE: When given a slice of any other type, zip values to
				// items as a convenience to client.
				v := reflect.ValueOf(datum)
				if v.Kind() != reflect.Slice {
					return buf, fmt.Errorf("Array: expected []interface{}; received: %T", datum)
				}
				// NOTE: Two better alternatives to the current algorithm are:
				//   (1) mutate the reflection tuple underneath to convert the
				//       []int, for example, to []interface{}, with O(1) complexity
				//   (2) use copy builtin to zip the data items over, much like
				//       what gorrd does, with O(n) complexity, but more
				//       efficient than what's below.
				arrayValues = make([]interface{}, v.Len())
				for idx := 0; idx < v.Len(); idx++ {
					arrayValues[idx] = v.Index(idx).Interface()
				}
			}

			arrayLength := int64(len(arrayValues))
			var alreadyEncoded, remainingInBlock int64

			for i, item := range arrayValues {
				if remainingInBlock == 0 { // start a new block
					remainingInBlock = arrayLength - alreadyEncoded
					if remainingInBlock > MaxBlockCount {
						// limit block count to MacBlockCount
						remainingInBlock = MaxBlockCount
					}
					buf, _ = longEncoder(buf, remainingInBlock)
				}

				if buf, err = itemCodec.encoder(buf, item); err != nil {
					return buf, fmt.Errorf("cannot encode Array item %d; %v: %s", i+1, item, err)
				}

				remainingInBlock--
				alreadyEncoded++
			}

			return longEncoder(buf, 0) // append trailing 0 block count to signal end of Array
		},
	}, nil
}
