package stratum

//Some functions and types commonly used by stratum implementations are grouped here

import (
	"encoding/hex"
	"errors"
	"fmt"
)

// HexStringToBytes converts a hex encoded string (but as go type interface{}) to a byteslice
// If v is no valid string or the string contains invalid characters, an error is returned
func HexStringToBytes(v interface{}) (result []byte, err error) {
	var ok bool
	var stringValue string
	if stringValue, ok = v.(string); !ok {
		return nil, errors.New("Not a valid string")
	}
	if result, err = hex.DecodeString(stringValue); err != nil {
		return nil, errors.New("Not a valid hexadecimal value")
	}
	return
}

// ExtraNonce2 is the nonce modified by the miner
type ExtraNonce2 struct {
	Value uint64
	Size  uint
}

// Bytes is a bigendian representation of the extranonce2
func (en *ExtraNonce2) Bytes() (b []byte) {
	b = make([]byte, en.Size, en.Size)
	for i := uint(0); i < en.Size; i++ {
		b[(en.Size-1)-i] = byte(en.Value >> (i * 8))
	}
	return
}

// Increment increases the nonce with 1, an error is returned if the resulting is value is bigger than possible given the size
func (en *ExtraNonce2) Increment() (err error) {
	if en.Size == 0 {
		return nil
	}
	if en.Size > 8 {
		return fmt.Errorf("extranonce2 size %d exceeds 8 bytes", en.Size)
	}
	if en.Size < 8 && en.Value == (uint64(1)<<(en.Size*8))-1 {
		return errors.New("extranonce2 exhausted")
	}
	if en.Size == 8 && en.Value == ^uint64(0) {
		return errors.New("extranonce2 exhausted")
	}
	en.Value++
	return
}
