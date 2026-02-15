package hfile

import "fmt"

// decodeVIntSize returns the total number of bytes used to encode a VInt,
// given the first byte. Follows Hadoop WritableUtils format.
func decodeVIntSize(firstByte int8) int {
	if firstByte >= -112 {
		return 1
	}
	if firstByte >= -120 {
		return int(-112-firstByte) + 1
	}
	return int(-120-firstByte) + 1
}

// isNegativeVInt returns whether the VInt encoded value is negative.
func isNegativeVInt(firstByte int8) bool {
	return firstByte < -120
}

// readVInt reads a Hadoop WritableUtils VInt from buf at offset.
// Returns the decoded value and the number of bytes consumed.
func readVInt(buf []byte, offset int) (int64, int, error) {
	if offset >= len(buf) {
		return 0, 0, fmt.Errorf("hfile: vint read past end of buffer")
	}
	firstByte := int8(buf[offset])
	size := decodeVIntSize(firstByte)
	if offset+size > len(buf) {
		return 0, 0, fmt.Errorf("hfile: vint extends past end of buffer")
	}
	if size == 1 {
		return int64(firstByte), 1, nil
	}
	var val int64
	for i := 1; i < size; i++ {
		val = (val << 8) | int64(buf[offset+i]&0xFF)
	}
	if isNegativeVInt(firstByte) {
		val = ^val
	}
	return val, size, nil
}

// readByteArray reads a VInt-prefixed byte array from buf at offset.
// Returns the byte array and the total number of bytes consumed.
func readByteArray(buf []byte, offset int) ([]byte, int, error) {
	length, n, err := readVInt(buf, offset)
	if err != nil {
		return nil, 0, err
	}
	if length < 0 {
		return nil, 0, fmt.Errorf("hfile: negative byte array length %d", length)
	}
	end := offset + n + int(length)
	if end > len(buf) {
		return nil, 0, fmt.Errorf("hfile: byte array extends past end of buffer")
	}
	return buf[offset+n : end], n + int(length), nil
}
