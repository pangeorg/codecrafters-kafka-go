package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
)

type TaggedFields struct{}

type KafkaResponse interface {
	Encode() []byte
}

type KafkaRequestHeader struct {
	MessageSize   int32
	ApiKey        int16
	ApiVersion    int16
	CorrelationId int32
	ClientId      *string
	TaggedFields  *TaggedFields
}

func (r *KafkaRequestHeader) Encode() []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, r)
	return buf.Bytes()
}

type KafkaErrorResponse struct {
	ErrorCode int16
}

func (r *KafkaErrorResponse) Encode() []byte {
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, uint16(r.ErrorCode))
	return buf
}

type KafkaApiVersionsResponse struct {
	ErrorCode        int16
	NumApiKeys       int8
	ApiKey           int16
	ApiKeyMinVersion int16
	ApiKeyMaxVersion int16
	TaggedFields1    *TaggedFields
	ThrottleTimeMs   int32
	TaggedFields2    *TaggedFields
}

func (r *KafkaApiVersionsResponse) Encode() []byte {
	buf := make([]byte, 2+1+2+2+2+1+4+1)
	binary.BigEndian.PutUint16(buf, uint16(r.ErrorCode))
	buf[2] = byte(r.NumApiKeys)
	binary.BigEndian.PutUint16(buf[3:], uint16(r.ApiKey))
	binary.BigEndian.PutUint16(buf[5:], uint16(r.ApiKeyMinVersion))
	binary.BigEndian.PutUint16(buf[7:], uint16(r.ApiKeyMaxVersion))
	buf[9] = byte(0) // nil for tagged fields
	binary.BigEndian.PutUint32(buf[10:], uint32(r.ThrottleTimeMs))
	buf[14] = byte(0) // nil for tagged fields
	return buf
}

func CreateMessage(correlationId int32, message KafkaResponse) []byte {
	encoded := message.Encode()

	headerBuf := make([]byte, 4+4)
	binary.BigEndian.PutUint32(headerBuf, uint32(len(encoded)+4))
	binary.BigEndian.PutUint32(headerBuf[4:], uint32(correlationId))

	return append(headerBuf, encoded...)
}

func ReadRequest(reader io.Reader) (KafkaRequestHeader, error) {
	var req = KafkaRequestHeader{}

	err := binary.Read(reader, binary.BigEndian, &req.MessageSize)
	if err != nil {
		return req, err
	}
	err = binary.Read(reader, binary.BigEndian, &req.ApiKey)
	if err != nil {
		return req, err
	}
	err = binary.Read(reader, binary.BigEndian, &req.ApiVersion)
	if err != nil {
		return req, err
	}
	err = binary.Read(reader, binary.BigEndian, &req.CorrelationId)
	if err != nil {
		return req, err
	}

	return req, nil
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage
	//
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}

	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}

	req, err := ReadRequest(conn)
	if err != nil {
		fmt.Println("Error reading connection: ", err.Error())
		os.Exit(1)
	}

	if (req.ApiVersion < 0) || (req.ApiVersion > 4) {
		errorResponse := KafkaErrorResponse{ErrorCode: 35}
		response := CreateMessage(req.CorrelationId, &errorResponse)
		fmt.Println("Unsupported api version: ", req.ApiVersion)
		_, err = conn.Write(response)
		conn.Close()
		os.Exit(1)
	}

	versionRespone := KafkaApiVersionsResponse{
		ErrorCode:        0,
		NumApiKeys:       2,
		ApiKey:           18,
		ApiKeyMinVersion: 3,
		ApiKeyMaxVersion: 4,
		TaggedFields1:    nil,
		ThrottleTimeMs:   0,
		TaggedFields2:    nil,
	}

	response := CreateMessage(req.CorrelationId, &versionRespone)

	_, err = conn.Write(response)
	if err != nil {
		fmt.Println("Error writing to connection: ", err.Error())
		os.Exit(1)
	}

	conn.Close()
}
