package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"time"
)

type TaggedFields struct{}

type KafkaResponse interface {
	Encode() []byte
}

type KafkaRequestHeader struct {
	MessageSize   int32
	ApiKey        uint16
	ApiVersion    uint16
	CorrelationId uint32
}

func (header *KafkaRequestHeader) Print() {
	fmt.Println("Message Size: ", header.MessageSize)
	fmt.Println("Api Key: ", header.ApiKey)
	fmt.Println("Api Version: ", header.ApiVersion)
	fmt.Println("CorrelationId: ", header.CorrelationId)
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

func CreateMessage(correlationId uint32, message KafkaResponse) []byte {
	encoded := message.Encode()

	headerBuf := make([]byte, 4+4)

	messageSize := uint32(len(encoded) + 4)
	fmt.Println("size: ", messageSize)
	binary.BigEndian.PutUint32(headerBuf, messageSize)

	fmt.Println("Response id: ", correlationId)
	binary.BigEndian.PutUint32(headerBuf[4:], correlationId)

	return append(headerBuf, encoded...)
}

func ReadRequest(reader io.Reader) (KafkaRequestHeader, error) {
	buf := make([]byte, 512)
	var req = KafkaRequestHeader{}

	reader.Read(buf)
	req.MessageSize = int32(binary.BigEndian.Uint32(buf))
	req.ApiKey = binary.BigEndian.Uint16(buf[4:])
	req.ApiVersion = binary.BigEndian.Uint16(buf[6:])
	req.CorrelationId = binary.BigEndian.Uint32(buf[8:])

	return req, nil
}

func ReadRequestBytes(reader io.Reader) ([]byte, error) {
	req := make([]byte, 512)
	_, err := reader.Read(req)
	return req, err
}

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}

	defer func(listener net.Listener) {
		err := listener.Close()
		if err != nil {
			fmt.Println("Failed to close listener")
			os.Exit(1)
		}
	}(l)

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	defer func(conn net.Conn) {
		fmt.Println("Closing connection: ", conn.RemoteAddr())
		err := conn.Close()
		if err != nil {
			fmt.Println("Error closing connection: ", err.Error())
			os.Exit(1)
		}

	}(conn)

	run := true
	for run {
		fmt.Println("Reading request from conn: ", conn.RemoteAddr())
		req, err := ReadRequest(conn)
		if err != nil {
			fmt.Println("Error reading request: ", err.Error())
			os.Exit(1)
		}

		var errorCode int16 = 0
		if (req.ApiVersion < 0) || (req.ApiVersion > 4) {
			errorCode = 35
		}

		versionRespone := KafkaApiVersionsResponse{
			ErrorCode:        errorCode,
			NumApiKeys:       2,
			ApiKey:           18,
			ApiKeyMinVersion: 3,
			ApiKeyMaxVersion: 4,
			TaggedFields1:    nil,
			ThrottleTimeMs:   0,
			TaggedFields2:    nil,
		}
		//
		response := CreateMessage(req.CorrelationId, &versionRespone)

		_, err = conn.Write(response)
		if err != nil {
			fmt.Println("Error writing to connection: ", err.Error())
			run = false
		}
	}
}
