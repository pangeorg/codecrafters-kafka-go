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

type KafakaApiKey struct {
	ApiKey     uint16
	MinVersion uint16
	MaxVersion uint16
	TagBuf     uint8
}

func (r *KafakaApiKey) Encode() []byte {
	buf := make([]byte, 2+2+2+1)
	binary.BigEndian.PutUint16(buf, r.ApiKey)
	binary.BigEndian.PutUint16(buf[2:], r.MinVersion)
	binary.BigEndian.PutUint16(buf[4:], r.MaxVersion)
	buf[6] = byte(0)
	return buf
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
	ErrorCode      int16
	NumApiKeys     int8
	ApiKeys        []KafakaApiKey
	ThrottleTimeMs int32
}

func (r *KafkaApiVersionsResponse) Encode() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, r.ErrorCode)
	binary.Write(buf, binary.BigEndian, r.NumApiKeys+1)
	for _, k := range r.ApiKeys {
		err := binary.Write(buf, binary.BigEndian, k)
		if err != nil {
			return []byte{}
		}
	}
	binary.Write(buf, binary.BigEndian, uint32(r.ThrottleTimeMs))
	binary.Write(buf, binary.BigEndian, int8(0)) // tag buf
	return buf.Bytes()
}

func MakeApiVersionsResponse(errorCode int16, throttleTime int32, apiKeys ...KafakaApiKey) KafkaApiVersionsResponse {
	return KafkaApiVersionsResponse{
		ErrorCode:      errorCode,
		ThrottleTimeMs: throttleTime,
		NumApiKeys:     int8(len(apiKeys)),
		ApiKeys:        apiKeys}
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
		go handleConnection(conn)
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

		versionRespone := MakeApiVersionsResponse(
			errorCode, 0,
			KafakaApiKey{ApiKey: 18, MinVersion: 4, MaxVersion: 4},  // version request
			KafakaApiKey{ApiKey: 1, MinVersion: 16, MaxVersion: 16}, // fetch Request
			KafakaApiKey{ApiKey: 75, MinVersion: 0, MaxVersion: 0},  // fetch Request
		)

		response := CreateMessage(req.CorrelationId, &versionRespone)

		_, err = conn.Write(response)
		if err != nil {
			fmt.Println("Error writing to connection: ", err.Error())
			run = false
		}
	}
}
