package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
)

type KafkaResponse struct {
	MessageSize   int32
	CorrelationId int32
	ErrorCode     int16
}

func (r *KafkaResponse) Encode() []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, r)
	return buf.Bytes()
}

type TaggedFields struct{}

type KafkaRequest struct {
	MessageSize   int32
	ApiKey        int16
	ApiVersion    int16
	CorrelationId int32
	ClientId      *string
	TaggedFields  *TaggedFields
}

func (r *KafkaRequest) Encode() []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, r)
	return buf.Bytes()
}

func ReadRequest(reader io.Reader) (KafkaRequest, error) {
	var req = KafkaRequest{}
	var l int
	var r int

	buf := make([]byte, 1024)
	_, err := reader.Read(buf)
	if err != nil {
		return req, err
	}

	l, r = 0, binary.Size(req.MessageSize)
	req.MessageSize = int32(binary.BigEndian.Uint32(buf[l:r]))
	l, r = r, r+binary.Size(req.ApiKey)
	req.ApiKey = int16(binary.BigEndian.Uint16(buf[l:r]))
	l, r = r, r+binary.Size(req.ApiVersion)
	req.ApiVersion = int16(binary.BigEndian.Uint16(buf[l:r]))
	l, r = r, r+binary.Size(req.CorrelationId)
	req.CorrelationId = int32(binary.BigEndian.Uint32(buf[l:r]))
	fmt.Println(req)
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

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		// rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
		req, err := ReadRequest(conn)
		if err != nil {
			fmt.Println("Error reading connection: ", err.Error())
			os.Exit(1)
		}

		var errorCode int16 = 0
		fmt.Printf("Version %d", req.ApiVersion)
		if (req.ApiVersion < 0) || (req.ApiVersion > 4) {
			errorCode = 35
		}

		response := KafkaResponse{
			MessageSize:   0,
			CorrelationId: req.CorrelationId,
			ErrorCode:     errorCode,
		}

		encoded := response.Encode()
		_, err = conn.Write(encoded)
		if err != nil {
			fmt.Println("Error writing to connection: ", err.Error())
			os.Exit(1)
		}
		conn.Close()
	}
}
