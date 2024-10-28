package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

type KafkaResponse struct {
	MessageSize   int32
	CorrelationId int32
}

func (r *KafkaResponse) Encode() []byte {

	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, r)
	return buf.Bytes()
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
		response := KafkaResponse{
			MessageSize:   0,
			CorrelationId: 7,
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
