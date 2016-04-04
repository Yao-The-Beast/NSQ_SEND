package benchmark

import (
	"encoding/binary"
	"log"
	"time"
)

type MessageSender interface {
	Send([]byte)
}

type SendEndpoint struct {
	MessageSender MessageSender
}

func (endpoint SendEndpoint) TestThroughput(messageSize int, numberToSend int) {
	message := make([]byte, messageSize)
	start := time.Now().UnixNano()
	for i := 0; i < numberToSend; i++ {
		endpoint.MessageSender.Send(message)
	}

	stop := time.Now().UnixNano()
	ms := float32(stop-start) / 1000000
	log.Printf("Sent %d messages in %f ms\n", numberToSend, ms)
	log.Printf("Sent %f per second\n", 1000*float32(numberToSend)/ms)
}

func (endpoint SendEndpoint) TestLatency(messageSize int, numberToSend int) {
	start := time.Now().UnixNano()
	b := make([]byte, messageSize)
	for i := 0; i < numberToSend*100; i++ {
		binary.PutVarint(b, time.Now().UnixNano())
		//j := int64(i)
		//binary.PutVarint(b, j)
		endpoint.MessageSender.Send(b)
		//time.Sleep(10 * time.Microsecond)
	}
	binary.PutVarint(b, int64(0));
	endpoint.MessageSender.Send(b);

	stop := time.Now().UnixNano()
	ms := float32(stop-start) / 1000000
	log.Printf("Sent %d messages in %f ms\n", numberToSend, ms)
	log.Printf("Sent %f per second\n", 1000*float32(numberToSend)/ms)
}
