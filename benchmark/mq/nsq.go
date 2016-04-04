package mq

import (
//	"strconv"
	"github.com/bitly/go-nsq"
	"github.com/tylertreat/NSQ_SEND/benchmark"
)

type Nsq struct {
	handler benchmark.MessageHandler
	pub     *nsq.Producer
	sub     *nsq.Consumer
	topic   string
	channel string
}

func NewNsq(numberOfMessages int, testLatency bool, channeL string) *Nsq {
	//topic := "0#ephemeral"
	channel := channeL
//	i, _ := strconv.Atoi(channel)
	channel += "#ephemeral"
	topic := channel

	config := nsq.NewConfig()
	config.MaxInFlight = 2000
    
    pub, _ := nsq.NewProducer("localhost:4150", config)
//	if i >= 128 {
//		pub, _ = nsq.NewProducer("192.168.1.11:4150", config)
//	}
	sub, _ := nsq.NewConsumer("111100", "111100", nsq.NewConfig())

	var handler benchmark.MessageHandler
	if testLatency {
		handler = &benchmark.LatencyMessageHandler{
			NumberOfMessages: numberOfMessages,
			Latencies:        []float32{},
		}
	} else {
		handler = &benchmark.ThroughputMessageHandler{NumberOfMessages: numberOfMessages}
	}

	return &Nsq{
		handler: handler,
		pub:     pub,
		sub:     sub,
		topic:   topic,
		channel: channel,
	}
}

func (n *Nsq) Setup() {
}

func (n *Nsq) Teardown() {
	n.sub.Stop()
	n.pub.Stop()
}

func (n *Nsq) Send(message []byte) {
	n.pub.PublishAsync(n.topic, message, nil)
}

func (n *Nsq) MessageHandler() *benchmark.MessageHandler {
	return &n.handler
}
