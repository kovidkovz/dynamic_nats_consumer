package main

import (
	"log"
	"os"

	"github.com/nats-io/nats.go"
)

func main() {
	// connect to the nats server
	_, js, _ := NatsConnector()

	kv, _ := js.KeyValue(os.Getenv("BUCKET"))

	// Start watching for new tokens and also read the older ones to start consuming...
	watcher, _ := kv.Watch("*")
	go func() {
		for update := range watcher.Updates() {
			if update == nil || update.Operation() != nats.KeyValuePut {
				continue
			}
			subject := "parseddata." + update.Key() + ".*"
			durable := "consumer_" + update.Key()
			stream := "parseddata_" + update.Key()

			go Consumer(stream, subject, durable, "", js, messageHandler)
		}
	}()

	select {} // Block forever
}

func messageHandler(msg *nats.Msg) {
	log.Printf("Received message on subject [%s]: %s", msg.Subject, string(msg.Data))
}
