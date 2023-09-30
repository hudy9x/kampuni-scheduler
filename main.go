package main

import (
	"fmt"
	"os"

	// report "kampuni/scheduler/internal/cronjob"
	// "kampuni/scheduler/pkg/db"
	"bufio"
	"io"
	"kampuni/scheduler/pkg/messenger"

	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		fmt.Println("Error loading .env file")
	}
	//
	// if err := db.Run(); err != nil {
	// 	panic(err)
	// }
	//
	// report.ReportDaily()

  broker := messenger.Broker{}
  defer broker.Close()

  topic := "kampuni.scheduler"
  broker.Init("amqp://guest:guest@localhost:5672/")
  broker.PublishOn(topic, read(os.Stdin))
  broker.ReceiveOn(topic, write(os.Stdout))

  // forever := make(chan struct{})
  var forever chan struct{}
  <- forever
}

// These 2 func are for testing
// read is this application's translation to the message format, scanning from
// stdin.
func read(r io.Reader) <-chan messenger.Message {
	lines := make(chan messenger.Message )
	go func() {
		defer close(lines)
		scan := bufio.NewScanner(r)
		for scan.Scan() {
			lines <- scan.Bytes()
		}
	}()
	return lines
}

// write is this application's subscriber of application messages, printing to
// stdout.
func write(w io.Writer) chan<- messenger.Message  {
	lines := make(chan messenger.Message )
	go func() {
		for line := range lines {
			fmt.Fprintln(w, "Receive " + string(line))
		}
	}()
	return lines
}

