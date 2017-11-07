package main

import (
	"github.com/sirupsen/logrus"

	"github.com/rybit/nats-util/cmd"
)

func main() {
	if err := cmd.RootCmd().Execute(); err != nil {
		logrus.Fatalf("Failed to execute command: %v", err)
	}
}
