package cmd

import (
	"fmt"
	"time"

	"github.com/nats-io/nats"
	"github.com/spf13/cobra"
	"gopkg.in/square/go-jose.v1/json"
)

var listenCmd = &cobra.Command{
	Use: "listen",
	Run: listen,
}

func listen(cmd *cobra.Command, args []string) {
	log, nc := setup(cmd, args)

	if len(args) == 0 {
		log.Fatal("Must provide a subject to listen to")
	}

	subj := args[0]
	log = log.WithField("subject", subj)
	var subscription *nats.Subscription
	var err error
	if len(args) > 1 {
		group := args[1]
		log = log.WithField("group", group)
		subscription, err = nc.QueueSubscribeSync(subj, group)
	} else {
		subscription, err = nc.SubscribeSync(subj)
	}

	if err != nil {
		log.WithError(err).Fatal("Failed to subscribe")
	}

	log.Info("Starting to listen forever")
	msg, err := subscription.NextMsg(60 * time.Minute)
	for err == nil {
		data := msg.Data
		parsed := make(map[string]interface{})
		if err := json.Unmarshal(msg.Data, &parsed); err != nil {
			pretty, err := json.MarshalIndent(&parsed, "", "  ")
			if err != nil {
				log.WithError(err).Warn("Failed to pretty up the data")
				continue
			}

			data = pretty
		}

		fmt.Println(string(data))

		msg, err = subscription.NextMsg(60 * time.Minute)
	}

}
