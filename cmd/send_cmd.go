package cmd

import (
	"bufio"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/nats-io/go-nats-streaming"
	"github.com/spf13/cobra"
)

type sendParams struct {
	connParams
	file  string
	times int
}

func sendCmd() *cobra.Command {
	p := new(sendParams)
	cmd := cobra.Command{
		Use: "send",
		Run: func(_ *cobra.Command, _ []string) {
			send(p)
		},
		PreRunE: func(_ *cobra.Command, args []string) error {
			if len(args) == 0 {
				return errors.New("Must provide a subject")
			}
			p.subject = args[0]
			return nil
		},
	}

	cmd.Flags().StringVar(&p.clientID, "client-id", "nats-util-sender", "a client ID to use if connecting using clustering")
	cmd.Flags().StringVar(&p.clusterID, "cluster-id", "", "a cluster ID to use, indicates we should connect to nats-streaming")
	cmd.Flags().StringVar(&p.file, "file", "", "a file to read and send")
	cmd.Flags().IntVar(&p.times, "times", 1, "the number of times to send data from a file")
	return &cmd
}

func send(params *sendParams) {
	nc := connect()
	log := log.WithField("subject", params.subject)
	pub := nc.Publish
	if params.clusterID != "" {
		log = log.WithFields(logrus.Fields{
			"clusterID": params.clusterID,
			"clientID":  params.clientID,
		})
		opts := stan.NatsConn(nc)
		sc, err := stan.Connect(params.clusterID, params.clientID, opts)
		if err != nil {
			log.WithError(err).Fatal("Failed to connect to NATS streaming")
		}
		defer sc.Close()
		pub = sc.Publish
	}

	if params.file == "" {
		// read from stdin
		reader := bufio.NewReader(os.Stdin)
		for {
			fmt.Print("Enter message: ")
			data, err := reader.ReadString('\n')
			if err != nil {
				log.WithError(err).Warn("failed to read data from stdin")
				continue
			}
			pub(params.subject, []byte(strings.TrimRight(data, "\n")))
		}

	} else {
		data, err := ioutil.ReadFile(params.file)
		if err != nil {
			log.WithError(err).Fatal("Failed to read data from file")
		}
		for i := params.times; i > 0; i-- {
			pub(params.subject, data)
		}
	}
}

type publisher func(string, []byte)
