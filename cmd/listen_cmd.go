package cmd

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Sirupsen/logrus"
	"github.com/nats-io/go-nats"
	"github.com/nats-io/go-nats-streaming"
	"github.com/nats-io/go-nats-streaming/pb"
	"github.com/spf13/cobra"
)

type connParams struct {
	subject   string
	group     string
	clusterID string
	clientID  string
}

type listenParams struct {
	connParams
	limit     int
	delivery  string
	parseJSON bool
}

func listenCmd() *cobra.Command {
	p := new(listenParams)

	listenCmd := &cobra.Command{
		Use: "listen",
		Run: func(_ *cobra.Command, args []string) {
			listen(p)
		},
		PreRunE: func(_ *cobra.Command, args []string) error {
			switch len(args) {
			case 0:
				return errors.New("Must provide a subject")
			case 1:
				p.subject = args[0]
			case 2:
				p.subject = args[0]
				p.group = args[1]
			}

			return nil
		},
	}
	listenCmd.Flags().IntVarP(&p.limit, "limit", "l", -1, "a number of messages to listen for")
	listenCmd.Flags().StringVar(&p.clientID, "client-id", "nats-util-listener", "a client ID to use if connecting using clustering")
	listenCmd.Flags().StringVar(&p.clusterID, "cluster-id", "", "a cluster ID to use, indicates we should connect to nats-streaming")
	listenCmd.Flags().StringVar(&p.delivery, "delivery", "new", "where to pickup the stream of incoming messages")
	listenCmd.Flags().BoolVar(&p.parseJSON, "json", false, "if the messages should be parsed as json")
	return listenCmd
}

func listen(params *listenParams) {
	nc := connect()

	log := logrus.WithField("subject", params.subject)
	var err error
	var shutdown chan error
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
		start := stan.StartAt(pb.StartPosition_First)
		switch params.delivery {
		case "all":
			start = stan.DeliverAllAvailable()
		case "last":
			start = stan.StartWithLastReceived()
		case "new":
			start = stan.StartAt(pb.StartPosition_NewOnly)
		}
		var sub stan.Subscription
		var h stan.MsgHandler
		if params.group == "" {
			log = log.WithField("group", params.group)
			h, shutdown = stanHandler(params.parseJSON, log)
			sub, err = sc.Subscribe(params.subject, h, start)
		} else {
			h, shutdown = stanHandler(params.parseJSON, log)
			sub, err = sc.QueueSubscribe(params.subject, params.group, h, start)
		}
		defer sub.Unsubscribe()
	} else {
		var sub *nats.Subscription
		var h nats.MsgHandler
		if params.group != "" {
			log = log.WithField("group", params.group)
			h, shutdown = handler(params.parseJSON, log)
			sub, err = nc.QueueSubscribe(params.subject, params.group, h)
		} else {

			h, shutdown = handler(params.parseJSON, log)
			sub, err = nc.Subscribe(params.subject, h)
		}
		defer sub.Unsubscribe()
	}
	if err != nil {
		log.WithError(err).Fatal("Failed to subscribe")
	}
	log.Info("Starting to listen forever")
	err = <-shutdown
	if err != nil {
		log.WithError(err).Fatal("Error while processing message")
	}
}

func stanHandler(parseJSON bool, log *logrus.Entry) (func(*stan.Msg), chan error) {
	shutdown := make(chan error)
	handler := func(msg *stan.Msg) {
		if parseJSON {
			print(msg.Data, log)
		} else {
			fmt.Println(string(msg.Data))
		}
	}
	return handler, shutdown
}

func handler(parseJSON bool, log *logrus.Entry) (func(*nats.Msg), chan error) {
	shutdown := make(chan error)
	handler := func(msg *nats.Msg) {
		if parseJSON {
			print(msg.Data, log)
		} else {
			fmt.Println(string(msg.Data))
		}
	}
	return handler, shutdown
}

func print(data []byte, log *logrus.Entry) {
	parsed := make(map[string]interface{})
	if err := json.Unmarshal(data, &parsed); err != nil {
		pretty, err := json.MarshalIndent(&parsed, "", "  ")
		if err != nil {
			log.WithError(err).Warn("Failed to pretty up the data")
		}
		fmt.Println(string(pretty))
	} else {
		log.WithError(err).Warnf("Failed to parse json msg: %s", string(data))
	}
}
