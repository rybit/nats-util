package cmd

import (
	"os"

	"github.com/nats-io/go-nats"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/rybit/nats-util/messaging"
)

var log = newLogger()

var servers = []string{}

type tlsConfig struct {
	caFiles  []string
	keyFile  string
	certFile string
}

type rootParameters struct {
	configFile  string
	enableDebug bool
}

var rootParams = new(rootParameters)
var tls = new(tlsConfig)

func RootCmd() *cobra.Command {
	rootCmd := cobra.Command{
		Short: "nats-util",
	}

	rootCmd.AddCommand(listenCmd(), sendCmd())

	rootCmd.PersistentFlags().BoolVarP(&rootParams.enableDebug, "debug", "D", false, "enable debug logging")
	rootCmd.PersistentFlags().StringVarP(&rootParams.configFile, "config", "c", "", "a configruation file to use")

	// for connecting to nats
	rootCmd.PersistentFlags().StringSliceVarP(&servers, "servers", "S", []string{}, "the nats server to connect to")
	rootCmd.PersistentFlags().StringVarP(&tls.keyFile, "key_file", "K", "", "the key file to use")
	rootCmd.PersistentFlags().StringVarP(&tls.certFile, "cert_file", "C", "", "the cert file to use")
	rootCmd.PersistentFlags().StringSliceVarP(&tls.caFiles, "ca_files", "A", []string{}, "the ca files to use")

	return &rootCmd
}

func connect() *nats.Conn {
	if rootParams.enableDebug {
		logrus.SetLevel(logrus.DebugLevel)
	}
	log := logrus.WithField("phase", "connection")
	var config *messaging.NatsConfig
	if rootParams.configFile != "" {
		viper.SetConfigFile(rootParams.configFile)

		if err := viper.ReadInConfig(); err != nil && !os.IsNotExist(err) {
			log.WithError(err).Fatalf("Failed to read in config file: %s", rootParams.configFile)
		}

		config = new(messaging.NatsConfig)
		if err := viper.Unmarshal(config); err != nil {
			log.WithError(err).Fatalf("Failed to unmarshal config file: %s", rootParams.configFile)
		}
	}

	if config == nil {
		log.Info("Configuring from the command line")
		if len(servers) == 0 {
			log.Fatal("Must provided at least one server")
		}

		config = &messaging.NatsConfig{
			Servers: servers,
		}

		if tls.certFile != "" && tls.keyFile != "" && len(tls.caFiles) > 0 {
			config.TLS = &messaging.TLSConfig{
				CertFile: tls.certFile,
				CAFiles:  tls.caFiles,
				KeyFile:  tls.keyFile,
			}
		}
	}
	log.WithField("nats_conf", config).Debug("connecting to nats")
	nc, err := messaging.ConnectToNats(config, messaging.ErrorHandler(logrus.WithField("component", "error")))
	if err != nil {
		log.WithError(err).Fatal("Failed to connect to nats")
	}
	log.Debug("Connected to NATS")

	return nc
}

func newLogger() *logrus.Entry {
	l := logrus.StandardLogger()
	l.Formatter = &logrus.TextFormatter{
		DisableTimestamp: false,
		FullTimestamp:    true,
	}
	return logrus.NewEntry(l)
}
