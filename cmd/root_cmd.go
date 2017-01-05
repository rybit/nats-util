package cmd

import (
	"os"

	"github.com/Sirupsen/logrus"
	"github.com/nats-io/nats"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/rybit/nats-util/messaging"
)

var log = newLogger()

var enableDebug bool
var servers = []string{}
var caFiles = []string{}
var keyFile string
var certFile string

func RootCmd() *cobra.Command {
	rootCmd := cobra.Command{
		Short: "nats-util",
	}

	listenCmd.Flags().IntP("limit", "l", 0, "a number of messages to listen for")

	rootCmd.AddCommand(listenCmd)

	rootCmd.PersistentFlags().BoolVarP(&enableDebug, "debug", "D", false, "enable debug logging")
	rootCmd.PersistentFlags().StringP("config", "c", "", "a configruation file to use")

	// for connecting to nats
	rootCmd.PersistentFlags().StringSliceVarP(&servers, "servers", "S", []string{}, "the nats server to connect to")
	rootCmd.PersistentFlags().StringVarP(&keyFile, "key_file", "K", "", "the key file to use")
	rootCmd.PersistentFlags().StringVarP(&certFile, "cert_file", "C", "", "the cert file to use")
	rootCmd.PersistentFlags().StringSliceVarP(&caFiles, "ca_files", "A", []string{}, "the ca files to use")

	return &rootCmd
}

func setup(cmd *cobra.Command, args []string) (*logrus.Entry, *nats.Conn) {
	if err := cmd.ParseFlags(args); err != nil {
		log.WithError(err).Fatal("Failed to parse flags")
	}

	if enableDebug {
		logrus.SetLevel(logrus.DebugLevel)
	}

	var config *messaging.NatsConfig
	if configFile, _ := cmd.Flags().GetString("config"); configFile != "" {
		viper.SetConfigFile(configFile)

		if err := viper.ReadInConfig(); err != nil && !os.IsNotExist(err) {
			log.WithError(err).Fatalf("Failed to read in config file: %s", configFile)
		}

		config = new(messaging.NatsConfig)
		if err := viper.Unmarshal(config); err != nil {
			log.WithError(err).Fatalf("Failed to unmarshal config file: %s", configFile)
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

		if certFile != "" && keyFile != "" && len(caFiles) > 0 {
			config.TLS = &messaging.TLSConfig{
				CertFile: certFile,
				CAFiles:  caFiles,
				KeyFile:  keyFile,
			}
		}
	}
	log.WithField("nats_conf", config).Debug("connecting to nats")
	nc, err := messaging.ConnectToNats(config, messaging.ErrorHandler(log))
	if err != nil {
		log.WithError(err).Fatal("Failed to connect to nats")
	}
	log.Debug("Connected to NATS")

	return log, nc
}

func newLogger() *logrus.Entry {
	l := logrus.StandardLogger()
	l.Formatter = &logrus.TextFormatter{
		DisableTimestamp: false,
		FullTimestamp:    true,
	}
	return logrus.NewEntry(l)
}
