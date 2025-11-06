// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"encoding/json"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cmd/util"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/logger"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/version"
	"github.com/pingcap/ticdc/server"
	ticonfig "github.com/pingcap/tidb/pkg/config"
	tiflowServer "github.com/pingcap/tiflow/pkg/cmd/server"
	tiflowConfig "github.com/pingcap/tiflow/pkg/config"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
)

// options defines flags for the `server` command.
type options struct {
	serverConfig         *config.ServerConfig
	pdEndpoints          []string
	serverConfigFilePath string

	caPath        string
	certPath      string
	keyPath       string
	allowedCertCN string
}

// newOptions creates new options for the `server` command.
func newOptions() *options {
	return &options{
		serverConfig: config.GetDefaultServerConfig(),
	}
}

// addFlags receives a *cobra.Command reference and binds
// flags related to template printing to it.
func (o *options) addFlags(cmd *cobra.Command) {
	cmd.Flags().BoolVarP(&o.serverConfig.Newarch, "newarch", "x", o.serverConfig.Newarch, "Run the new architecture of TiCDC server")
	cmd.Flags().StringVar(&o.serverConfig.ClusterID, "cluster-id", "default", "Set cdc cluster id")
	cmd.Flags().StringVar(&o.serverConfig.Addr, "addr", o.serverConfig.Addr, "Set the listening address")
	cmd.Flags().StringVar(&o.serverConfig.AdvertiseAddr, "advertise-addr", o.serverConfig.AdvertiseAddr, "Set the advertise listening address for client communication")

	cmd.Flags().StringVar(&o.serverConfig.TZ, "tz", o.serverConfig.TZ, "Specify time zone of TiCDC cluster")
	cmd.Flags().Int64Var(&o.serverConfig.GcTTL, "gc-ttl", o.serverConfig.GcTTL, "CDC GC safepoint TTL duration, specified in seconds")

	cmd.Flags().StringVar(&o.serverConfig.LogFile, "log-file", o.serverConfig.LogFile, "log file path")
	cmd.Flags().StringVar(&o.serverConfig.LogLevel, "log-level", o.serverConfig.LogLevel, "log level (etc: debug|info|warn|error)")

	cmd.Flags().StringVar(&o.serverConfig.DataDir, "data-dir", o.serverConfig.DataDir, "the path to the directory used to store TiCDC-generated data")

	cmd.Flags().StringSliceVar(&o.pdEndpoints, "pd", []string{"http://127.0.0.1:2379"}, "Set the PD endpoints to use. Use ',' to separate multiple PDs")
	cmd.Flags().StringVar(&o.serverConfigFilePath, "config", "", "Path of the configuration file")

	cmd.Flags().StringVar(&o.caPath, "ca", "", "CA certificate path for TLS connection")
	cmd.Flags().StringVar(&o.certPath, "cert", "", "Certificate path for TLS connection")
	cmd.Flags().StringVar(&o.keyPath, "key", "", "Private key path for TLS connection")
	cmd.Flags().StringVar(&o.allowedCertCN, "cert-allowed-cn", "", "Verify caller's identity (cert Common Name). Use ',' to separate multiple CN")
}

// run runs the server cmd.
func (o *options) run(cmd *cobra.Command) error {
	loggerConfig := &logger.Config{
		File:                 o.serverConfig.LogFile,
		Level:                o.serverConfig.LogLevel,
		FileMaxSize:          o.serverConfig.Log.File.MaxSize,
		FileMaxDays:          o.serverConfig.Log.File.MaxDays,
		FileMaxBackups:       o.serverConfig.Log.File.MaxBackups,
		ZapInternalErrOutput: o.serverConfig.Log.InternalErrOutput,
	}
	err := logger.InitLogger(loggerConfig)
	if err != nil {
		cmd.Printf("init logger error %v\n", errors.Trace(err))
		os.Exit(1)
	}
	log.Info("init log", zap.String("file", loggerConfig.File), zap.String("level", loggerConfig.Level))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	version.LogVersionInfo("Change Data Capture (CDC)")
	log.Info("The TiCDC release version", zap.String("ReleaseVersion", version.ReleaseVersion))

	util.LogHTTPProxies()
	metrics.RecordGoRuntimeSettings()
	svr, err := server.New(o.serverConfig, o.pdEndpoints)
	if err != nil {
		log.Error("create cdc server failed", zap.Error(err))
		return errors.Trace(err)
	}
	log.Info("TiCDC(new arch) server created",
		zap.Strings("pd", o.pdEndpoints), zap.Stringer("config", o.serverConfig))

	// shutdown is used to notify the server to shutdown (by closing the context) when receiving the signal, and exit the process.
	shutdown := func() <-chan struct{} {
		done := make(chan struct{})
		cancel()
		close(done)
		return done
	}
	// Gracefully shutdown the server when receiving the signal, and exit the process.
	util.InitSignalHandling(shutdown, cancel)

	err = svr.Run(ctx)
	if err != nil && !errors.Is(errors.Cause(err), context.Canceled) {
		log.Error("cdc server exits with error", zap.Error(err))
	} else {
		log.Info("cdc server exits normally")
	}
	// Gracefully close the server, and exit the process.
	ch := make(chan struct{})
	ticker := time.NewTicker(server.GracefulShutdownTimeout)
	defer ticker.Stop()
	go func() {
		svr.Close(ctx)
		close(ch)
	}()
	select {
	case <-ch:
	case <-ticker.C:
		log.Warn("graceful shutdown timeout, exit server")
	}
	return err
}

// complete adapts from the command line args and config file to the data required.
func (o *options) complete(command *cobra.Command) error {
	cfg := config.GetDefaultServerConfig()
	if len(o.serverConfigFilePath) > 0 {
		// strict decode config file, but ignore debug item
		if err := util.StrictDecodeFile(o.serverConfigFilePath, "TiCDC server", cfg, config.DebugConfigurationItem); err != nil {
			return err
		}
	}

	o.serverConfig.Security = o.getCredential()
	command.Flags().Visit(func(flag *pflag.Flag) {
		switch flag.Name {
		case "addr":
			cfg.Addr = o.serverConfig.Addr
		case "advertise-addr":
			cfg.AdvertiseAddr = o.serverConfig.AdvertiseAddr
		case "tz":
			cfg.TZ = o.serverConfig.TZ
		case "gc-ttl":
			cfg.GcTTL = o.serverConfig.GcTTL
		case "log-file":
			cfg.LogFile = o.serverConfig.LogFile
		case "log-level":
			cfg.LogLevel = o.serverConfig.LogLevel
		case "data-dir":
			cfg.DataDir = o.serverConfig.DataDir
		case "ca":
			cfg.Security.CAPath = o.serverConfig.Security.CAPath
		case "cert":
			cfg.Security.CertPath = o.serverConfig.Security.CertPath
		case "key":
			cfg.Security.KeyPath = o.serverConfig.Security.KeyPath
		case "cert-allowed-cn":
			cfg.Security.CertAllowedCN = o.serverConfig.Security.CertAllowedCN
		case "cluster-id":
			cfg.ClusterID = o.serverConfig.ClusterID
		case "pd", "config":
			// do nothing
		case "newarch", "x":
			cfg.Newarch = o.serverConfig.Newarch
		default:
			log.Panic("unknown flag, please report a bug", zap.String("flagName", flag.Name))
		}
	})

	if err := cfg.ValidateAndAdjust(); err != nil {
		return errors.Trace(err)
	}

	if cfg.DataDir == "" {
		command.Printf(color.HiYellowString("[WARN] TiCDC server data-dir is not set. " +
			"Please use `cdc server --data-dir` to start the cdc server if possible.\n"))
	}

	o.serverConfig = cfg
	config.StoreGlobalServerConfig(o.serverConfig)
	return nil
}

// validate checks that the provided attach options are specified.
func (o *options) validate() error {
	if len(o.pdEndpoints) == 0 {
		return errors.ErrInvalidServerOption.GenWithStack("empty PD address")
	}
	log.Info("validate pd address", zap.Strings("pd", o.pdEndpoints), zap.Int("pdCount", len(o.pdEndpoints)))
	for _, ep := range o.pdEndpoints {
		// NOTICE: The configuration used here is the one that has been completed,
		// as it may be configured by the configuration file.
		if err := util.VerifyPdEndpoint(ep, o.serverConfig.Security.IsTLSEnabled()); err != nil {
			return errors.WrapError(errors.ErrInvalidServerOption, err)
		}
	}
	return nil
}

// getCredential returns security credential.
func (o *options) getCredential() *security.Credential {
	var certAllowedCN []string
	if len(o.allowedCertCN) != 0 {
		certAllowedCN = strings.Split(o.allowedCertCN, ",")
	}

	return &security.Credential{
		CAPath:        o.caPath,
		CertPath:      o.certPath,
		KeyPath:       o.keyPath,
		CertAllowedCN: certAllowedCN,
	}
}

func parseConfigFlagFromOSArgs() string {
	var serverConfigFilePath string
	for i, arg := range os.Args[1:] {
		if strings.HasPrefix(arg, "--config=") {
			serverConfigFilePath = strings.SplitN(arg, "=", 2)[1]
		} else if arg == "--config" && i+2 < len(os.Args) {
			serverConfigFilePath = os.Args[i+2]
		}
	}
	log.Info("parse config file path from os.Args", zap.String("config", serverConfigFilePath))

	// If the command is `cdc cli changefeed`, means it's not a server config file.
	if slices.Contains(os.Args, "cli") && slices.Contains(os.Args, "changefeed") {
		serverConfigFilePath = ""
	}

	return serverConfigFilePath
}

func isNewArchEnabledByConfig(serverConfigFilePath string) bool {
	cfg := config.GetDefaultServerConfig()
	if len(serverConfigFilePath) > 0 {
		// strict decode config file, but ignore debug item
		if err := util.StrictDecodeFile(serverConfigFilePath, "TiCDC server", cfg, config.DebugConfigurationItem); err != nil {
			log.Error("failed to parse server configuration, please check the config file for errors and try again.", zap.Error(err))
			return false
		}
	}

	return cfg.Newarch
}

func isNewArchEnabled(o *options) bool {
	newarch := o.serverConfig.Newarch
	if newarch {
		log.Debug("Set newarch from command line")
		return newarch
	}

	newarch = os.Getenv("TICDC_NEWARCH") == "true"
	if newarch {
		log.Debug("Set newarch from environment variable")
		return newarch
	}

	serverConfigFilePath := parseConfigFlagFromOSArgs()
	newarch = isNewArchEnabledByConfig(serverConfigFilePath)
	if newarch {
		log.Debug("Set newarch from config file")
	}
	return newarch
}

func runTiFlowServer(o *options, cmd *cobra.Command) error {
	cfgData, err := json.Marshal(o.serverConfig)
	if err != nil {
		return errors.Trace(err)
	}

	var oldCfg tiflowConfig.ServerConfig
	err = json.Unmarshal(cfgData, &oldCfg)
	if err != nil {
		return errors.Trace(err)
	}

	var oldOptions tiflowServer.Options
	oldOptions.ServerConfig = &oldCfg
	oldOptions.ServerPdAddr = strings.Join(o.pdEndpoints, ",")
	oldOptions.ServerConfigFilePath = o.serverConfigFilePath
	oldOptions.CaPath = o.caPath
	oldOptions.CertPath = o.certPath
	oldOptions.KeyPath = o.keyPath
	oldOptions.AllowedCertCN = o.allowedCertCN

	return tiflowServer.Run(&oldOptions, cmd)
}

// NewCmdServer creates the `server` command.
func NewCmdServer() *cobra.Command {
	o := newOptions()

	command := &cobra.Command{
		Use:   "server",
		Short: "Start a TiCDC server server",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if isNewArchEnabled(o) {
				log.Info("Running TiCDC server in new architecture")
				err := o.complete(cmd)
				if err != nil {
					return err
				}
				err = o.validate()
				if err != nil {
					return err
				}
				err = o.run(cmd)
				cobra.CheckErr(err)
				return nil
			}
			log.Info("Running TiCDC server in old architecture")
			return runTiFlowServer(o, cmd)
		},
	}
	patchTiDBConfig()
	o.addFlags(command)
	return command
}

func patchTiDBConfig() {
	ticonfig.UpdateGlobal(func(conf *ticonfig.Config) {
		// Disable kv client batch send loop introduced by tidb library, it's not used by the TiCDC server.
		conf.TiKVClient.MaxBatchSize = 0
	})
}
