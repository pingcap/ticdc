// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// kafka_auth_server supplies an isolated authenticated Kafka sink for the
// changefeed_update_config integration case. /seen reports consumed row markers.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

func main() {
	password := flag.String("password", "", "test SASL password")
	kafkaPort := flag.Int("kafka-port", 19092, "Kafka listener port")
	apiAddr := flag.String("api-addr", "127.0.0.1:18089", "test control listener")
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.Ports(*kafkaPort),
		kfake.SeedTopics(1, "credentials"), kfake.EnableSASL(), kfake.Superuser("PLAIN", "alice", *password))
	if err != nil {
		log.Fatal(err)
	}
	defer cluster.Close()
	// Allow OAuth clients to reach the token endpoint. Its deliberate failure
	// occurs before SASLAuthenticate, so no OAuth token validation is needed here.
	cluster.ControlKey(17, func(request kmsg.Request) (kmsg.Response, error, bool) {
		if request.(*kmsg.SASLHandshakeRequest).Mechanism != "OAUTHBEARER" {
			return nil, nil, false
		}
		cluster.KeepControl()
		response := request.ResponseKind().(*kmsg.SASLHandshakeResponse)
		response.SupportedMechanisms = []string{"PLAIN", "OAUTHBEARER"}
		return response, nil, true
	})
	client, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...),
		kgo.SASL(plain.Auth{User: "alice", Pass: *password}.AsMechanism()),
		kgo.ConsumeTopics("credentials"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	if err := client.Ping(ctx); err != nil {
		log.Fatal("authenticated Kafka fixture failed to start")
	}

	var mu sync.Mutex
	var records []string
	var tokenCalls uint64
	var wg sync.WaitGroup
	wg.Go(func() {
		for ctx.Err() == nil {
			fetches := client.PollRecords(ctx, 100)
			if !fetches.IsClientClosed() && ctx.Err() == nil && len(fetches.Errors()) != 0 {
				log.Print("Kafka fixture consumer failed")
				cancel()
				return
			}
			mu.Lock()
			fetches.EachRecord(func(record *kgo.Record) { records = append(records, string(record.Value)) })
			mu.Unlock()
		}
	})
	defer wg.Wait()
	defer cancel()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /ready", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /seen/{marker}", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		for _, record := range records {
			if strings.Contains(record, r.PathValue("marker")) {
				w.WriteHeader(http.StatusOK)
				return
			}
		}
		w.WriteHeader(http.StatusNotFound)
	})
	// OAuth failures may echo credentials. Exercise both OAuth client adapters
	// through the API without requiring an OAuth-capable Kafka deployment.
	mux.HandleFunc("POST /token", func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		tokenCalls++
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"invalid_client","error_description":"oauth-credential-sentinel","error_uri":"http://localhost/?client_secret=oauth-credential-sentinel"}`))
	})
	mux.HandleFunc("GET /token-count", func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		_, _ = fmt.Fprint(w, tokenCalls)
	})
	server := &http.Server{
		Addr:              *apiAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	stop := context.AfterFunc(ctx, func() { _ = server.Close() })
	defer stop()
	if err := server.ListenAndServe(); err != nil && ctx.Err() == nil {
		log.Fatal("Kafka fixture control server failed")
	}
}
