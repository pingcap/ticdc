// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/pingcap/ticdc/pkg/snapshot/provider"
)

func main() {
	root := flag.String("root", "", "shared artifact directory")
	binary := flag.String("cse-binary", "", "CSE batch binary")
	addr := flag.String("listen", "127.0.0.1:18400", "listen address")
	flag.Parse()
	if *root == "" || *binary == "" {
		log.Fatal("root and cse-binary required")
	}
	if e := os.MkdirAll(*root, 0o700); e != nil {
		log.Fatal(e)
	}
	f, e := os.OpenFile(filepath.Join(*root, "provider.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if e != nil {
		log.Fatal(e)
	}
	defer f.Close()
	if e = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		log.Fatal("another controller owns this demo store")
	}
	s := http.Server{Addr: *addr, Handler: provider.New(*root, *binary), ReadHeaderTimeout: 5 * time.Second}
	log.Fatal(s.ListenAndServe())
}
