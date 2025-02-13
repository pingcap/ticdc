TiCDC
====
TiCDC pulls change logs from TiDB clusters and pushes them to downstream systems, such as MySQL, TiDB, Kafka, Pulsar, and Object Storages (e.g., S3). Beginning from v9.0, we use this repository to build TiCDC instead of the old repository [tiflow](https://github.com/pingcap/tiflow). The new TiCDC in this repository has undergone a complete architectural redesign while retaining the same user interface. The architectural upgrade primarily aims to address certain drawbacks of TiCDC and propel it forward.

* **Better scalability**. E.g. support over 1 million tables.
* **More efficiency**. Use less machine resource to support large volume.
* **Better maintainability**. E.g. simpler and human readable code, clear code module, and open to extensions.
* **Cloud native architecture**. We want to design a new architecture from the ground to support the cloud.

## Quick Start

### Find the latest tag
Go to [pingcap/ticdc/tags](https://github.com/pingcap/ticdc/tags) to find the latest tag, e.g. `v9.0.0-alpha`

### Download the patch binary
* For Linux x86-64: [https://tiup-mirrors.pingcap.com/cdc-**v9.0.0-alpha**-nightly-linux-amd64.tar.gz](https://tiup-mirrors.pingcap.com/cdc-v9.0.0-alpha-nightly-linux-amd64.tar.gz)
* For Linux ARM64: [https://tiup-mirrors.pingcap.com/cdc-**v9.0.0-alpha**-nightly-linux-arm64.tar.gz](https://tiup-mirrors.pingcap.com/cdc-v9.0.0-alpha-nightly-linux-arm64.tar.gz)
* For MacOS x86-64: [https://tiup-mirrors.pingcap.com/cdc-**v9.0.0-alpha**-nightly-darwin-amd64.tar.gz](https://tiup-mirrors.pingcap.com/cdc-v9.0.0-alpha-nightly-darwin-amd64.tar.gz)
* For MacOS ARM64: [https://tiup-mirrors.pingcap.com/cdc-**v9.0.0-alpha**-nightly-darwin-arm64.tar.gz](https://tiup-mirrors.pingcap.com/cdc-v9.0.0-alpha-nightly-darwin-arm64.tar.gz)

### Patch to the existing TiCDC nodes
Examples:
```bash
# Scale out some old version TiCDC nodes, if you don't already have some
tiup cluster scale-out test-cluster scale-out.yml

#scale-out.yml
#cdc_servers:
#  - host: 172.31.10.1

# Patch the download binary to the cluster
tiup cluster patch --overwrite test-cluster cdc-v9.0.0-alpha-nightly-linux-amd64.tar.gz -R cdc

# Enable TiCDC new architecture by setting the "newarch" parameter
tiup cluster edit-config test-cluster
#cdc_servers:
# ...
# config:
#    newarch: true

tiup cluster reload test-cluster -R cdc
```

> TiUP has integrated the monitoring dashboard for TiCDC new architecture into the Grafana page, named <cluster-name>-TiCDC-New-Arch. 

## How to compile TiCDC from source code

### Prerequests
TiCDC can be built on the following operating systems:

* Linux
* MacOS

Install GoLang 1.23.2

```bash
# Linux
wget https://go.dev/dl/go1.23.2.linux-amd64.tar.gz
sudo tar -C /usr/local -xzf go1.23.2.linux-amd64.tar.gz

# MacOS
curl -O https://go.dev/dl/go1.23.2.darwin-amd64.tar.gz
sudo tar -C /usr/local -xzf go1.23.2.darwin-amd64.tar.gz


export PATH=$PATH:/usr/local/go/bin
export GOPATH=$HOME/go
export PATH=$PATH:$GOPATH/bin
```

### Download the source code and compile

1. Download the code
```bash
git clone git@github.com:pingcap/ticdc.git
cd ticdc
```

2. Build TiCDC

```bash
make cdc

# Generate the patchable tar file
cd bin
tar -czf newarch_cdc.tar.gz cdc
```