# TiCDC

[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/pingcap/ticdc)

TiCDC pulls change logs from TiDB clusters and pushes them to downstream systems, such as MySQL, TiDB, Kafka, Pulsar, and Object Storages (e.g., S3). Beginning from v8.5.4, we use this repository to build TiCDC instead of the old repository [tiflow](https://github.com/pingcap/tiflow). The new TiCDC in this repository has undergone a complete architectural redesign while retaining the same user interface. The architectural upgrade primarily aims to address certain drawbacks of TiCDC and propel it forward.

* **Better scalability**. E.g. support over 1 million tables.
* **More efficiency**. Use less machine resource to support large volume.
* **Better maintainability**. E.g. simpler and human readable code, clear code module, and open to extensions.
* **Cloud native architecture**. We want to design a new architecture from the ground to support the cloud.

## Quick Start

### Obtaining TiCDC Download Links

#### Construct the Download Link Manually

1. Get the latest version tag:

   ```bash
   # Get the latest version tag from GitHub API
   export TICDC_VERSION=$(curl -s https://api.github.com/repos/pingcap/ticdc/releases/latest | grep tag_name | cut -d '"' -f 4)
   echo "Latest version is ${TICDC_VERSION}"
   ```

2. Use the tag to build the download link for your platform: `https://tiup-mirrors.pingcap.com/cdc-${TICDC_VERSION}-${os}-${arch}.tar.gz`, for example:
    * For Linux x86-64: `https://tiup-mirrors.pingcap.com/cdc-${TICDC_VERSION}-linux-amd64.tar.gz`
    * For Linux ARM64: `https://tiup-mirrors.pingcap.com/cdc-${TICDC_VERSION}-linux-arm64.tar.gz`
    * For MacOS x86-64: `https://tiup-mirrors.pingcap.com/cdc-${TICDC_VERSION}-darwin-amd64.tar.gz`
    * For MacOS ARM64: `https://tiup-mirrors.pingcap.com/cdc-${TICDC_VERSION}-darwin-arm64.tar.gz`

#### Use TiUP to Retrieve the Download Link

You can also use the tiup command to get the latest platform-specific binary download link:

```bash
tiup install cdc --force
```

This command will provide the download address for the nightly build tailored to your platform.

### Use a Script to Download Automatically

You can use the following script to automatically detect your operating system and architecture, and download the latest TiCDC binary:

```bash
# Detect OS and Architecture
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)
case $ARCH in
    x86_64) ARCH="amd64" ;;
    aarch64|arm64) ARCH="arm64" ;;
esac

# Get the latest version tag from GitHub
TICDC_VERSION=$(curl -s https://api.github.com/repos/pingcap/ticdc/releases/latest | grep tag_name | cut -d '"' -f 4)

# Download the package
URL="https://tiup-mirrors.pingcap.com/cdc-${TICDC_VERSION}-${OS}-${ARCH}.tar.gz"
echo "Downloading TiCDC ${TICDC_VERSION} for ${OS}-${ARCH}..."
curl -L -O "$URL"
```

### Patch to the existing TiCDC nodes

Examples:

```bash
# Scale out some old version TiCDC nodes, if you don't already have some
tiup cluster scale-out test-cluster scale-out.yml

#scale-out.yml
#cdc_servers:
#  - host: 172.31.10.1

# Patch the download binary to the cluster
tiup cluster patch --overwrite test-cluster cdc-${TICDC_VERSION}-linux-amd64.tar.gz -R cdc

# Enable TiCDC new architecture by setting the "newarch" parameter
tiup cluster edit-config test-cluster
#cdc_servers:
# ...
# config:
#    newarch: true

tiup cluster reload test-cluster -R cdc
```

> Note that TiUP has integrated the monitoring dashboard for TiCDC new architecture into the Grafana page, named `<cluster-name>`-TiCDC-New-Arch.

## How to compile TiCDC from source code

### Prerequests

TiCDC can be built on the following operating systems:

* Linux
* MacOS

Install GoLang 1.25.5

```bash
# Linux
wget https://go.dev/dl/go1.25.5.linux-amd64.tar.gz
sudo tar -C /usr/local -xzf go1.25.5.linux-amd64.tar.gz

# MacOS
curl -O https://go.dev/dl/go1.25.5.darwin-amd64.tar.gz
sudo tar -C /usr/local -xzf go1.25.5.darwin-amd64.tar.gz


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
