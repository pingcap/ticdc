# TiCDC

[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/pingcap/ticdc)

TiCDC pulls change logs from TiDB clusters and pushes them to downstream systems, such as MySQL, TiDB, Kafka, Pulsar, and Object Storages (e.g., S3). Beginning from v9.0, we use this repository to build TiCDC instead of the old repository [tiflow](https://github.com/pingcap/tiflow). The new TiCDC in this repository has undergone a complete architectural redesign while retaining the same user interface. The architectural upgrade primarily aims to address certain drawbacks of TiCDC and propel it forward.

* **Better scalability**. E.g. support over 1 million tables.
* **More efficiency**. Use less machine resource to support large volume.
* **Better maintainability**. E.g. simpler and human readable code, clear code module, and open to extensions.
* **Cloud native architecture**. We want to design a new architecture from the ground to support the cloud.

## Quick Start

### Obtaining TiCDC Nightly Build Download Links

#### Construct the Download Link Manually

1. Go to [pingcap/ticdc/tags](https://github.com/pingcap/ticdc/tags) to find the latest tag, e.g. `v9.0.0-beta.1.pre`
2. Use the tag to build the download link for your platform: `https://tiup-mirrors.pingcap.com/cdc-${tag}-nightly-${os}-${arch}.tar.gz`, for example:
    * For Linux x86-64: [https://tiup-mirrors.pingcap.com/cdc-**v9.0.0-beta.1.pre**-nightly-linux-amd64.tar.gz](https://tiup-mirrors.pingcap.com/cdc-v9.0.0-beta.1.pre-nightly-linux-amd64.tar.gz)
    * For Linux ARM64: [https://tiup-mirrors.pingcap.com/cdc-**v9.0.0-beta.1.pre**-nightly-linux-arm64.tar.gz](https://tiup-mirrors.pingcap.com/cdc-v9.0.0-beta.1.pre-nightly-linux-arm64.tar.gz)
    * For MacOS x86-64: [https://tiup-mirrors.pingcap.com/cdc-**v9.0.0-beta.1.pre**-nightly-darwin-amd64.tar.gz](https://tiup-mirrors.pingcap.com/cdc-v9.0.0-beta.1.pre-nightly-darwin-amd64.tar.gz)
    * For MacOS ARM64: [https://tiup-mirrors.pingcap.com/cdc-**v9.0.0-beta.1.pre**-nightly-darwin-arm64.tar.gz](https://tiup-mirrors.pingcap.com/cdc-v9.0.0-beta.1.pre-nightly-darwin-arm64.tar.gz)

#### Use TiUP to Retrieve the Download Link

You can also use the tiup command to get the platform-specific nightly binary download link:

```bash
tiup install cdc:nightly --force
```

This command will provide the download address for the nightly build tailored to your platform.

![TiUP Nightly](./docs/media/tiup-nightly-install.jpg)

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

> Note that TiUP has integrated the monitoring dashboard for TiCDC new architecture into the Grafana page, named `<cluster-name>`-TiCDC-New-Arch. 

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

# 数据库插入脚本

这个脚本可以创建一个只有 id 主键的表，并按指定速率向表中插入记录。

## 安装依赖

```bash
pip install -r requirements.txt
```

如果你使用 PostgreSQL，需要额外安装：
```bash
pip install psycopg2-binary
```

## 使用方法

### 基本用法
```bash
python db_insert_script.py --host localhost --port 3306 --db test_db --rowCount 1000 --rowPerSecond 10
```

### 完整参数
```bash
python db_insert_script.py \
  --host localhost \
  --port 3306 \
  --db test_db \
  --rowCount 1000 \
  --rowPerSecond 10 \
  --username root \
  --password your_password \
  --table my_table \
  --db-type mysql
```

## 参数说明

- `--host`: 数据库主机地址（必需）
- `--port`: 数据库端口（必需）
- `--db`: 数据库名称（必需）
- `--rowCount`: 要插入的记录数（必需）
- `--rowPerSecond`: 每秒插入的记录数（必需）
- `--username`: 数据库用户名（默认：root）
- `--password`: 数据库密码（默认：空）
- `--table`: 表名（默认：test_table）
- `--db-type`: 数据库类型，支持 mysql 或 postgresql（默认：mysql）

## 示例

### MySQL 示例
```bash
# 连接本地 MySQL，插入 5000 条记录，每秒 100 条
python db_insert_script.py --host localhost --port 3306 --db testdb --rowCount 5000 --rowPerSecond 100 --username root --password mypassword

# 连接远程 MySQL，使用默认表名
python db_insert_script.py --host 192.168.1.100 --port 3306 --db production --rowCount 10000 --rowPerSecond 50 --username admin --password secret123
```

### PostgreSQL 示例
```bash
# 连接 PostgreSQL
python db_insert_script.py --host localhost --port 5432 --db testdb --rowCount 1000 --rowPerSecond 20 --username postgres --password mypassword --db-type postgresql
```

## 功能特点

1. **自动创建表**：如果表不存在，脚本会自动创建一个只有 id 主键的表
2. **速率控制**：精确控制插入速率，避免对数据库造成过大压力
3. **进度显示**：实时显示插入进度和统计信息
4. **支持多数据库**：同时支持 MySQL 和 PostgreSQL
5. **错误处理**：完善的错误处理和日志记录
6. **优雅退出**：支持 Ctrl+C 中断操作

## 注意事项

1. 确保数据库服务已启动且可访问
2. 确保指定的数据库已存在
3. 确保用户有创建表和插入数据的权限
4. 大量数据插入时请注意数据库性能和磁盘空间
