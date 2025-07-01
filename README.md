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

# TiDB 数据插入脚本

这是一个简单的 Golang 脚本，用于连接 TiDB 数据库并向 `iac_report_manage` 表插入数据。

## 功能特性

- 连接 TiDB 数据库
- 根据表结构自动生成数据
- 支持所有字段的数据插入
- 错误处理和连接测试

## 表结构

脚本基于以下表结构：

```sql
CREATE TABLE `iac_report_manage` (
  `created_by` varchar(100) NOT NULL,
  `created_date` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_by` varchar(100) NOT NULL,
  `updated_date` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `archive_date` date NOT NULL,
  `deleted_time` datetime NULL,
  `id_iac_report_manage` varchar(32) NOT NULL,
  `unique_id` varchar(100) NULL,
  `report_no` varchar(25) NULL,
  `partner_code` varchar(32) NULL,
  `report_param` text NULL,
  `flow_status` varchar(2) NULL,
  `msg` text NULL,
  `retry_times` tinyint(4) NULL DEFAULT 0,
  `case_times` tinyint(4) NULL,
  PRIMARY KEY (`id_iac_report_manage`),
  UNIQUE KEY `unique_id` (`unique_id`),
  KEY `updated_date` (`updated_date`),
  KEY `flow_status` (`flow_status`),
  KEY `partner_code` (`partner_code`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_general_ci;
```

## 使用方法

### 1. 安装依赖

```bash
go mod tidy
```

### 2. 修改数据库连接参数

编辑 `main.go` 文件中的数据库连接参数：

```go
host := "localhost"           // TiDB 主机地址
port := "4000"               // TiDB 端口
database := "your_database_name"  // 数据库名称
username := "your_username"       // 用户名
password := "your_password"       // 密码
```

### 3. 运行脚本

```bash
go run main.go
```

## 自定义数据

你可以修改 `main.go` 中的 `data` 结构体来自定义插入的数据：

```go
data := IacReportManage{
    CreatedBy:         "admin",                    // 创建者
    CreatedDate:       now,                        // 创建时间
    UpdatedBy:         "admin",                    // 更新者
    UpdatedDate:       now,                        // 更新时间
    ArchiveDate:       archiveDate,                // 归档日期
    DeletedTime:       nil,                        // 删除时间（NULL）
    IDIacReportManage: generateUUID(),             // 主键ID
    UniqueID:          stringPtr("UNIQUE_" + generateUUID()), // 唯一ID
    ReportNo:          stringPtr("REP_" + time.Now().Format("20060102150405")), // 报告编号
    PartnerCode:       stringPtr("PARTNER001"),    // 合作伙伴代码
    ReportParam:       stringPtr(`{"param1": "value1", "param2": "value2"}`), // 报告参数
    FlowStatus:        stringPtr("01"),            // 流程状态
    Msg:               stringPtr("初始化报告"),      // 消息
    RetryTimes:        &retryTimes,                // 重试次数
    CaseTimes:         &caseTimes,                 // 案例次数
}
```

## 注意事项

1. 确保 TiDB 服务正在运行
2. 确保数据库连接参数正确
3. 确保数据库用户有插入权限
4. 脚本会自动生成唯一的主键和唯一ID
5. 时间字段会自动设置为当前时间

## 错误处理

脚本包含完整的错误处理：
- 数据库连接失败
- 连接测试失败
- 数据插入失败

如果遇到错误，脚本会输出详细的错误信息并退出。
