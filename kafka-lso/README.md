# kafka-archive-to-json

把**已复制的 Kafka 分区前缀**（一串 record batch 字节，message format
magic=2，对应 [Kafka 4.2](https://kafka.apache.org/42/implementation/message-format/)）
转换为已提交样本 JSON：

```json
{"lso": 0, "rows": [{"offset": 0, "key": "azA=", "value": "djA=", "headers": [["h", "MQ=="]]}]}
```

解析与事务裁决全部为自写实现，仅用标准库（`gzip` 解压；CRC32C 优先调用
`crc32c`/`crcmod` 库，缺失时回退到内置纯 Python 表实现）。

## 用法

```
python3 kafka_archive_to_json.py INPUT.bin OUTPUT.json
```

| 退出码 | 含义 |
|---|---|
| 0 | 成功，`OUTPUT.json` 已被原子替换（临时文件 + `os.replace`） |
| 1 | 输入被拒绝；stderr 报告**出错批起点的零基字节偏移**，`OUTPUT.json` 保持原样（旧输出保留） |
| 2 | 用法或 I/O 错误 |

拒绝时的 stderr 形如：

```
rejected: CRC32C mismatch; error batch starts at byte offset 4096
```

## 输入假设

* 输入是分区日志的一个**前缀副本**，可以为空（空日志 → `{"lso": 0, "rows": []}`）。
* 每个批的**头部与标记（控制批）完整**；批内记录可被删减（条数可少于
  `lastOffsetDelta + 1`，如压实日志）；支持**空批**（count=0）与批间**偏移洞**。
* 文件必须恰好由完整批拼接而成：截断的批头/批体、非法 gzip、尾随字节一律拒绝。

## 批格式（magic=2）与校验规则

批头 61 字节（含记录计数），其后是记录区：

| 偏移 | 字段 | 处理 |
|---|---|---|
| 0 | baseOffset int64 | ≥0；批覆盖区间递增不重叠 |
| 8 | length int32 | = 批总字节数−12，≥49；越界即截断 |
| 12 | partitionLeaderEpoch int32 | 忽略 |
| 16 | magic int8 | 必须 = 2 |
| 17 | crc uint32 | CRC32C，覆盖 `[21, 批末)` |
| 21 | attributes int16 | bit0-2 压缩（**仅收 0=none / 1=gzip**）；bit4 事务；bit5 控制；bit3/6/7-15 等保留位忽略 |
| 23 | lastOffsetDelta int32 | ≥0 |
| 27 / 35 | baseTimestamp / maxTimestamp | 忽略 |
| 43 | producerId int64 | 事务批须 ≥0 |
| 51 | producerEpoch int16 | 事务批须 ≥0 |
| 53 | baseSequence int32 | 忽略 |
| 57 | recordCount int32 | ≥0，须与记录区精确对应 |
| 61 | records | gzip 时整体解压（计数不在压缩流内） |

记录：`length` varint（ZigZag，≤5 字节；varlong ≤10 字节）、`attributes`
（忽略）、`timestampDelta`（忽略）、`offsetDelta`、`key`（长度 −1 = null）、
`value`（−1 = null）、`headers`（计数 ≥0；名称为 UTF-8 文字；值可 null）。
记录体必须恰好等于声明长度；记录区必须恰好解析出 `recordCount` 条、无残余。

逐条校验：CRC32C、长度、计数、变长整数与边界；记录 `offsetDelta` **≥0、严格
递增、≤ lastOffsetDelta**；批覆盖区间 `[baseOffset, baseOffset+lastOffsetDelta]`
**非负、递增、互不重叠**（允许空洞）。其他格式（magic≠2、snappy/lz4/zstd）、
截断及尾随字节一律拒绝。

CRC32C 已知答案自测：`crc32c("123456789") = 0xE3069283`（验收中包含）。

## 事务裁决

* 事务批（含控制批）要求 `producerId ≥ 0` 且 `producerEpoch ≥ 0`。
* **数据事务批**：生产者无打开事务时**开启**事务；已打开时必须**同代**
  （同 epoch），否则拒绝。每生产者至多一个打开事务。
* 事务**结束后**该生产者才能换代或写非事务批；打开期间写非事务批 → 拒绝。
  不同生产者的批可任意交错。
* **控制批**：必须是事务批、恰含 1 条记录、key 为 4 字节
  `version(=0) + type(0=ABORT,1=COMMIT)`；必须结束**同代已打开**事务，
  否则拒绝；value 不解释。
* EOF 时仍有打开事务合法。

## LSO 与输出

* `lso` = 最早未结束事务**首个批的 baseOffset**；若无未结束事务，取
  **末批覆盖终点 + 1**；空日志为 0。
* `rows`：仅输出 `offset < lso` 的**非事务**或**已提交**记录；撤销
  （aborted）与控制记录不输出；按 `offset` 升序。
* 字节串一律为**带填充 Base64** 或 `null`；`headers` 为 `[name, value]`
  数组，保留重复与原序；`name` 为 UTF-8 文本。

示例（任务书用例，夹具 `f07_tx_example`）：批 0 开启事务 A、批 1 开启事务
C、批 2 提交 C，A 未结束 → `{"lso": 0, "rows": []}`。

## 字节夹具验收

```
python3 tests/build_fixtures.py    # 重新生成 tests/fixtures/*.bin 与 tests/expected/*.json
python3 tests/run_acceptance.py    # 运行验收（当前 15 个成功夹具 + 27 个错误夹具）
```

`run_acceptance.py` 对每个成功夹具校验退出码 0 且输出 JSON 与期望一致
（并验证旧输出被替换）；对每个错误夹具预置哨兵输出文件，校验退出码 1、
stderr 报告期望的零基字节偏移、且**旧输出保持不变**。

### 成功夹具

| 夹具 | 场景 |
|---|---|
| f01_empty | 空输入 → `lso=0, rows=[]` |
| f02_simple | 普通批：null key/value、重复头、null 头值、UTF-8 头名 |
| f03_gzip | gzip 压缩批 |
| f04_empty_batches | 空批（无压缩 + gzip）与后续批 |
| f05_holes | 批间偏移洞、批内增量跳号 |
| f06_trimmed | 批内记录被删减（count < lastOffsetDelta+1） |
| f07_tx_example | 任务书示例：0 开 A、1 开 C、2 提交 C → `lso=0, rows=[]` |
| f08_commit_abort | 提交/撤销/非事务混合，控制记录不输出 |
| f09_committed_beyond_lso | 已提交但 offset ≥ lso 的记录不输出 |
| f10_reopen | 事务结束后换代、写非事务批 |
| f11_interleave | 多生产者交错均提交，rows 按 offset 升序 |
| f12_ignored_fields | 时间戳/序列号/保留位/记录属性被忽略 |
| f15_tx_empty_open | 空事务批开启事务、控制批结束 |
| f16_tx_trimmed | 已提交事务批内记录被删减 |
| f17_aborted_below_lso | offset < lso 的已撤销记录不输出 |

### 错误夹具（期望退出码 1 + 报告出错批起点）

magic 错误、CRC 不匹配、不支持的压缩、尾随字节、批截断、记录越界、
varint 超长、负计数、计数与记录不符（多/少）、增量不递增、增量超界、
批覆盖重叠、负 baseOffset、控制批非事务、控制批多条记录、控制记录版本
非 0、无打开事务的提交、控制批代不符、打开期间换代、打开期间写非事务批、
事务批负 producerId、伪 gzip、记录长度与内容不符、负增量、length 小于
49、key 越界。

## 文件布局

```
kafka_archive_to_json.py   # 工具本体（单文件、仅标准库）
README.md                  # 本说明
tests/build_fixtures.py    # 夹具生成器（自带独立 CRC32C/编码器，交叉校验）
tests/run_acceptance.py    # 验收运行器
tests/fixtures/*.bin       # 字节夹具
tests/expected/*.json      # 期望输出与错误偏移
```
