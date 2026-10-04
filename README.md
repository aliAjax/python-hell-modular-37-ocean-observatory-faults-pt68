# 海底观测网设备故障管理

这是一个只使用Python标准库和SQLite的模块化原型项目，默认端口为`8337`。领域对象包括站点、资产、链路、遥测、故障事件、恢复动作、出海任务和数据缺口。`app.py`只负责参数解析、依赖组装和服务生命周期，业务状态机与约束集中在`src/rules.py`。

## 近海站网断网—回网同步

近海站网断网后先在本地登记，回网后按固定顺序处理：**先合并故障事件、遥测和恢复动作，再与中心对账**。相关逻辑在`src/sync.py`（编排）与`src/merge.py`（三向合并）：

- **先登记**：断网期间记录只写入持久队列`sync_outbox`（`queued`），不做合并。
- **回网合并**：按稳定编号`stable_id`逐字段三向合并（中心版、站点版、断网前共同基线`base`）。仅一方修改直接采纳；双方都改同一字段时，修订号/严重级别等可裁决字段自动取高，其余字段保留两版。
- **测量和处置各留两版**：冲突字段分别存入`entity_versions`的`center`/`station`分支，实体标记`divergent`；**选定版本前不允许解决/关闭故障事件**（见`rules.py`的`resolve`校验）。
- **后到留待处理副本**：中心已存在活动恢复动作（同`dedupe_key`）或重叠时间窗的数据缺口时，后到提交进入`pending_copies`，可人工`apply`/`reject`。
- **新遥测使旧关闭依据失效**：更高修订号的遥测到达后，已`resolved`/`closed`的相关事件自动重开为`open`并写审计。
- **同一处置先入库者胜**：同一事件同一`disposition_key`（默认取`action_type`）由`disposition_claims`唯一约束原子仲裁，两人并发提交只有先入库一方生效。
- **失败重试、重启续传**：写入失败的步骤退回`queued`稍后重试；进程重启时未完成的`inflight`步骤自动回到队列接着做。

### 同步接口

- `POST /api/sync/stations/<station_id>/queue`：断网登记，请求体`{"records":[...]}`，状态码202，仅入队。
- `POST /api/sync/stations/<station_id>/reconnect`：回网后先排空队列合并，再对账，返回对账报告。
- `POST /api/sync/batches`：直接提交一个回传批次（只合并不对账）。
- `POST /api/sync/stations/<station_id>/reconcile`：单独触发对账。
- `GET /api/sync/versions/<stable_id>`：查看某稳定编号保留的两版。
- `POST /api/sync/versions/<stable_id>/<version_id>`：择一（空请求体），选定后事件方可关闭。
- `GET /api/sync/pending?status=pending`：待处理副本列表。
- `POST /api/sync/pending/<id>`：`{"decision":"apply"}`或`{"decision":"reject"}`。
- `GET /api/sync/stations/<station_id>/outbox?status=queued`：查看登记队列。
- `GET /api/sync/stations/<station_id>/reconciliations`：历史对账记录。

同步角色为`admin`/`operator`/`engineer`/`field`。记录可携带`"actor_id"`以现场操作员身份留痕，跨对象引用（`incident_id`等）可直接使用稳定编号，系统按`stable_index`解析为中心实体编号。

## 模块结构

- `app.py`：命令行参数、依赖组装、启动和信号处理。
- `src/domain.py`：角色、领域异常、身份解析和实体数据结构。
- `src/rules.py`：状态机、角色权限、领域计算、冲突和跨对象校验。
- `src/repository.py`：SQLite建表、查询、事务、乐观锁、审计、幂等键和同步持久表。
- `src/service.py`：用例编排、版本控制和审计写入。
- `src/sync.py`：断网登记、回网合并、两版择一、待处理副本、重开重算和对账。
- `src/merge.py`：纯函数三向字段合并与状态合并。
- `src/http_api.py`：HTTP路由、JSON解析和统一错误响应。
- `src/audit.py`：实体操作审计时间线。
- `static/index.html`：最小演示页面。
- `tests/`：完整流程、规则、失败场景和断网回网同步测试。

## 初始化与启动

```bash
python3 app.py --db ./data.db --port 8337
```

服务启动时自动建表。`--host`可修改监听地址，`--db`可指定其他SQLite文件。初始化服务不需要单独命令，首次启动即可访问：

```bash
curl http://127.0.0.1:8337/health
```

## 主要接口

- `GET /health`：健康检查。
- `GET /api/<kind>`：按对象类型查询，可用`?status=`过滤。
- `POST /api/<kind>`：创建对象；请求体为JSON。
- `GET /api/entities/<id>`：读取对象当前版本。
- `POST /api/entities/<id>/actions`：提交`{"action":"动作名","data":{...},"expected_version":数字}`。
- `GET /api/audit`：读取审计记录。

身份通过`X-User-Id`和`X-Role`请求头传入，角色和动作权限由规则引擎校验。## 核心流程

建立站点、资产和链路后记录遥测与故障事件，创建恢复动作并跟踪重启、备用链路、出海任务和数据缺口，最后关闭事件。遥测`revise`动作只接受更高修订号，用于处理迟到数据。

## 规则重点

- 同一资产和故障类型不能同时有多个活动事件。
- 恢复动作按`dedupe_key`防止重复执行。
- 事件解决前恢复动作、数据缺口和受影响资产必须达到可关闭状态。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 局限

项目使用请求头模拟身份、SQLite单机持久化和简化状态机，适合原型演示和流程验证，不替代行业正式系统、设备控制系统或现场安全规程。
