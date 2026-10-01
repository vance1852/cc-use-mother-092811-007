# 国际通道韧性编排服务

在综合交通协同基础服务（运营机构、操作者、场所、参考资料登记，角色权限、请求幂等、
SQLite 事务与哈希串联审计）之上，构建面向**国际物流通道中断**的韧性编排能力：
把线路分段、口岸日历、承运能力、货物限制、客户时限、合同费用与风险通告全部按
**不可变版本**组织；中断发生时生成**可解释**的候选路径与受影响清单；只有
**锁定全部分段并完成必要审批**后才能切换；部分失败释放尚未使用的能力但保留已发生
费用；同一货物的竞争方案至多一个生效；迟到的恢复消息不能把已交接货物拉回旧路线；
运营与财务通过 API 分别看到各自视图，并能从任一历史时点复原当时的通道与决定。

## 领域模型

- **版本化参考数据**（`ref_versions`）：`segment` / `border_calendar` / `capacity` /
  `cargo_restriction` / `customer_deadline` / `contract_rate` / `advisory` 七类资料。
  每次发布生成不可变新版本并自增版本号，旧版本以 `superseded_at` 闭合生效区间，
  因此任一时点都能还原当时生效的通道。
- **货物与行程**（`shipments` / `shipment_legs` / `shipment_journal`）：状态机
  `booked → switched → handed_over → delivered`；行程按代（generation）组织，
  改道后旧代以 `released_at` 闭合，已实际走行的前缀分段结转到新代；状态只追加流水。
- **方案**（`plans` / `plan_approvals` / `plan_history`）：`created → locked →
  committed`，失败/撤销进入终态。部分唯一索引保证每件货物至多一个非终态方案。
- **能力占用**（`capacity_holds`）：原始预订、方案锁定、已消费、已释放统一建模，
  按“能力 + 班期”实时统计占用，防止超卖。
- **费用台账**（`cost_entries`）：只追加。原始预订费始终保留，改道时新增新订费、
  按合同计未使用分段违约费与改签费。
- **影响清单**（`disruption_impacts`）：风险通告与受影响货物/分段的幂等对应关系，
  并记录恢复消息到达后的处置（含“已交接忽略”）。

## 关键业务规则

1. **候选路径可解释**：规划器从货物当前物理位置（已发运前缀不重复预订）枚举分段图，
   逐班次给出选取原因与跳过原因（口岸关闭、风险通告、货物限制、余位、衔接、交付窗、
   载重），输出可行方案与被否决方案、费用差异、所需单证与必要审批。
2. **全分段锁定 Saga**：任一分段锁定失败，已获取的锁定全部释放、方案置 `failed`，
   补偿结果随事务提交后再向调用方报错。
3. **审批闸门**：必须先锁定再审批；运营值班审批恒需，改运输方式或晚于原 ETA 需客服
   窗口审批，费用上升需财务审批，触碰风险通告或需通行单证需口岸主管审批；切换前还会按
   最新版本复核通行限制，新阻断使方案失败闭合且不产生费用。
4. **费用保留**：切换只释放**未使用**的旧能力；已发运能力继续占用、已发生费用全部保留，
   违约/改签费按合同费率另计。
5. **交接保护**：货物 `handed_over`/`delivered` 后，迟到的切换请求被拒绝、迟到的恢复
   消息不会把货物拉回旧路线。
6. **竞争互斥**：每件货物至多一个生效中（非终态）方案。
7. **请求幂等**：所有写操作以 `request_id` 去重；状态机操作在状态校验前先查回执，
   客户端超时重试得到同一结果而非状态冲突。
8. **时点复原**：`/reconstruct?at=...` 还原该时点生效的资料版本、货物状态/责任方/ETA、
   当时行程代与方案状态。

## 角色

`admin`、`operator`（运营值班）、`reviewer`（客服/口岸审核）、`finance`（财务）、
`auditor`。运营视图拒绝财务，财务视图拒绝运营；合同费率允许财务发布。

## 目录

- `src/transport_coordination/`
  - `base.py`：服务基类（鉴权、校验、时钟、幂等回执）
  - `access.py`：角色鉴权助手
  - `reference.py`：七类版本化资料与时点快照
  - `planner.py`：纯函数可解释候选路径规划器
  - `orchestration.py`：韧性编排领域服务（资料、货物、中断、方案 Saga、恢复、视图、复原）
  - `service.py` / `storage.py` / `audit.py` / `api.py` / `clock.py`：基础登记、
    SQLite 事务、哈希审计链、HTTP 边界、可替换时钟
- `tests/`：基础规则、编排集成、接口路由、存储与离线验收测试

## 环境

- Linux，Python 3.11+，运行时仅用标准库与 SQLite。

## 测试

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
python3 -m compileall -q src tests
```

## 离线验收

```bash
PYTHONPATH=src python3 -m transport_coordination.acceptance
```

验收在临时 SQLite 库中完成基础登记链与“口岸限流 → 影响清单 → 海运改道方案 →
锁定/审批/切换 → 交接保护 → 时点复原”全链路，成功时输出一行 `status` 为 `ok` 的
JSON，退出码 0。

## HTTP 服务

```bash
PYTHONPATH=src python3 -m transport_coordination.api --database service.sqlite3 \
    --host 127.0.0.1 --port 8080
```

写接口带 `X-Actor-Id`。主要端点：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/references` | 发布版本化资料（七类 entity_type） |
| GET | `/references/{type}/{id}?at=` | 读取当前或指定时点的生效版本 |
| POST | `/shipments` | 登记货物与原始多分段预订（即时占座） |
| POST | `/disruptions/detect` | 按通告扫描并生成受影响清单（幂等） |
| GET | `/disruptions?advisory_id=` | 查询受影响清单及恢复处置 |
| POST | `/recoveries` | 登记中断解除（已交接不拉回，未切换方案自动作废释放） |
| GET | `/candidates?cargo_id=` | 只读可解释候选路径 |
| POST | `/plans` | 基于候选生成改道方案（尚未锁定） |
| POST | `/plans/{id}/lock` | 全分段锁定（部分失败整体回滚） |
| POST | `/plans/{id}/approvals` | 提交一项必要审批 |
| POST | `/plans/{id}/commit` | 审批齐备后切换 |
| POST | `/plans/{id}/cancel` | 撤销未切换方案并释放能力 |
| GET | `/plans/{id}` | 方案详情：候选、改道依据、费用、审批/锁定/历史 |
| POST | `/shipments/{id}/handover` / `/delivered` | 交接 / 交付 |
| GET | `/shipments/{id}/timeline` | 货物状态流水与方案轨迹 |
| GET | `/ops?cargo_id=&at=` | 运营视图：责任方、ETA、状态、改道依据 |
| GET | `/finance?cargo_id=&at=` | 财务视图：费用台账、费用差异、已发生费用 |
| GET | `/reconstruct?at=&cargo_id=` | 任一时点复原 |
