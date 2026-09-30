# 国际通道韧性编排服务

在综合交通协同基础服务（组织/操作者/角色、幂等回执、SQLite 事务、哈希串联审计）
之上，构建国际物流通道的中断韧性编排能力：把线路分段、口岸日历、承运能力、
货物限制、客户时限、合同费用与风险通告按**版本**组织起来；口岸限流等中断
发生时生成**可解释的候选路径与受影响清单**；只有锁定全部分段并完成必要
审批后才能切换；部分失败释放尚未使用的能力但保留已发生费用；同一货物的
竞争方案只有一个生效，迟到的恢复消息不能把已交接货物拉回旧路线；运营与
财务通过各自 API 视图查看责任方、ETA、改道依据与费用差异，并能从任一历史
时点复原当时的通道与决定。

## 目录

- `src/transport_coordination/`
  - `domain.py` `models.py` `service.py`：基础登记、角色与权限
  - `storage.py`：SQLite 建表与事务（含通道域表）
  - `audit.py` `clock.py` `errors.py`：哈希审计链、时钟、业务异常
  - `planning.py`：候选路径枚举与可行性校验（纯函数引擎）
  - `corridor.py`：韧性编排服务（版本资料、评估、saga 切换、视图、时点复原）
  - `api.py`：HTTP/JSON 路由
  - `acceptance.py` / `acceptance_corridor.py`：两条离线端到端验收
- `tests/`：基础规则、事务、HTTP 路由、通道编排与端到端验收测试

## 环境

- Linux；Python 3.11+；仅使用 Python 标准库与 SQLite。

## 测试

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

## 构建检查

```bash
python3 -m compileall -q src tests
```

## 离线验收

```bash
PYTHONPATH=src python3 -m transport_coordination.acceptance
PYTHONPATH=src python3 -m transport_coordination.acceptance_corridor
```

通道验收会构建"铁路基线 + 海运备选 + 公路接驳"网络，制造口岸限流，核对
受影响清单、候选解释、审批门禁、部分失败补偿（释放能力、保留承诺费）、
幂等切换、交接冻结、费用差异与历史复原，成功时输出 `status` 为 `ok` 的
JSON 并以退出码 `0` 结束。

## HTTP 服务

```bash
PYTHONPATH=src python3 -m transport_coordination.api --database service.sqlite3 \
    --host 127.0.0.1 --port 8080
```

写入接口通过 `X-Actor-Id` 标识操作者；`request_id` 提供请求幂等。

### 基础接口

`POST /organizations`、`POST /actors`、`POST /sites`、
`POST /domain-records`、`GET /domain-records`、`GET /audit-events`、
`GET /health`。

### 通道韧性接口

| 方法 & 路径 | 说明 | 允许角色 |
| --- | --- | --- |
| `POST /corridor/references` | 发布版本化资料：`segment` / `border_calendar` / `capacity` / `cargo_restriction` / `contract_rate` | 运营/管理员（费用需财务、限制可由安全） |
| `POST /corridor/notices` | 发布风险通告（影响口岸/分段/节点/承运方） | 运营/管理员/安全 |
| `POST /corridor/notices/recover` | 登记恢复；不回拉已交接或已切换货物 | 运营/管理员/安全 |
| `POST /corridor/shipments` | 登记货物与首个客户交付窗 | 运营/管理员 |
| `POST /corridor/deadlines` | 发布交付窗新版本 | 运营/管理员 |
| `POST /corridor/baselines` | 登记中断前既订基线路线 | 运营/管理员 |
| `POST /corridor/evaluations` | 中断评估：受影响清单 + 可解释候选路径 | 运营/管理员 |
| `POST /corridor/plans/select` | 选定一个候选（同货唯一待执行方案） | 运营/管理员 |
| `POST /corridor/plans/approve` | 按方案要求登记审批（admin/finance/safety） | 对应审批角色 |
| `POST /corridor/plans/switch` | 锁定全部分段并在审批齐备后原子切换 | 运营/管理员 |
| `POST /corridor/checkpoints` | 登记节点事件；`handed_over` 后路线冻结 | 运营/管理员 |
| `GET  /corridor/plans` | 查询候选/方案及解释（可按货物过滤） | 已登记操作者 |
| `GET  /corridor/ops-view` | 运营视图：当前责任方、ETA、改道依据、是否准点 | 运营/管理员/安全/审计 |
| `GET  /corridor/finance-view` | 财务视图：基线 vs 当前费用、差异、沉没/罚没费用 | 财务/管理员/审计 |
| `GET  /corridor/history?at=…` | 复原任一时点的资料版本、通告、方案状态与决定 | 已登记操作者 |

`ops-view` 与 `finance-view` 支持 `as_of` 查询参数，按历史时点返回。

## 关键业务规则

- **候选可行性**逐分段给出解释：通告命中的口岸/分段/节点/承运方为硬阻断；
  货物限制（按运输方式/口岸/承运方作用域）与班期不覆盖、余量不足为硬
  阻断；口岸日历关闭只产生等待，等待叠加在途时间后若超过交付窗则该路径
  不可行。不可行路径同样返回，附明被拒原因。
- **必要审批**按改道内容推导：更换承运组合需管理员；费用较基线上涨超过
  10% 需财务；危险品更换过境口岸需安全。审批不齐不能切换。
- **Saga 切换**：方案经 `selected → switching → effective`。逐段锁定
  （`BEGIN IMMEDIATE` 串行化并发余量核算），任一段失败即释放本次全部
  `held` 锁定、承诺费以 `commitment_forfeited` 记账，方案回到 `selected`
  可重试；进程崩溃留下的 `switching` 状态在下次尝试时同样先补偿。成功
  切换在单事务内把新锁定置为 `committed`、释放旧路线能力（未用能力的
  承诺费以 `sunk_commitment` 记账）、作废旧基线与全部竞争候选。
- **唯一生效**：同一货物同时最多一个 `selected/switching` 方案；切换后
  该货物恰好一个 `effective` 方案，其他候选标记 `obsolete`，路由
  `epoch` 递增。
- **交接冻结**：`handed_over` 只能在目的地登记；此后任何切换请求被拒，
  迟到的恢复消息只改变后续规划并在结果中列出"未被回拉的已交接货物"。
- **时点复原**：资料以 `(kind, entity_id, version)` 保存、按
  `published_at <= at` 取最新；通告按恢复时刻判定活跃；方案状态由状态
  历史还原；决定（评估、选定、审批、锁定失败、切换、交接）可逐笔追溯。
