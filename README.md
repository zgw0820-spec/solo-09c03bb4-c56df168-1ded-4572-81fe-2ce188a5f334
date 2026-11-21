# PostgreSQL 故障切换值班台（FastAPI + PostgreSQL 16 + Docker Compose）

值班员在一次主库故障中录入：

- 主库最后提交的 LSN（提交水位）与 timeline；
- 每个候选副本的 timeline 与 replay LSN；
- 归档 WAL 区间（segment 文件名，含 timeline 前缀）。

系统判定哪些副本**可以接任**、每个被淘汰副本的**具体原因**，并支持在页面上确认接任、
处理旧主库迟到的恢复回报。

## 接任规则（核心业务约束）

候选副本只有在**同一条 timeline 上能连续重放到提交水位**时才可接任，判定顺序：

1. **timeline 不一致** → 淘汰。分叉 timeline 上的副本无法沿主库历史重放。
2. **replay LSN 超过提交水位** → 淘汰。包含主库在本代次从未提交过的记录，属于分叉历史。
3. **replay LSN 等于提交水位** → 可直接接任。
4. **replay LSN 落后** → 计算从 replay 位置到水位之间所需的全部 WAL segment；
   只要归档里缺任意一个 segment（归档不连续）就淘汰，否则可在补齐归档后连续重放至水位。

LSN 按 64 位整数处理（`hi/lo` 十六进制），segment 号 = `LSN / 16MiB`（PostgreSQL 默认
`wal_segment_size`），文件名格式 `T<8位timeline><16位segment号>`。

## 并发与一致性保证

| 场景 | 处理方式 |
| --- | --- |
| 新观测与接任确认交错，页面已过期 | 每个事件带单调 `version`，表单提交 `expected_version`；不匹配返回 `409 stale_version`，过期页面**不能覆盖已确认的主库代次** |
| 重复的接任确认 / 检查 / 恢复回报 | 幂等键（`request_id` / `client_request_id`）落在 `idempotency_records` 表；重放只返回**第一次已生效的结果**，且幂等检查先于版本检查，旧页面重放同样拿到原结果 |
| 旧主库迟到的恢复回报 | 与当前生效主库/代次不符的回报只写入审计观测（`accepted=false`），**不 bump 版本、不改变生效主库**，响应里回传当前真正的主库 |
| 检查失败（无可接任副本） | 记录一条 `failed_kept_old_primary` 检查及全部淘汰原因，**不产生新代次、不 bump 版本，继续保留上次主库**，值班员修正数据后可重试 |
| 并发写 | PostgreSQL 下每个写事务先 `SELECT ... FOR UPDATE` 锁定事件行，再做版本/幂等判断 |

## 目录结构

```
app/
  lsn.py        纯函数：LSN 解析、segment 命名、归档连续性/缺口计算
  failover.py   纯函数：接任资格规则（无数据库依赖，可单测）
  models.py     SQLAlchemy 模型、引擎与会话
  schemas.py    Pydantic 请求模型
  service.py    事务服务层：版本控制、幂等、评估落库、接任/回报状态机
  main.py       FastAPI 路由（JSON API + Jinja2 页面）
  templates/    index.html（事件列表/建单）、incident.html（值班台）
scripts/
  seed.py             本地样例数据（含 4 种判定结果）
  run_local_demo.sh   一键本地演示（SQLite）
tests/          25 个测试：纯规则单测 + 服务层状态机 + HTTP 端到端
docker-compose.yml / Dockerfile
```

## 方式一：Docker Compose（PostgreSQL 16）

```bash
docker compose up --build
# Web:   http://localhost:8000
# DB:    postgres:16，localhost:5432（用户/库名默认 failover）
docker compose exec web python -m scripts.seed   # 载入样例事件
```

首次启动自动建表（`Base.metadata.create_all`）。

## 方式二：本地直接运行（无需 Docker，用 SQLite 演示）

```bash
python3 -m venv --without-pip .venv          # 若无 pip 可先 bootstrap
.venv/bin/pip install -r requirements-dev.txt
bash scripts/run_local_demo.sh               # 自动建库、播种、起服务
# 或手动：
export DATABASE_URL="sqlite:///$(pwd)/demo.db"
.venv/bin/python -m scripts.seed
.venv/bin/uvicorn app.main:app --port 8000
```

## 样例事件说明

水位 `timeline=1, LSN=0/3000100`（位于 segment 3）：

| 副本 | 观测 | 归档情况 | 结论 |
| --- | --- | --- | --- |
| replica-1 | t1，replay 已到水位 | — | ✅ 可接任（已追平） |
| replica-2 | t1，replay 在 segment 2 | 归档含 segment 2–3 | ✅ 可接任（补齐后连续重放） |
| replica-3 | t1，replay 在 segment 1 | 缺 segment 1 | ❌ 归档 WAL 不连续 |
| replica-4 | timeline 2 | — | ❌ timeline 与主库不一致 |

页面上可依次体验：执行检查（看淘汰原因）→ 确认接任（产生第 1 代主库）→
用旧版本号提交观测（被 409 拒绝）→ 旧主库恢复回报（被标记过期忽略）→
重复提交同一接任请求（返回已生效结果）。

## JSON API 摘要

```
POST   /api/incidents                                  建故障事件（含水位、节点）
GET    /api/incidents / /api/incidents/{id}            列表 / 详情
POST   /api/incidents/{id}/observations                录入候选观测（expected_version）
POST   /api/incidents/{id}/archives                    登记归档 WAL 区间
POST   /api/incidents/{id}/checks                      只评估，落库每个候选的结论
POST   /api/incidents/{id}/promote                     确认接任（request_id 幂等）
POST   /api/incidents/{id}/recovery-reports            主库恢复回报（旧主库迟到会被忽略）
GET    /api/health
```

## 测试

```bash
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/python -m pytest -q
```

默认用内存外的临时 SQLite 库跑全部测试（PG 行锁在非 PG 后端自动跳过）。
对真实 PostgreSQL 16，只需导出 `DATABASE_URL=postgresql+psycopg://...` 后运行同一套测试。
