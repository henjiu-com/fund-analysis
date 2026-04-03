# fund-analysis · 富国基金竞争力分析仪表盘

> 富国基金 vs 14 家竞品的权益基金竞争力静态分析看板。  
> 运行一条 Python 命令拉取数据，然后在浏览器里打开 HTML 即可。

---

## 目录

- [项目概述](#项目概述)
- [快速开始](#快速开始)
- [项目结构](#项目结构)
- [数据架构](#数据架构)
- [开发日志](#开发日志)

---

## 项目概述

### 背景

分析富国基金主动权益产品相对 14 家头部竞品的整体竞争力，包括规模、收益率排名、回撤等维度。覆盖机构：

| 机构 | 本次收录 |
|------|---------|
| 富国基金 | 10 只 |
| 易方达基金 | 14 只 |
| 广发基金 | 13 只 |
| 鹏华基金 | 13 只 |
| 华夏基金 | 12 只 |
| 大成基金 | 11 只 |
| 嘉实基金 | 10 只 |
| 中欧基金 | 10 只 |
| 南方基金 | 10 只 |
| 永赢基金 | 10 只 |
| 景顺长城基金 | 9 只 |
| 工银瑞信基金 | 7 只 |
| 汇添富基金 | 7 只 |
| 华安基金 | 6 只 |
| 兴证全球基金 | 5 只 |
| **合计** | **147 只** |

### 技术方案：纯静态文件

```
fetch_data.py  ──(调用 ths-cli)──►  data.js
                                     classification.js
                                     cls_history.js
                                     nav_curves.js
                                          │
                                     index.html 读取并渲染
```

- **无服务器**：数据是静态 JS 文件，直接 `open index.html` 即可
- **刷新数据**：重新执行 `python3 fetch_data.py` 即可覆盖写入
- **数据源**：同花顺 iFinD SDK，通过本机安装的 `ths-cli` 命令行工具访问

---

## 快速开始

### 前置条件

- Python 3.10+
- `ths-cli` 已安装并完成认证（路径：`~/.local/bin/ths-cli`）
- THS iFinD 账号有效（见账号配置）

### 拉取最新数据

```bash
# 默认取最近工作日
python3 fetch_data.py

# 指定报告期
python3 fetch_data.py --date 2026-03-31
```

脚本运行约 2–3 分钟，完成后输出类似：

```
[fetch_data] 报告期: 2026-04-02
[fetch_data] 阶段1: 基础指标查询（7 批次）
  批次 1/7: 查询 50 只... 有效 20 只
  ...
[fetch_data] 阶段2: 周期收益率查询（5 个周期）
  return_1w (2026-03-26 → 2026-04-02): 3 批次... 填入 141 条
  ...
[fetch_data] 完成！共 147 只基金，10 个分类。
```

### 查看仪表盘

```bash
open index.html   # macOS
# 或直接用浏览器打开文件
```

---

## 项目结构

```
fund-analysis/
├── index.html          # 前端仪表盘（单文件，含所有 CSS/JS）
├── fetch_data.py       # 数据拉取脚本
├── data.js             # 生成：147 只基金基础信息 + 收益率
├── classification.js   # 生成：三级分类排名数据
├── cls_history.js      # 生成：历史快照（暂留空）
└── nav_curves.js       # 生成：净值曲线（暂留空）
```

### index.html 视图结构

- **View A（首屏）**：结论摘要 · 各公司榜单 · 市场概览 · 富国快览
- **View B（第二屏）**：基金经理卡片
- **第三屏**：6 个图表面板 + 公司排名 + 全量数据表格

---

## 数据架构

### `window.FUNDS_DATA` (data.js)

每条记录结构：

```json
{
  "code": "161005.OF",
  "short_name": "富国天惠精选成长混合(LOF)A",
  "company": "富国基金",
  "manager": "朱少醒",
  "scale": 211.02,
  "l1": "混合型基金",
  "l2": "偏股混合型基金",
  "cls3": "标准偏股混合型基金",
  "return_1w": -2.33,
  "return_1m": 2.69,
  "return_3m": 1.12,
  "return_6m": 17.83,
  "return_1y": 3.41,
  "return_ytd": 1.34,
  "max_drawdown_1y": 26.51
}
```

`scale` 单位：亿元；`return_*` / `max_drawdown_1y` 单位：%

### `window.CLASSIFICATION_DATA` (classification.js)

按三级分类（cls3）分组，每组包含各周期（近1周/近1月/近3月/近6月/近1年/年初至今）的排名列表：

```json
{
  "标准偏股混合型基金": {
    "l1": "混合型基金",
    "l2": "偏股混合型基金",
    "l3": "标准偏股混合型基金",
    "periods": {
      "近1周": {
        "total": 39,
        "all_funds": [
          { "code": "161005.OF", "rank": 5, "ret": -2.33, "pct": 12.8, "total": 39 }
        ]
      }
    }
  }
}
```

`pct` = rank/total × 100，越小越好（pct ≤ 30 前三分之一，pct ≥ 70 后三分之一）。

---

## 开发日志

### 整体思路选型

原始需求是为 `index.html`（已有完整前端）补充真实数据。评估了两种方案：

- **方案A**：后端服务 + API，实时查询
- **方案B**：Python 脚本离线拉取 → 生成静态 JS 文件

选方案B：无需部署，刷新一条命令，与现有纯静态 HTML 完全兼容。

---

### 坑一：基金代码种子表错误（永赢、大成、鹏华）

THS iFinD 没有"按管理人查询全部基金"的接口，必须提供明确的基金代码。因此脚本内置了约 340 只基金的种子代码表，由 THS 返回数据后再通过管理人名称过滤。

问题是手动整理种子表时，**永赢、大成、鹏华三家的大量代码填错**（填成了其他公司的产品）：

- 永赢：原始种子中大部分是广发、新华、建信、大成的代码，永赢只命中 1 只 → 扫描 007100–007199 等代码段补充，最终 10 只
- 大成：原始只有 4 只 → 补充 011066.OF（大成高鑫，56 亿）等 7 只，最终 11 只
- 鹏华：原始只有 4 只 → 补充 008134.OF（鹏华优选价值，27.9 亿）等 9 只，最终 13 只

**修复**：先查询一小批代码，核对返回的 `ths_fs_short_name_fund`（管理人简称），发现对不上则扫码补充。脚本中的公司过滤逻辑本身是可靠的，错的只是种子表。

---

### 坑二：回撤指标选错（`ths_max_retrace_rate_fund`）

最初使用 `ths_max_retrace_rate_fund`（最大回撤率），返回的值全是 ~0.31%，明显是日内振幅而非区间最大回撤，对分析无意义。

**修复**：换用 `ths_retracement_fund`（回撤相对前期高点），返回有意义的数值：
- 富国天惠：26.5%
- 易方达蓝筹：49.8%

---

### 坑三：周期收益率指标大范围返回异常值（最主要的坑）

这是整个开发过程中最隐蔽、耗时最长的问题。

**现象**：使用 `--date YYYY-MM-DD` 查询 `ths_yeild_1w_fund`、`ths_yeild_1m_fund` 等周期收益率指标，返回完全不合理的数据：

| 日期 | 指标 | 返回值 |
|------|------|--------|
| 2026-04-02 | return_1w（富国天惠） | **+146.67%** |
| 2025-09-30 | return_1w（富国天惠） | **+206%** |
| 2025-06-30 | return_1w（富国天惠） | **+270%** |
| 2023-12-29 | return_1w（易方达蓝筹） | **+741%** |
| 2024-01-31 | return_1w（富国天惠） | **-81%** |

问题跨多个日期、多只基金稳定复现，不是偶发异常。

**排查过程**：

1. 怀疑是参数格式问题（report_date 类型 vs none 类型）→ 尝试 `--params "YYYY-MM-DD,100"` → 无效
2. 怀疑是特定基金问题 → 验证多只基金均异常 → 排除
3. 怀疑是日期范围问题 → 尝试多个不同日期 → 依然异常
4. 尝试 `--params "start_date,end_date"` 格式（模仿区间计算语义）→ **返回正常值 -3.41%**

**根本原因**：`ths_yeild_*_fund` 系列指标需要显式传入起止日期（`--params start,end`），用 `--date` 时 THS 对 paramOption 的处理产生了错误的基期映射，导致收益率计算错误。

**修复方案**：将查询分为两阶段：

```python
# 阶段1：基础信息 + ytd + 回撤，使用 --date
query_batch(codes, report_date)   # BASE_INDICATORS

# 阶段2：各周期收益率，使用 --params start,end
query_batch_yield(codes, "ths_yeild_1w_fund",  date-7d,   report_date)
query_batch_yield(codes, "ths_yeild_1m_fund",  date-30d,  report_date)
query_batch_yield(codes, "ths_yeild_3m_fund",  date-90d,  report_date)
query_batch_yield(codes, "ths_yeild_6m_fund",  date-180d, report_date)
query_batch_yield(codes, "ths_yeild_1y_fund",  date-365d, report_date)
```

**例外**：`ths_yeild_ytd_fund`（年初至今）反而用 `--date` 才返回正确值，用 `--params "2025-12-31,end"` 时返回异常，因此保留在阶段1。

---

### 补充说明：THS 基金代码体系

- 场外基金（申购赎回）：`.OF` 后缀，如 `161005.OF`
- 上交所 ETF：`.SH` 后缀，如 `510050.SH`
- 深交所 ETF：`.SZ` 后缀，如 `159915.SZ`

查询基金信息类指标必须使用 `fund` type 代码（即 `.OF`/`.SH`/`.SZ`），管理人简称通过 `ths_fs_short_name_fund` 获取（不是 stk 类型的公司名接口）。

---

### 后续可扩展方向

- **NAV 曲线**：补充时序净值数据（`nav_curves.js`），支持走势图对比
- **历史分类快照**（`cls_history.js`）：保存多期排名数据，支持分位数变化趋势检测
- **扩大覆盖面**：兴证全球（5 只）、华安（6 只）等仍有提升空间，可继续扫描代码段补充
- **定期刷新**：配合 cron 自动运行 `fetch_data.py`，保持数据时效
