---
name: helloClaude
description: 触发词「海外看板」「海外业绩日报」「helloClaude」「跑一下海外数据」。输出海外业务滚动7天业绩看板（GMV / 间夜 / 订单数 的 WoW / YoY + S/A/B 重点城市明细），按"图 → Markdown 表"顺序交付（不保留 CSV）。
---

# helloClaude — 海外业绩日报看板（薄壳）

**真身目录**：`/home/q/vibe/projects/jianyangz/途家代码合集/oversea_project/`

本 SKILL.md 只是触发入口；SQL、画图脚本、每日产物都落在上述工作目录里，便于 jianyangz 直接查看和修改。

## 项目结构

```
/home/q/vibe/projects/jianyangz/途家代码合集/oversea_project/
├── sql/
│   ├── overview.sql        # 大盘 3 指标 × 3 时间窗口（本期/上期/去年同期）
│   └── key_cities.sql      # S/A/B 级城市明细
├── scripts/
│   └── plot.py             # matplotlib 画 overview.png + key_cities_gmv.png
└── output/YYYYMMDD/        # 每日产物（csv + png）
```

## 输入参数

- `end_date`（可选）：统计窗口结束日（含）。默认 `date_sub(current_date, 1)`（昨天）。
- 口径：**支付口径**（`is_paysuccess_order = 1`，时间用 `create_date`）。

## 时间窗口（以 `end_date = T` 为基准）

| 窗口 | 区间 |
|---|---|
| 本期（滚动7天） | `[T-6, T]` |
| 上期（WoW） | `[T-13, T-7]` |
| 去年同期（YoY） | `[T-6 的去年同日, T 的去年同日]`（用 `add_months(.., -12)`） |

## 执行步骤

1. **取数** · 调用 `query-sql` skill，分别跑：
   - `/home/q/vibe/projects/jianyangz/途家代码合集/oversea_project/sql/overview.sql`
   - `/home/q/vibe/projects/jianyangz/途家代码合集/oversea_project/sql/key_cities.sql`
   
   跑之前把 SQL 里的占位符 `${end_date}` 替换成实际日期（YYYY-MM-DD）。

2. **存临时 CSV**（仅作为 plot.py 的输入，不作为交付产物）· 写到 `/home/q/vibe/projects/jianyangz/途家代码合集/oversea_project/output/YYYYMMDD/`：`overview.csv`、`key_cities.csv`

3. **画图** · 执行：
   ```bash
   python /home/q/vibe/projects/jianyangz/途家代码合集/oversea_project/scripts/plot.py \
     /home/q/vibe/projects/jianyangz/途家代码合集/oversea_project/output/YYYYMMDD
   ```
   产出 `overview.png` 和 `key_cities_gmv.png`。

4. **清理** · 画图成功后删除两个 `.csv`，只保留 PNG：
   ```bash
   rm -f /home/q/vibe/projects/jianyangz/途家代码合集/oversea_project/output/YYYYMMDD/*.csv
   ```

5. **输出** · 在对话里按顺序贴：
   1. 两张 PNG（用 `![](...)` 方便 IDE 预览）
   2. 两张 Markdown 摘要表（大盘 + 重点城市）

## Markdown 表模板

**大盘（滚动7天，支付口径）**
| 指标 | 本期 | 上期 | WoW | 去年同期 | YoY |
|---|---|---|---|---|---|
| GMV（元） | … | … | …% | … | …% |
| 间夜 | … | … | …% | … | …% |
| 订单数 | … | … | …% | … | …% |

**重点城市（S/A/B）**
| 城市 | 等级 | GMV 本期 | WoW | YoY | 间夜本期 | 订单本期 |

## 规则

- GMV 按「元」保留整数 + 千分位；百分比保留 1 位小数，带正负号。
- WoW / YoY 绝对值 > 30% 的行末尾加 ⚠️。
- 如果 `dws.dws_order` 的 `T` 分区还没就绪，直接提示"数据未就绪，建议 end_date 往前推 1 天"，不要出残缺看板。
- 同一天重复跑会覆盖 `output/YYYYMMDD/`。

## 每天自动跑

Skill 不负责定时。配合：

```
/schedule 每天 10:00 跑一下海外业绩日报
```
