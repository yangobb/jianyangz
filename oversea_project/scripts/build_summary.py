#!/usr/bin/env python3
"""生成 summary.md（大盘 + 重点城市）
用法: build_summary.py <out_dir> <end_date>
"""
import sys, csv
from pathlib import Path
from datetime import date, timedelta

out_dir, end_date_s = sys.argv[1], sys.argv[2]
out_dir = Path(out_dir)
T = date.fromisoformat(end_date_s)
cur_s, cur_e = T - timedelta(days=6), T
pre_s, pre_e = T - timedelta(days=13), T - timedelta(days=7)
yoy_s = cur_s.replace(year=cur_s.year - 1)
yoy_e = cur_e.replace(year=cur_e.year - 1)

def pct(a, b): return None if not b else (a - b) / b * 100

def fmt_pct(p):
    if p is None: return "—"
    s = f"{p:+.1f}%"
    return s + (" ⚠️" if abs(p) > 30 else "")

ov = {}
for row in csv.DictReader((out_dir / "overview.csv").open()):
    ov[row["period"]] = (float(row["gmv"]), int(row["room_night"]), int(row["order_cnt"]))

cities = {}
for row in csv.DictReader((out_dir / "key_cities.csv").open()):
    k = (row["city_name"], row["city_level"])
    cities.setdefault(k, {})[row["period"]] = (float(row["gmv"]), int(row["room_night"]), int(row["order_cnt"]))

lines = []
lines.append(f"# 海外业绩日报 · 滚动 7 天（支付口径）\n")
lines.append(f"**窗口**：本期 `{cur_s} ~ {cur_e}` · 上期 `{pre_s} ~ {pre_e}` · 去年同期 `{yoy_s} ~ {yoy_e}`\n")
lines.append("## 图表\n")
lines.append("![大盘](overview.png)\n")
lines.append("![重点城市 GMV](key_cities_gmv.png)\n")

lines.append("## 大盘\n")
lines.append("| 指标 | 本期 | 上期 | WoW | 去年同期 | YoY |")
lines.append("|---|---|---|---|---|---|")
names = [("GMV（元）", 0, "{:,.0f}"), ("间夜", 1, "{:,}"), ("订单数", 2, "{:,}")]
cur, pre, yoy = ov.get("本期", (0, 0, 0)), ov.get("上期", (0, 0, 0)), ov.get("去年同期", (0, 0, 0))
for name, idx, f in names:
    lines.append(f"| {name} | {f.format(cur[idx])} | {f.format(pre[idx])} | {fmt_pct(pct(cur[idx], pre[idx]))} | {f.format(yoy[idx])} | {fmt_pct(pct(cur[idx], yoy[idx]))} |")

lines.append("\n## 重点城市（S/A/B，按等级 + 本期 GMV 降序）\n")
lines.append("| 城市 | 等级 | GMV本期 | WoW | YoY | 间夜本期 | 订单本期 |")
lines.append("|---|---|---|---|---|---|---|")
rows = []
for (c, lv), per in cities.items():
    cc = per.get("本期", (0, 0, 0)); pp = per.get("上期", (0, 0, 0)); yy = per.get("去年同期", (0, 0, 0))
    rows.append((lv, c, cc, pct(cc[0], pp[0]), pct(cc[0], yy[0])))
rows.sort(key=lambda x: (x[0], -x[2][0]))
for lv, c, cc, w, y in rows:
    lines.append(f"| {c} | {lv} | {cc[0]:,.0f} | {fmt_pct(w)} | {fmt_pct(y)} | {cc[1]:,} | {cc[2]:,} |")

(out_dir / "summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
print(f"OK: {out_dir}/summary.md")
