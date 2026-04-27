#!/usr/bin/env python3
"""
海外业绩看板 · 画图脚本
用法:
    python plot.py <out_dir>
输入: <out_dir>/overview.csv, <out_dir>/key_cities.csv
输出: <out_dir>/overview.png, <out_dir>/key_cities_gmv.png
"""
import sys
from pathlib import Path
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager

def setup_cn_font():
    for name in ["WenQuanYi Zen Hei", "Noto Sans CJK SC", "Noto Sans CJK JP", "SimHei", "Microsoft YaHei"]:
        try:
            font_manager.findfont(name, fallback_to_default=False)
            plt.rcParams["font.sans-serif"] = [name]
            plt.rcParams["axes.unicode_minus"] = False
            return True
        except Exception:
            continue
    print("[WARN] 未找到中文字体，图中中文可能显示为方块", file=sys.stderr)
    return False

def pct(cur, ref):
    if ref in (0, None) or pd.isna(ref):
        return None
    return (cur - ref) / ref * 100.0

def plot_overview(df, out_path):
    pivot = df.set_index("period")
    metrics = [("gmv", "GMV（元）"), ("room_night", "间夜"), ("order_cnt", "订单数")]
    fig, axes = plt.subplots(1, 3, figsize=(15, 5))
    for ax, (col, title) in zip(axes, metrics):
        cur = pivot.loc["本期", col] if "本期" in pivot.index else 0
        pre = pivot.loc["上期", col] if "上期" in pivot.index else 0
        yoy = pivot.loc["去年同期", col] if "去年同期" in pivot.index else 0
        vals = [cur, pre, yoy]
        labels = ["本期", "上期", "去年同期"]
        bars = ax.bar(labels, vals, color=["#2E86AB", "#A3C4BC", "#E8A87C"])
        ax.set_title(title)
        for b, v in zip(bars, vals):
            ax.text(b.get_x() + b.get_width() / 2, v, f"{v:,.0f}", ha="center", va="bottom", fontsize=9)
        wow = pct(cur, pre)
        yoy_p = pct(cur, yoy)
        note = []
        if wow is not None: note.append(f"WoW {wow:+.1f}%")
        if yoy_p is not None: note.append(f"YoY {yoy_p:+.1f}%")
        ax.set_xlabel("  |  ".join(note), fontsize=10)
    fig.suptitle("海外业绩大盘 · 滚动 7 天", fontsize=14, fontweight="bold")
    plt.tight_layout()
    plt.savefig(out_path, dpi=120, bbox_inches="tight")
    plt.close()

def plot_key_cities(df, out_path):
    wide = df.pivot_table(index=["city_name", "city_level"], columns="period", values="gmv", aggfunc="sum").reset_index()
    for col in ["本期", "上期", "去年同期"]:
        if col not in wide.columns:
            wide[col] = 0
    wide["wow"] = wide.apply(lambda r: pct(r["本期"], r["上期"]), axis=1)
    wide["yoy"] = wide.apply(lambda r: pct(r["本期"], r["去年同期"]), axis=1)
    wide = wide.sort_values(["city_level", "本期"], ascending=[True, False])

    fig, ax = plt.subplots(figsize=(11, max(4, 0.35 * len(wide))))
    colors = {"S": "#C73E1D", "A": "#E8A87C", "B": "#85C7DE"}
    bar_colors = [colors.get(lv, "#888") for lv in wide["city_level"]]
    y = range(len(wide))
    ax.barh(list(y), wide["本期"], color=bar_colors)
    ax.set_yticks(list(y))
    ax.set_yticklabels([f"{c} [{lv}]" for c, lv in zip(wide["city_name"], wide["city_level"])])
    ax.invert_yaxis()
    for i, (v, w, yv) in enumerate(zip(wide["本期"], wide["wow"], wide["yoy"])):
        parts = [f"{v:,.0f}"]
        if w is not None: parts.append(f"WoW{w:+.1f}%")
        if yv is not None: parts.append(f"YoY{yv:+.1f}%")
        ax.text(v, i, "  " + "  ".join(parts), va="center", fontsize=9)
    ax.set_title("S/A/B 重点城市 · 本期 GMV 与 WoW/YoY")
    ax.set_xlabel("GMV（元）")
    plt.tight_layout()
    plt.savefig(out_path, dpi=120, bbox_inches="tight")
    plt.close()

def main():
    if len(sys.argv) != 2:
        print("usage: plot.py <out_dir>", file=sys.stderr)
        sys.exit(1)
    out_dir = Path(sys.argv[1])
    setup_cn_font()

    overview = pd.read_csv(out_dir / "overview.csv")
    plot_overview(overview, out_dir / "overview.png")

    cities = pd.read_csv(out_dir / "key_cities.csv")
    plot_key_cities(cities, out_dir / "key_cities_gmv.png")

    print(f"OK: wrote {out_dir}/overview.png and {out_dir}/key_cities_gmv.png")

if __name__ == "__main__":
    main()
