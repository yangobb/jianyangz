#!/usr/bin/env bash
# 海外业绩日报 · 每日定时脚本
# 调用方式：直接跑 = 取昨天数据；带参数 = ./run_daily.sh 2026-04-23
set -euo pipefail

PROJECT_DIR="/home/q/vibe/projects/jianyangz/途家代码合集/oversea_project"
END_DATE="${1:-$(date -d 'yesterday' +%Y-%m-%d)}"
OUT_DIR="${PROJECT_DIR}/output/$(echo "$END_DATE" | tr -d '-')"
LOG_FILE="${PROJECT_DIR}/output/cron.log"

mkdir -p "$OUT_DIR"
exec >>"$LOG_FILE" 2>&1
echo "===== $(date '+%F %T') end_date=${END_DATE} ====="

SPARK_BIN=/home/q/spark/spark-3.5.0-bin-hadoop3/bin/spark-sql
PAIMON_JAR=/home/q/spark/spark-3.5.0-bin-hadoop3/jars/paimon-spark-3.5-0.7.0-incubating.jar

run_sql () {
    local in_sql="$1" out_tsv="$2"
    sed "s/\${end_date}/${END_DATE}/g" "$in_sql" > "${OUT_DIR}/_$(basename "$in_sql")"
    SPARK_LOCAL_DIRS=/home/q/tmp/spill \
    JAVA_TOOL_OPTIONS="-Djava.io.tmpdir=/home/q/tmp/spill" \
        "$SPARK_BIN" \
        --driver-memory 8g \
        --conf spark.sql.shuffle.partitions=64 \
        --jars "$PAIMON_JAR" \
        -f "${OUT_DIR}/_$(basename "$in_sql")" \
        > "$out_tsv"
}

echo "[1/4] overview SQL"
run_sql "${PROJECT_DIR}/sql/overview.sql"   "${OUT_DIR}/overview.tsv"
echo "[2/4] key_cities SQL"
run_sql "${PROJECT_DIR}/sql/key_cities.sql" "${OUT_DIR}/key_cities.tsv"

echo "[3/4] TSV -> CSV"
{ echo "period,gmv,room_night,order_cnt";                 awk -F'\t' 'BEGIN{OFS=","}{print $1,$2,$3,$4}'       "${OUT_DIR}/overview.tsv";   } > "${OUT_DIR}/overview.csv"
{ echo "city_name,city_level,period,gmv,room_night,order_cnt"; awk -F'\t' 'BEGIN{OFS=","}{print $1,$2,$3,$4,$5,$6}' "${OUT_DIR}/key_cities.tsv"; } > "${OUT_DIR}/key_cities.csv"

echo "[4/4] plot + summary"
python3 "${PROJECT_DIR}/scripts/plot.py" "$OUT_DIR"
python3 "${PROJECT_DIR}/scripts/build_summary.py" "$OUT_DIR" "$END_DATE"

# 清理中间文件，只保留 PNG + summary.md
rm -f "${OUT_DIR}"/_*.sql "${OUT_DIR}"/*.tsv "${OUT_DIR}"/*.csv

echo "DONE. Artifacts: $(ls "$OUT_DIR")"
