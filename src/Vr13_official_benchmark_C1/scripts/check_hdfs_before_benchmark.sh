#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# HDFS preflight check before VideoGraphDB benchmark
#
# Purpose:
#   Detect NameNode safe mode / low-resource / HDFS capacity problems
#   before launching Spark benchmark jobs.
#
# Usage:
#   chmod +x check_hdfs_before_benchmark.sh
#   ./check_hdfs_before_benchmark.sh
# ============================================================

echo "============================================================"
echo "HDFS / NameNode preflight check"
echo "Started at: $(date '+%Y-%m-%d %H:%M:%S')"
echo "============================================================"

echo
echo "[1] Safe mode status"
hdfs dfsadmin -safemode get || true

echo
echo "[2] HDFS report"
hdfs dfsadmin -report || true

echo
echo "[3] HDFS root capacity"
hdfs dfs -df -h / || true

echo
echo "[4] Largest first-level HDFS directories"
hdfs dfs -du -s -h /* 2>/dev/null | sort -hr || true

echo
echo "[5] /spark-logs status"
hdfs dfs -ls /spark-logs 2>/dev/null || echo "/spark-logs does not exist or cannot be listed."

echo
echo "[6] NameNode/master local disk"
df -h || true

echo
echo "[7] NameNode/master inode usage"
df -ih || true

echo
echo "============================================================"
echo "Interpretation:"
echo "  - If safe mode is ON because resources are low, first free local"
echo "    NameNode disk/inodes, then run: hdfs dfsadmin -safemode leave"
echo "  - If /spark-logs is huge and safe mode is OFF, consider:"
echo "      hdfs dfs -rm -r -skipTrash /spark-logs/*"
echo "  - If /spark-logs does not exist and safe mode is OFF, create it:"
echo "      hdfs dfs -mkdir -p /spark-logs"
echo "      hdfs dfs -chmod 1777 /spark-logs"
echo "============================================================"

