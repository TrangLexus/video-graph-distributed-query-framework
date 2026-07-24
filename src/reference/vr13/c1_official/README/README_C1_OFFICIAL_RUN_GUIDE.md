# VideoGraphDB C1 — Official Benchmark Run Guide

## 0. Mục tiêu

File này là **hướng dẫn duy nhất** để chạy project:

```bash
/home/hduser/VideoGraphDB_Project/src/Vr13_official_benchmark_C1
```

Project này chỉ dùng cho **C1 — Official benchmark**:

- `correctness`: kiểm tra Baseline và Proposed có cùng witness set không.
- `official`: đo runtime chính thức dùng cho bài báo/luận án.
- Không dùng `profiling` trong các script C1.

Không dùng các script/tài liệu cũ như:

```text
run_multi_queries_full.sh
run_one_query_full.sh
run_one_query_timing.sh
run_multi_queries_timing.sh
```

---

## 1. Script chuẩn cần dùng

```text
scripts/
├── check_hdfs_before_benchmark.sh
├── run_correctness_official.sh
├── run_one_query_official.sh
└── run_multi_queries_official.sh
```

| Script | Mục đích |
|---|---|
| `check_hdfs_before_benchmark.sh` | Kiểm tra HDFS trước khi chạy benchmark |
| `run_correctness_official.sh` | Chạy correctness cho nhiều query |
| `run_one_query_official.sh` | Chạy official benchmark cho một query |
| `run_multi_queries_official.sh` | Chạy official benchmark cho toàn bộ query |

---

## 2. Vào đúng thư mục project

```bash
cd /home/hduser/VideoGraphDB_Project/src/Vr13_official_benchmark_C1
```

---

## 3. Kiểm tra quyền chạy và compile

```bash
chmod +x scripts/check_hdfs_before_benchmark.sh
chmod +x scripts/run_correctness_official.sh
chmod +x scripts/run_one_query_official.sh
chmod +x scripts/run_multi_queries_official.sh
```

```bash
python3 -m py_compile   run_experiment.py   baseline.py   proposed_distributed.py   queries.py   graph_io.py   config.py   query_specs/q23.py   query_specs/q31.py
```

```bash
bash -n scripts/check_hdfs_before_benchmark.sh
bash -n scripts/run_correctness_official.sh
bash -n scripts/run_one_query_official.sh
bash -n scripts/run_multi_queries_official.sh
```

Kiểm tra không chạy nhầm profiling:

```bash
grep -RInE '^[[:space:]]*[^#].*(--mode profiling|PROFILING_BASE|PROFILING_REPEATS)' scripts/run_*official.sh
```

Kết quả đúng: **không in ra dòng nào**.

Kiểm tra mode chính:

```bash
grep -RInE '^[[:space:]]*--mode ' scripts/run_*official.sh
```

Kết quả đúng:

```text
scripts/run_multi_queries_official.sh: --mode official
scripts/run_one_query_official.sh: --mode official
scripts/run_correctness_official.sh: --mode correctness
```

---

## 4. Kiểm tra HDFS và Spark

```bash
./scripts/check_hdfs_before_benchmark.sh
```

Cần đảm bảo:

```text
Safe mode is OFF
Live datanodes >= 2
/spark-logs tồn tại và ghi được
```

Kiểm tra process:

```bash
jps
ssh slave01 "jps"
ssh slave02 "jps"
```

Kỳ vọng tối thiểu:

```text
master:  NameNode, SecondaryNameNode, Master
slave01: DataNode, Worker
slave02: DataNode, Worker
```

---

## 5. Chạy thử một query nhỏ

```bash
DATASETS=dataset_1m OFFICIAL_REPEATS=1 MAX_NEXTTW_HOPS=120 EPSILON_TIME_SECONDS=120 QUICK_EXIT_SECONDS=120 ./scripts/run_one_query_official.sh Q1.1
```

Kiểm tra status mới nhất:

```bash
STATUS_FILE=$(ls -t benchmark_outputs_official/status/run_one_query_official_status_*.csv | head -1)
echo "$STATUS_FILE"
cat "$STATUS_FILE"
```

Nếu status là `PASS`, chuyển sang bước tiếp theo.

---

## 6. Chạy correctness cho toàn bộ workload

```bash
DATASETS=dataset_1m,dataset_2m,dataset_4m,dataset_6m,dataset_8m,dataset_10m MAX_NEXTTW_HOPS=120 EPSILON_TIME_SECONDS=120 QUICK_EXIT_SECONDS=120 ./scripts/run_correctness_official.sh
```

Theo dõi status:

```bash
STATUS_FILE=$(ls -t benchmark_outputs_official/status/run_correctness_official_status_*.csv | head -1)
echo "$STATUS_FILE"
cat "$STATUS_FILE"
```

Điều kiện pass:

```text
status = PASS cho tất cả query
```

Trong file comparison, điều kiện correctness mạnh là:

```text
only_baseline = 0
only_proposed = 0
outputs_match = True
```

Nếu correctness chưa pass, dừng lại và sửa lỗi trước khi chạy official benchmark toàn bộ.

---

## 7. Chạy official benchmark toàn bộ

Khuyến nghị chạy nền bằng `nohup`:

```bash
nohup bash -c '
DATASETS=dataset_1m,dataset_2m,dataset_4m,dataset_6m,dataset_8m,dataset_10m OFFICIAL_REPEATS=3 MAX_NEXTTW_HOPS=120 EPSILON_TIME_SECONDS=120 QUICK_EXIT_SECONDS=120 ./scripts/run_multi_queries_official.sh
' > benchmark_outputs_official/run_all_official_nohup.log 2>&1 &
```

Theo dõi log:

```bash
tail -f benchmark_outputs_official/run_all_official_nohup.log
```

Kiểm tra tiến trình:

```bash
ps -ef | grep -E "run_multi_queries_official|run_experiment|spark-submit|baseline.py|proposed_distributed.py" | grep -v grep
```

---

## 8. Kiểm tra kết quả sau khi chạy xong

Xem status mới nhất:

```bash
STATUS_FILE=$(ls -t benchmark_outputs_official/status/run_multi_queries_official_status_*.csv | head -1)
echo "$STATUS_FILE"
cat "$STATUS_FILE"
```

Nếu tất cả query là `PASS`, kết quả official nằm trong:

```text
benchmark_outputs_official/official/<RUN_ID>/
```

Liệt kê runtime aggregate:

```bash
find benchmark_outputs_official/official -name "runtime_aggregate_repeats_*.csv" | sort
```

Liệt kê runtime từng repeat:

```bash
find benchmark_outputs_official/official -name "runtime_algorithm_end_to_end_summary_*.csv" | sort
```

Liệt kê comparison:

```bash
find benchmark_outputs_official/official -name "baseline_vs_proposed_comparison_*.csv" | sort
```

---

## 9. File kết quả dùng cho bài báo/luận án

### Bảng correctness

Dùng:

```text
baseline_vs_proposed_comparison_<QUERY>.csv
```

Các cột cần xem:

```text
baseline_witnesses
proposed_witnesses
only_baseline
only_proposed
outputs_match
both_zero
```

### Bảng runtime chính

Dùng:

```text
runtime_aggregate_repeats_<QUERY>.csv
```

Các cột quan trọng:

```text
Baseline_query_algorithm_time_s_mean
Proposed_query_algorithm_wall_time_s_mean
Algorithm_speedup_wall_mean
Algorithm_time_reduction_wall_pct_mean
Baseline_end_to_end_time_s_mean
Proposed_end_to_end_time_s_mean
End_to_end_speedup_mean
End_to_end_time_reduction_pct_mean
```

### Kiểm tra từng repeat

Dùng:

```text
runtime_algorithm_end_to_end_summary_<QUERY>.csv
```

---

## 10. Quy tắc quan trọng

Không chạy đồng thời nhiều benchmark official.

Không vừa chạy profiling vừa chạy official.

Không dùng output có dấu hiệu sau làm official runtime:

```text
--mode profiling
PROFILING_REPEATS
PROFILING_BASE
MAX_NEXTTW_HOPS=3
```

Không trộn output cũ trước chuẩn hóa với output mới trong cùng một bảng kết quả.

---

## 11. Tóm tắt thứ tự chạy chuẩn

```bash
cd /home/hduser/VideoGraphDB_Project/src/Vr13_official_benchmark_C1

# 1. Cấp quyền và kiểm tra compile
chmod +x scripts/*.sh

python3 -m py_compile   run_experiment.py   baseline.py   proposed_distributed.py   queries.py   graph_io.py   config.py   query_specs/q23.py   query_specs/q31.py

bash -n scripts/*.sh

# 2. Kiểm tra HDFS/Spark
./scripts/check_hdfs_before_benchmark.sh

# 3. Chạy thử một query
DATASETS=dataset_1m OFFICIAL_REPEATS=1 MAX_NEXTTW_HOPS=120 EPSILON_TIME_SECONDS=120 QUICK_EXIT_SECONDS=120 ./scripts/run_one_query_official.sh Q1.1

# 4. Chạy correctness toàn bộ
DATASETS=dataset_1m,dataset_2m,dataset_4m,dataset_6m,dataset_8m,dataset_10m MAX_NEXTTW_HOPS=120 EPSILON_TIME_SECONDS=120 QUICK_EXIT_SECONDS=120 ./scripts/run_correctness_official.sh

# 5. Chạy official benchmark toàn bộ
nohup bash -c '
DATASETS=dataset_1m,dataset_2m,dataset_4m,dataset_6m,dataset_8m,dataset_10m OFFICIAL_REPEATS=3 MAX_NEXTTW_HOPS=120 EPSILON_TIME_SECONDS=120 QUICK_EXIT_SECONDS=120 ./scripts/run_multi_queries_official.sh
' > benchmark_outputs_official/run_all_official_nohup.log 2>&1 &
```

