# VideoGraphDB C2 — Profiling Benchmark Run Guide

## 0. Mục tiêu

File này là **hướng dẫn chạy độc lập** cho project:

```bash
/home/hduser/VideoGraphDB_Project/src/Vr13_profile_benchmark_C2
```

Project này chỉ dùng cho **C2 — Profiling benchmark / materialized phase-boundary profiling**.

Mục tiêu của C2 là phân tích chi phí từng phase của hai chiến lược thực thi:

- **Baseline**: Global Graph Build / Merge / Union / NEXT_TW preparation → Global Query + Hard Validation.
- **Proposed**: Global Lookup → LocalEval + Boundary Fragment Preparation → Stitching / GlobalEval + Hard Validation.

C2 **không thay thế** C1. Trong bài báo/luận án:

- **C1 — Official benchmark**: dùng để báo cáo runtime chính thức, speedup chính thức.
- **C2 — Profiling benchmark**: dùng để giải thích phase cost, overhead, cardinality, candidate/witness/fragment behavior.

Không dùng kết quả C2 làm official runtime vì profiling có overhead do Spark action như `count()`, `persist()`, materialization và phase-boundary forcing.

---

## 1. Phân biệt C1 và C2

| Nội dung | C1 — Official benchmark | C2 — Profiling benchmark |
|---|---|---|
| Thư mục | `Vr13_official_benchmark_C1` | `Vr13_profile_benchmark_C2` |
| Mục tiêu | Đo runtime chính thức | Phân rã chi phí từng phase |
| Spark execution | Lazy / end-to-end | Materialized phase boundaries |
| Mode runner | `--mode official` | `--mode profiling` |
| Output | `benchmark_outputs_official/` | `benchmark_outputs_profile/` |
| Dùng cho paper chính | Có | Không, chỉ dùng phân tích bổ sung |
| Ghi full witness mặc định | Không trong timing/profiling | Không |
| Correctness | Chạy riêng trong C1 | Không chạy mặc định trong C2 |

---

## 2. Script chuẩn cần dùng trong C2

Trong C2, chỉ dùng các script sau:

```text
scripts/
├── check_hdfs_before_benchmark.sh
├── run_one_query_profile.sh
└── run_multi_queries_profile.sh
```

| Script | Mục đích |
|---|---|
| `check_hdfs_before_benchmark.sh` | Kiểm tra HDFS, NameNode, Spark logs, dung lượng trước khi chạy |
| `run_one_query_profile.sh` | Chạy profiling cho một query |
| `run_multi_queries_profile.sh` | Chạy profiling cho nhiều query, query lỗi vẫn chạy tiếp query sau |

Không dùng các script C1 trong C2:

```text
run_one_query_official.sh
run_multi_queries_official.sh
run_correctness_official.sh
```

Không dùng các script cũ:

```text
run_multi_queries_full.sh
run_one_query_full.sh
run_one_query_timing.sh
run_multi_queries_timing.sh
run_multi_queries_profiling.sh
```

Nếu còn file `run_multi_queries_profiling.sh`, nên đổi tên chuẩn thành:

```bash
mv scripts/run_multi_queries_profiling.sh scripts/run_multi_queries_profile.sh
```

---

## 3. Vào đúng thư mục project C2

```bash
cd /home/hduser/VideoGraphDB_Project/src/Vr13_profile_benchmark_C2
```

Kiểm tra đang ở đúng thư mục:

```bash
pwd
```

Kết quả đúng phải là:

```text
/home/hduser/VideoGraphDB_Project/src/Vr13_profile_benchmark_C2
```

---

## 4. Cấu trúc thư mục C2 khuyến nghị

```text
Vr13_profile_benchmark_C2/
├── benchmark_outputs_profile/
├── query_specs/
├── scripts/
│   ├── check_hdfs_before_benchmark.sh
│   ├── run_one_query_profile.sh
│   └── run_multi_queries_profile.sh
├── baseline.py
├── proposed_distributed.py
├── config.py
├── graph_io.py
├── queries.py
├── run_experiment.py
└── README_C2_PROFILE_RUN_GUIDE.md
```

Trong C2, `run_experiment.py` là runner profiling của project C2. Tên file giống C1 nhưng nằm trong thư mục khác, vì vậy không bị nhầm nếu luôn `cd` đúng thư mục trước khi chạy.

---

## 5. Kiểm tra quyền chạy và compile

Cấp quyền chạy script:

```bash
cd /home/hduser/VideoGraphDB_Project/src/Vr13_profile_benchmark_C2

chmod +x scripts/check_hdfs_before_benchmark.sh
chmod +x scripts/run_one_query_profile.sh
chmod +x scripts/run_multi_queries_profile.sh
```

Kiểm tra Python compile:

```bash
python3 -m py_compile \
  run_experiment.py \
  baseline.py \
  proposed_distributed.py \
  queries.py \
  graph_io.py \
  config.py \
  query_specs/*.py
```

Kiểm tra cú pháp shell script:

```bash
bash -n scripts/check_hdfs_before_benchmark.sh
bash -n scripts/run_one_query_profile.sh
bash -n scripts/run_multi_queries_profile.sh
```

Kiểm tra script profile có gọi đúng mode profiling:

```bash
grep -RInE '^[[:space:]]*--mode profiling|python3 run_experiment.py' scripts/run_*profile.sh
```

Kết quả đúng phải thể hiện các script gọi:

```text
python3 run_experiment.py
--mode profiling
```

Kiểm tra không chạy nhầm official trong script profile:

```bash
grep -RInE '^[[:space:]]*[^#].*(--mode official|OFFICIAL_BASE|OFFICIAL_REPEATS|benchmark_outputs_official)' scripts/run_*profile.sh
```

Kết quả đúng: **không in ra dòng nào**.

---

## 6. Kiểm tra query registry

```bash
cd /home/hduser/VideoGraphDB_Project/src/Vr13_profile_benchmark_C2

python3 - <<'PY'
from queries import QUERY_REGISTRY
print("Supported queries:")
for q in sorted(QUERY_REGISTRY.keys()):
    spec = QUERY_REGISTRY[q]
    print(f"{q:5s} | {spec.query_group} | {spec.query_name}")
PY
```

Kỳ vọng có đủ 12 query workload:

```text
Q1.1 Q1.2 Q1.3
Q2.1 Q2.2 Q2.3
Q3.1 Q3.2 Q3.3
Q4.1 Q4.2 Q4.3
```

Có thể có thêm alias:

```text
Q14
```

nếu `Q14` được ánh xạ vào `Q4.3`.

---

## 7. Kiểm tra HDFS và Spark

```bash
cd /home/hduser/VideoGraphDB_Project/src/Vr13_profile_benchmark_C2

./scripts/check_hdfs_before_benchmark.sh
```

Cần đảm bảo:

```text
Safe mode is OFF
Live datanodes >= 2
/spark-logs tồn tại và ghi được
HDFS còn đủ dung lượng
Ổ local master còn đủ dung lượng
```

Kiểm tra dataset mẫu:

```bash
hdfs dfs -test -d hdfs:///data/videographdb/dataset_1m/by_partition && echo "OK: dataset_1m/by_partition exists"
```

Kiểm tra Spark/HDFS process:

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

## 8. Chạy thử profiling một query nhỏ

Nên chạy `Q1.1` với `dataset_1m` trước:

```bash
cd /home/hduser/VideoGraphDB_Project/src/Vr13_profile_benchmark_C2

DATASETS=dataset_1m \
PROFILE_REPEATS=1 \
MAX_NEXTTW_HOPS=120 \
EPSILON_TIME_SECONDS=120 \
QUICK_EXIT_SECONDS=120 \
./scripts/run_one_query_profile.sh Q1.1
```

Kiểm tra status mới nhất:

```bash
STATUS_FILE=$(ls -t benchmark_outputs_profile/status/run_one_query_profile_status_*.csv | head -1)
echo "$STATUS_FILE"
cat "$STATUS_FILE"
```

Nếu status là `PASS`, kiểm tra output:

```bash
find benchmark_outputs_profile/profiling -name "profile_materialized_phase_summary_*.csv" | sort
find benchmark_outputs_profile/profiling -name "profile_status_*.csv" | sort
find benchmark_outputs_profile/profiling -name "profile_aggregate_repeats_*.csv" | sort
```

Kiểm tra dung lượng output:

```bash
du -sh benchmark_outputs_profile
du -sh benchmark_outputs_profile/logs
```

---

## 9. Chạy profiling một nhóm query nhỏ

Trước khi chạy toàn bộ 12 query, nên chạy một nhóm nhỏ để kiểm tra:

```bash
cd /home/hduser/VideoGraphDB_Project/src/Vr13_profile_benchmark_C2

DATASETS=dataset_1m \
PROFILE_REPEATS=1 \
QUERIES_CSV="Q1.1,Q1.2,Q4.3" \
MAX_NEXTTW_HOPS=120 \
EPSILON_TIME_SECONDS=120 \
QUICK_EXIT_SECONDS=120 \
./scripts/run_multi_queries_profile.sh
```

Kiểm tra status mới nhất:

```bash
STATUS_FILE=$(ls -t benchmark_outputs_profile/status/run_multi_queries_profile_status_*.csv | head -1)
echo "$STATUS_FILE"
cat "$STATUS_FILE"
```

Nếu có query `FAIL`, script vẫn tiếp tục query sau. Cần mở log riêng của query bị lỗi trong:

```text
benchmark_outputs_profile/logs/
```

---

## 10. Chạy profiling toàn bộ workload trên dataset_1m

```bash
cd /home/hduser/VideoGraphDB_Project/src/Vr13_profile_benchmark_C2

DATASETS=dataset_1m \
PROFILE_REPEATS=1 \
MAX_NEXTTW_HOPS=120 \
EPSILON_TIME_SECONDS=120 \
QUICK_EXIT_SECONDS=120 \
./scripts/run_multi_queries_profile.sh
```

Theo dõi log:

```bash
tail -f benchmark_outputs_profile/logs/run_multi_queries_profile_*.log
```

Kiểm tra tiến trình:

```bash
ps -ef | grep -E "run_multi_queries_profile|run_experiment|spark-submit|baseline.py|proposed_distributed.py" | grep -v grep
```

---

## 11. Chạy profiling cho nhiều dataset

Sau khi `dataset_1m` đã ổn, mở rộng dần:

```bash
cd /home/hduser/VideoGraphDB_Project/src/Vr13_profile_benchmark_C2

DATASETS=dataset_1m,dataset_2m \
PROFILE_REPEATS=1 \
MAX_NEXTTW_HOPS=120 \
EPSILON_TIME_SECONDS=120 \
QUICK_EXIT_SECONDS=120 \
./scripts/run_multi_queries_profile.sh
```

Sau đó mới cân nhắc chạy đầy đủ:

```bash
cd /home/hduser/VideoGraphDB_Project/src/Vr13_profile_benchmark_C2

DATASETS=dataset_1m,dataset_2m,dataset_4m,dataset_6m,dataset_8m,dataset_10m \
PROFILE_REPEATS=1 \
MAX_NEXTTW_HOPS=120 \
EPSILON_TIME_SECONDS=120 \
QUICK_EXIT_SECONDS=120 \
./scripts/run_multi_queries_profile.sh
```

Với profiling, **khuyến nghị `PROFILE_REPEATS=1`** trước. Profiling vốn có overhead do materialization/count nên không nên chạy nhiều repeat trên dataset lớn ngay từ đầu.

---

## 12. Chạy nền bằng nohup

Chạy toàn bộ workload trên `dataset_1m`:

```bash
cd /home/hduser/VideoGraphDB_Project/src/Vr13_profile_benchmark_C2

nohup bash -c '
DATASETS=dataset_1m PROFILE_REPEATS=1 MAX_NEXTTW_HOPS=120 EPSILON_TIME_SECONDS=120 QUICK_EXIT_SECONDS=120 ./scripts/run_multi_queries_profile.sh
' > benchmark_outputs_profile/run_all_profile_dataset_1m_nohup.log 2>&1 &
```

Theo dõi log:

```bash
tail -f benchmark_outputs_profile/run_all_profile_dataset_1m_nohup.log
```

Theo dõi dung lượng:

```bash
watch -n 10 'df -h .; echo; hdfs dfs -df -h /; echo; du -sh benchmark_outputs_profile 2>/dev/null'
```

---

## 13. Kiểm tra kết quả sau khi chạy xong

Xem status mới nhất:

```bash
STATUS_FILE=$(ls -t benchmark_outputs_profile/status/run_multi_queries_profile_status_*.csv | head -1)
echo "$STATUS_FILE"
cat "$STATUS_FILE"
```

Nếu tất cả query là `PASS`, kết quả profiling nằm trong:

```text
benchmark_outputs_profile/profiling/<RUN_ID>/
```

Liệt kê profiling summary:

```bash
find benchmark_outputs_profile/profiling -name "profile_materialized_phase_summary_*.csv" | sort
```

Liệt kê aggregate:

```bash
find benchmark_outputs_profile/profiling -name "profile_aggregate_repeats_*.csv" | sort
```

Liệt kê status theo query:

```bash
find benchmark_outputs_profile/profiling -name "profile_status_*.csv" | sort
```

Liệt kê log:

```bash
ls -lh benchmark_outputs_profile/logs/
```

---

## 14. File kết quả dùng cho phân tích C2

### 14.1. File phase profiling chính

Dùng:

```text
profile_materialized_phase_summary_<QUERY>.csv
```

Các cột quan trọng:

```text
Dataset
Query
Repeat
Timing_mode
Phase_materialized

Baseline_Profile_Step1_GlobalGraphBuild_materialized_s
Baseline_Profile_Step2_GlobalQueryValidation_materialized_s
Baseline_Profile_Algorithm_materialized_s
Baseline_Profile_phase_sum_s
Baseline_Profile_algorithm_minus_phase_sum_s

Proposed_Profile_GlobalLookup_materialized_s
Proposed_Profile_LocalEvalBoundary_materialized_s
Proposed_Profile_StitchingValidation_materialized_s
Proposed_Profile_Step1_FragmentPreparation_materialized_s
Proposed_Profile_Step2_StitchingValidation_materialized_s
Proposed_Profile_Algorithm_materialized_s
Proposed_Profile_phase_sum_s
Proposed_Profile_algorithm_minus_phase_sum_s
```

### 14.2. Cardinality / diagnosis

Các cột cần dùng để giải thích node/edge/candidate/fragment behavior:

```text
Num_global_vertices
Num_base_global_edges
Baseline_num_candidates
Proposed_num_candidates
Baseline_num_valid_witnesses
Proposed_num_valid_witnesses
Proposed_num_local_fragments
Proposed_num_boundary_fragments
Proposed_num_localeval_fragments_total
```

### 14.3. Speedup profiling

Các cột này chỉ dùng tham khảo trong profiling:

```text
Profile_speedup_wall
Profile_time_reduction_wall_pct
```

Không đưa các cột này vào bảng runtime chính của paper nếu không ghi rõ là **profiling-only / materialized phase-boundary measurement**.

### 14.4. Aggregate across repeats

Dùng:

```text
profile_aggregate_repeats_<QUERY>.csv
```

Nếu `PROFILE_REPEATS=1`, các giá trị `mean`, `median`, `min`, `max` sẽ gần như trùng nhau. Khi cần kiểm tra độ ổn định, có thể tăng `PROFILE_REPEATS=3` cho một vài query đại diện, không nhất thiết cho toàn bộ workload.

---

## 15. Cách diễn giải kết quả C2 trong bài báo/luận án

Nên viết:

> We use the official lazy end-to-end benchmark for the primary runtime comparison. In addition, we run a profiling benchmark with materialized phase boundaries to attribute the cost of global graph construction, local evaluation, boundary-fragment preparation, stitching, and validation. The profiling benchmark intentionally triggers Spark actions at phase boundaries and is therefore used only for cost attribution, not as a replacement for official runtime measurement.

Trong tiếng Việt:

> Benchmark chính thức C1 được sử dụng để báo cáo runtime và speedup chính. Benchmark profiling C2 ép materialization tại ranh giới phase để phân tích chi phí từng bước. Do profiling kích hoạt thêm Spark action như `count()` và `persist()`, kết quả C2 chỉ dùng cho phân tích nguyên nhân và phân rã chi phí, không dùng thay thế runtime chính thức.

---

## 16. Nếu đã chạy C1 rồi thì có thể bỏ qua phần nào?

Nếu C1 đã chạy thành công trên cùng cluster, cùng code query, cùng dataset và chưa khởi động lại HDFS/Spark, có thể rút gọn C2 như sau.

### Có thể bỏ qua hoặc làm rất nhanh

| Phần | Có thể bỏ qua khi nào? | Ghi chú |
|---|---|---|
| Correctness toàn bộ | Nếu C1 correctness đã PASS toàn bộ query/dataset | C2 không dùng để kiểm tra correctness |
| Kiểm tra Spark process chi tiết | Nếu ngay trước đó C1 chạy ổn và cluster chưa restart | Vẫn nên chạy `jps` nhanh nếu nghi ngờ |
| Chạy profile nhiều repeat | Nếu chỉ cần phase attribution ban đầu | Dùng `PROFILE_REPEATS=1` |
| Chạy toàn bộ dataset lớn | Nếu chỉ cần minh họa phase cost | Có thể dùng `dataset_1m` hoặc vài dataset đại diện |
| Query registry check thủ công | Nếu compile và script tự check đã PASS | Script `run_one_query_profile.sh` đã có bước này |

### Không nên bỏ qua

| Phần | Lý do |
|---|---|
| `cd` đúng thư mục C2 | Tránh ghi nhầm output sang C1 |
| `python3 -m py_compile ...` sau khi copy/sửa file | Phát hiện lỗi cú pháp sớm |
| `bash -n scripts/*.sh` | Phát hiện lỗi shell trước khi chạy Spark |
| Kiểm tra HDFS safe mode ngắn | C1 chạy xong có thể làm đầy log/disk hoặc HDFS vào safe mode |
| Chạy thử `Q1.1` trên `dataset_1m` | Xác nhận C2 scripts, output path và mode profiling đúng |
| Theo dõi dung lượng `benchmark_outputs_profile` | Profiling có thể sinh log/status lớn hơn official |

### Lệnh C2 rút gọn nếu C1 đã PASS

```bash
cd /home/hduser/VideoGraphDB_Project/src/Vr13_profile_benchmark_C2

chmod +x scripts/*.sh

python3 -m py_compile \
  run_experiment.py \
  baseline.py \
  proposed_distributed.py \
  queries.py \
  graph_io.py \
  config.py \
  query_specs/*.py

bash -n scripts/*.sh

hdfs dfsadmin -safemode get
hdfs dfs -df -h /
df -h .

DATASETS=dataset_1m \
PROFILE_REPEATS=1 \
MAX_NEXTTW_HOPS=120 \
EPSILON_TIME_SECONDS=120 \
QUICK_EXIT_SECONDS=120 \
./scripts/run_one_query_profile.sh Q1.1
```

Nếu bước trên PASS, chạy nhóm nhỏ:

```bash
DATASETS=dataset_1m \
PROFILE_REPEATS=1 \
QUERIES_CSV="Q1.1,Q1.2,Q4.3" \
MAX_NEXTTW_HOPS=120 \
EPSILON_TIME_SECONDS=120 \
QUICK_EXIT_SECONDS=120 \
./scripts/run_multi_queries_profile.sh
```

---

## 17. Quy tắc quan trọng

Không chạy C1 official và C2 profiling đồng thời.

Không dùng output C2 làm official runtime.

Không trộn CSV của C1 và C2 vào cùng một bảng nếu không có cột phân biệt:

```text
Timing_mode
Phase_materialized
Benchmark_type
```

Không bật ghi full witness/candidate/intermediate mặc định trong C2.

Không đổi query logic giữa C1 và C2. C1 và C2 chỉ khác **cách đo**, không khác **thuật toán logic**.

Không chạy `PROFILE_REPEATS=3` trên toàn bộ dataset lớn ngay từ đầu. Hãy chạy `PROFILE_REPEATS=1` trước để kiểm soát thời gian và dung lượng.

---

## 18. Tóm tắt thứ tự chạy chuẩn cho C2

```bash
cd /home/hduser/VideoGraphDB_Project/src/Vr13_profile_benchmark_C2

# 1. Cấp quyền và kiểm tra compile
chmod +x scripts/*.sh

python3 -m py_compile \
  run_experiment.py \
  baseline.py \
  proposed_distributed.py \
  queries.py \
  graph_io.py \
  config.py \
  query_specs/*.py

bash -n scripts/*.sh

# 2. Kiểm tra HDFS/Spark
./scripts/check_hdfs_before_benchmark.sh

# 3. Chạy thử một query
DATASETS=dataset_1m \
PROFILE_REPEATS=1 \
MAX_NEXTTW_HOPS=120 \
EPSILON_TIME_SECONDS=120 \
QUICK_EXIT_SECONDS=120 \
./scripts/run_one_query_profile.sh Q1.1

# 4. Chạy thử nhóm query nhỏ
DATASETS=dataset_1m \
PROFILE_REPEATS=1 \
QUERIES_CSV="Q1.1,Q1.2,Q4.3" \
MAX_NEXTTW_HOPS=120 \
EPSILON_TIME_SECONDS=120 \
QUICK_EXIT_SECONDS=120 \
./scripts/run_multi_queries_profile.sh

# 5. Chạy profiling toàn bộ workload trên dataset_1m
DATASETS=dataset_1m \
PROFILE_REPEATS=1 \
MAX_NEXTTW_HOPS=120 \
EPSILON_TIME_SECONDS=120 \
QUICK_EXIT_SECONDS=120 \
./scripts/run_multi_queries_profile.sh

# 6. Khi ổn mới mở rộng dataset
DATASETS=dataset_1m,dataset_2m,dataset_4m,dataset_6m,dataset_8m,dataset_10m \
PROFILE_REPEATS=1 \
MAX_NEXTTW_HOPS=120 \
EPSILON_TIME_SECONDS=120 \
QUICK_EXIT_SECONDS=120 \
./scripts/run_multi_queries_profile.sh
```

---

## 19. Tóm tắt ý nghĩa nghiên cứu

C2 giúp trả lời các câu hỏi nghiên cứu bổ sung:

- Baseline tốn bao nhiêu chi phí ở Global Graph Build / Union / NEXT_TW preparation?
- Proposed tốn bao nhiêu chi phí ở Global Lookup?
- LocalEval + Boundary Fragment Preparation có giảm candidate space không?
- Stitching/GlobalEval có trở thành bottleneck không?
- Số boundary fragments, local fragments, candidates, valid witnesses có tăng theo dataset như thế nào?
- Phần nào gây overhead ngoài phase timer?
- Kết quả C1 nhanh/chậm là do phase nào?

C2 phù hợp đưa vào phần:

```text
Experimental Analysis
Ablation / Profiling Study
Cost Attribution
Discussion
Threats to Validity
```

Không nên đưa C2 vào bảng runtime chính nếu chưa ghi rõ đây là **materialized profiling benchmark**.

