# VideoGraphDB — Indexing & Partitioning Lightweight Evaluation Guide

## 0. Mục tiêu

File này là **hướng dẫn độc lập** để chạy phần thực nghiệm nhỏ cho bài báo:

**Indexing and Partitioning Design Framework for Spatio-Temporal Surveillance Video Metadata Graphs**

Thực nghiệm này chỉ dùng để kiểm chứng nhẹ rằng thiết kế vật lý theo workload-aware framework giúp giảm **metadata access space** trước khi xử lý truy vấn.

Cụ thể, thực nghiệm đo:

- số partition phải truy cập;
- số dòng metadata phải scan;
- số candidate observation/relation còn lại sau filtering;
- tỷ lệ giảm scan/candidate;
- thời gian lọc metadata ban đầu.

Thực nghiệm này **không phải** benchmark distributed RPQ/ERPQ.  
Không dùng để báo cáo LocalEval, GlobalEval, stitching, hard validation, witness reconstruction hoặc speedup của thuật toán truy vấn phân tán.

---

## 1. Thư mục project

Làm việc trong đúng thư mục:

```bash
cd /home/hduser/VideoGraphDB_Project/src/Vr13_official_benchmark_C1
```

Cấu trúc cần có thêm cho phần thực nghiệm này:

```text
Vr13_official_benchmark_C1
│
├── run_indexing_partitioning_lightweight_eval.py
├── indexing_partitioning_workloads.py
│
├── graph_io.py
├── config.py
├── queries.py
│
├── query_specs/
│   ├── common.py
│   ├── q11.py
│   └── q43.py
│
└── scripts/
    ├── check_hdfs_before_benchmark.sh
    └── run_indexing_partitioning_lightweight_eval.sh
```

---

## 2. Vai trò các file mới

| File | Mục đích |
|---|---|
| `indexing_partitioning_workloads.py` | Mô tả workload W1/W2/W4 và các cấu hình C0/C1/C2 |
| `run_indexing_partitioning_lightweight_eval.py` | Python runner chính để đo filtering và partition access |
| `scripts/run_indexing_partitioning_lightweight_eval.sh` | Script shell chuẩn để chạy thực nghiệm |

Các file benchmark chính của bài main như `baseline.py`, `proposed_distributed.py`, `run_experiment.py` **không bị sửa và không được gọi trong thực nghiệm này**.

---

## 3. Cấp quyền và kiểm tra compile

```bash
chmod +x scripts/run_indexing_partitioning_lightweight_eval.sh
```

Kiểm tra Python:

```bash
python3 -m py_compile \
  run_indexing_partitioning_lightweight_eval.py \
  indexing_partitioning_workloads.py \
  graph_io.py \
  config.py \
  query_specs/common.py \
  query_specs/q11.py \
  query_specs/q43.py
```

Kiểm tra shell script:

```bash
bash -n scripts/run_indexing_partitioning_lightweight_eval.sh
```

Nếu các lệnh trên không báo lỗi thì chuyển sang bước tiếp theo.

---

## 4. Kiểm tra HDFS và Spark trước khi chạy

Chạy preflight HDFS:

```bash
./scripts/check_hdfs_before_benchmark.sh
```

Cần đảm bảo:

```text
Safe mode is OFF
Live datanodes >= 2
/spark-logs tồn tại và ghi được
Spark master/workers đang chạy
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

## 5. Chạy thử nhanh trên một dataset

Chạy thử với `dataset_1m`:

```bash
DATASETS=dataset_1m REPEATS=1 \
./scripts/run_indexing_partitioning_lightweight_eval.sh
```

Mặc định script mới chạy ba workload đại diện: `W1,W2,W4`.

Nếu muốn giới hạn theo TimeWindow để thấy rõ hiệu quả pruning:

```bash
DATASETS=dataset_1m REPEATS=1 \
TW_START_NUM=1 TW_END_NUM=20 \
./scripts/run_indexing_partitioning_lightweight_eval.sh
```

Nếu muốn chỉ định entity cụ thể cho W2:

```bash
DATASETS=dataset_1m REPEATS=1 \
W2_ENTITY_ID=P20260521_0002 \
TW_START_NUM=1 TW_END_NUM=20 \
./scripts/run_indexing_partitioning_lightweight_eval.sh
```

Nếu không truyền `W2_ENTITY_ID`, chương trình sẽ tự chọn một person/entity xuất hiện nhiều nhất trong dữ liệu để làm target cho W2.

---

## 6. Chạy trên nhiều dataset

Ví dụ chạy trên 6 dataset:

```bash
DATASETS=dataset_1m,dataset_2m,dataset_4m,dataset_6m,dataset_8m,dataset_10m \
REPEATS=1 \
TW_START_NUM=1 TW_END_NUM=20 \
./scripts/run_indexing_partitioning_lightweight_eval.sh
```

Nếu muốn lặp lại 3 lần để lấy median/mean:

```bash
DATASETS=dataset_1m,dataset_2m,dataset_4m,dataset_6m,dataset_8m,dataset_10m \
REPEATS=3 \
TW_START_NUM=1 TW_END_NUM=20 \
./scripts/run_indexing_partitioning_lightweight_eval.sh
```

---

## 7. Các workload và cấu hình được đo

### Workload

| Workload | Ý nghĩa |
|---|---|
| `W1` | Entity/context lookup access: truy cập entity/observation theo location, camera và time window |
| `W2` | Trajectory candidate access: truy cập observation của cùng entity theo không gian–thời gian |
| `W4` | Composite pattern candidate access: truy cập các relation như `CARRIES`, `INTERACTS_WITH`, `USES` trước khi pattern validation |

### Configuration

| Configuration | Ý nghĩa |
|---|---|
| `C0_FULL_SCAN` | Scan toàn bộ metadata liên quan, chưa dùng partition pruning/index |
| `C1_LxTW_PRUNING` | Dùng Location × TimeWindow pruning |
| `C2_WORKLOAD_INDEX` | Dùng workload-aware index: `context_lookup_index` cho W1, `entity_partition_index` cho W2, `relation_index` cho W4 |

---

## 8. Output kết quả

Kết quả được ghi riêng, không trộn với official benchmark của bài main:

```text
benchmark_outputs_official/
└── indexing_partitioning/
    └── <RUN_ID>/
        ├── lightweight_design_validation_detail.csv
        ├── lightweight_design_validation_aggregate.csv
        └── logs/
```

Ý nghĩa file:

| File | Mục đích |
|---|---|
| `lightweight_design_validation_detail.csv` | Kết quả chi tiết theo dataset, repeat, workload, configuration |
| `lightweight_design_validation_aggregate.csv` | Kết quả tổng hợp, dùng để đưa vào bài báo |
| `logs/` | Log chạy thực nghiệm |

---

## 9. Các cột kết quả quan trọng

| Cột | Ý nghĩa |
|---|---|
| `dataset` | Dataset đang chạy |
| `workload_id` | W1, W2 hoặc W4 |
| `configuration_id` | C0, C1 hoặc C2 |
| `total_partitions` | Tổng số partition trong dataset |
| `scanned_partitions` | Số partition phải truy cập |
| `partition_pruning_ratio` | Tỷ lệ giảm partition |
| `rows_scanned` | Số dòng metadata/index được scan |
| `candidate_rows` | Số candidate observation/relation còn lại |
| `row_scan_reduction_vs_C0` | Tỷ lệ giảm rows scanned so với C0 |
| `candidate_reduction_vs_C0` | Tỷ lệ giảm candidate so với C0 |
| `filtering_time_s` | Thời gian lọc metadata ban đầu |

---

## 10. Bảng kết quả nên đưa vào bài báo

Có thể tạo bảng gọn như sau:

| Workload | Configuration | Scanned partitions | Partition pruning ratio | Rows scanned | Candidate rows | Row scan reduction |
|---|---|---:|---:|---:|---:|---:|
| W2 | C0 Full scan | ... | ... | ... | ... | ... |
| W2 | C1 L×TW pruning | ... | ... | ... | ... | ... |
| W2 | C2 L×TW + entity index | ... | ... | ... | ... | ... |
| W4 | C0 Full scan | ... | ... | ... | ... | ... |
| W4 | C1 L×TW pruning | ... | ... | ... | ... | ... |
| W4 | C2 L×TW + relation index | ... | ... | ... | ... | ... |

Nên lấy số liệu từ:

```text
lightweight_design_validation_aggregate.csv
```

---

## 11. Cách diễn giải kết quả trong bài

Nên viết:

```text
The lightweight validation does not evaluate distributed RPQ execution.
Instead, it measures how the workload-aware physical design reduces the
metadata access space before query processing.
```

Có thể kết luận:

- Location × TimeWindow giúp giảm số partition phải scan.
- Entity-partition index hỗ trợ W2 bằng cách giới hạn các partition có chứa entity cần truy vết.
- Relation index hỗ trợ W4 bằng cách giảm số relation candidate trước pattern validation.
- Thiết kế vật lý này tạo tiền đề cho downstream distributed query processing.

Không nên viết:

```text
The proposed distributed RPQ algorithm is faster.
```

Không nên báo cáo các chỉ số sau trong bài Indexing & Partitioning:

```text
LocalEval runtime
GlobalEval runtime
stitched candidates
valid witnesses
hard validation time
distributed RPQ speedup
```

Các chỉ số này thuộc bài main về distributed RPQ/ERPQ.

---

## 12. Quy tắc quan trọng

Không chạy script này đồng thời với official benchmark chính.

Không trộn kết quả của script này với:

```text
benchmark_outputs_official/official/
benchmark_outputs_official/correctness/
```

Không dùng kết quả này để so sánh Baseline vs Proposed Distributed.

Không gọi đây là full benchmark. Nên gọi là:

```text
lightweight design validation
```

hoặc:

```text
metadata filtering and partition-access evaluation
```

---

## 13. Tóm tắt thứ tự chạy chuẩn

```bash
cd /home/hduser/VideoGraphDB_Project/src/Vr13_official_benchmark_C1

# 1. Cấp quyền
chmod +x scripts/run_indexing_partitioning_lightweight_eval.sh

# 2. Kiểm tra compile
python3 -m py_compile \
  run_indexing_partitioning_lightweight_eval.py \
  indexing_partitioning_workloads.py \
  graph_io.py \
  config.py \
  query_specs/common.py \
  query_specs/q11.py \
  query_specs/q43.py

bash -n scripts/run_indexing_partitioning_lightweight_eval.sh

# 3. Kiểm tra HDFS/Spark
./scripts/check_hdfs_before_benchmark.sh

# 4. Chạy thử nhanh
DATASETS=dataset_1m REPEATS=1 \
TW_START_NUM=1 TW_END_NUM=20 \
./scripts/run_indexing_partitioning_lightweight_eval.sh

# 5. Chạy nhiều dataset
DATASETS=dataset_1m,dataset_2m,dataset_4m,dataset_6m,dataset_8m,dataset_10m \
REPEATS=3 \
TW_START_NUM=1 TW_END_NUM=20 \
./scripts/run_indexing_partitioning_lightweight_eval.sh
```

