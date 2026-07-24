# VideoGraphDB — Compact Indexing & Partitioning Evaluation

## 1. Mục tiêu tổ chức

Bộ mã được rút từ 10 file xuống 7 file, nhưng vẫn giữ hai luồng độc lập:

- `correctness`: xác nhận C0, C1 và C2 trả cùng kết quả logic;
- `evaluation`: đặc trưng hóa dữ liệu, xây chỉ mục, scalability, robustness và mismatch.

Correctness dùng runner và shell riêng để lỗi đúng/sai không bị trộn với số liệu thực nghiệm.

## 2. Cấu trúc file

```text
project_root/
├── indexing_partitioning_core.py
├── run_indexing_partitioning_correctness.py
├── run_indexing_partitioning_eval.py
├── config/
│   └── ip_query_instances.csv
└── scripts/
    ├── run_indexing_partitioning_correctness.sh
    └── run_indexing_partitioning_eval.sh
```

Bộ mã tiếp tục sử dụng các file đã có của project:

```text
config.py
graph_io.py
query_specs/q11.py
scripts/check_hdfs_before_benchmark.sh   # tùy chọn
```

## 3. Vai trò file

- `indexing_partitioning_core.py`: workload W1–W5, query manifest, data utilities, access structures C0–C3.
- `run_indexing_partitioning_correctness.py`: count/fingerprint correctness.
- `run_indexing_partitioning_eval.py`: các phase đo lường, không chạy correctness.
- `ip_query_instances.csv`: query instances dùng chung.
- Hai shell script: tách quy trình correctness và evaluation.

## 4. Kiểm tra compile

```bash
python3 -m py_compile \
  indexing_partitioning_core.py \
  run_indexing_partitioning_correctness.py \
  run_indexing_partitioning_eval.py

bash -n scripts/run_indexing_partitioning_correctness.sh
bash -n scripts/run_indexing_partitioning_eval.sh
```

## 5. Thứ tự chạy

### Bước 1 — Dataset characterization

```bash
PHASE=dataset_characterization \
DATASETS=dataset_1m \
./scripts/run_indexing_partitioning_eval.sh
```

Kết quả chính:

```text
dataset_characterization.csv
```

Sau bước này, kiểm tra các entity ID, relation type và TimeWindow trong `config/ip_query_instances.csv`.

### Bước 2 — Correctness độc lập

```bash
DATASETS=dataset_1m \
WORKLOADS=W1,W2,W3,W4,W5 \
INDEX_MODE=memory \
./scripts/run_indexing_partitioning_correctness.sh
```

Chỉ chuyển bước khi mọi query PASS count và fingerprint.

### Bước 3 — Build indexes

```bash
PHASE=build_indexes \
DATASETS=dataset_1m,dataset_2m,dataset_4m,dataset_6m,dataset_8m,dataset_10m \
./scripts/run_indexing_partitioning_eval.sh
```

### Bước 4 — Scalability

```bash
PHASE=scalability \
DATASETS=dataset_1m,dataset_2m,dataset_4m,dataset_6m,dataset_8m,dataset_10m \
WORKLOADS=W1,W2,W3,W4,W5 \
INDEX_MODE=hdfs \
./scripts/run_indexing_partitioning_eval.sh
```

### Bước 5 — Robustness

```bash
PHASE=robustness \
DATASETS=dataset_10m \
WORKLOADS=W1,W2,W3,W4,W5 \
INDEX_MODE=hdfs \
./scripts/run_indexing_partitioning_eval.sh
```

### Bước 6 — Mismatch analysis

```bash
PHASE=mismatch \
DATASETS=dataset_10m \
WORKLOADS=W1,W2,W3,W4,W5 \
INDEX_MODE=hdfs \
./scripts/run_indexing_partitioning_eval.sh
```

## 6. Nguyên tắc không gộp thêm

Không gộp correctness runner vào evaluation runner vì:

- correctness cần exit code riêng;
- từng query cần log/status độc lập;
- scalability chỉ được chạy sau khi correctness PASS;
- giảm nguy cơ số liệu hiệu năng che khuất lỗi logic.

Không đưa query instances vào Python vì CSV giúp thay đổi query mà không sửa code và lưu lại cấu hình thực nghiệm.

