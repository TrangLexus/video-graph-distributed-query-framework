# Vr15 — Gói sửa đồng bộ Baseline, Proposed, QuerySpec và Runner

## 1. Các tệp phải thay thế

- `baseline.py`
- `proposed_distributed.py`
- `run_experiment.py`
- `query_specs/common.py`
- `query_specs/q11.py`
- `query_specs/q13.py`
- `query_specs/q22.py`
- `query_specs/q23.py`
- `query_specs/q43.py`

Các tệp QuerySpec còn lại được đóng gói để đối chiếu nhưng không thay đổi ngữ nghĩa.

## 2. Nội dung sửa chính

### Baseline
- Tách `time_dataset_integrity_validation_seconds` khỏi runtime thuật toán.
- Không tái sử dụng logical plan validation cho Step 1.
- Sửa công thức non-algorithm overhead.
- Chuẩn hóa `end_to_end_scope`.

### Proposed
- Giữ boundary fix theo logical fragment ID, loại replicated evidence giả.
- Không dùng `edge_id` trong logical fragment ID.
- Khai báo role-pair Q4.3 trong QuerySpec thay vì hard-code theo query ID.
- Phân biệt raw candidate, complete-local candidate và valid witness.
- Dùng một Spark action cho mỗi partition để materialize toàn bộ đầu ra LocalEval.
- Dùng semantic dedup columns qua QuerySpec.
- Tạo final witness trực tiếp từ merged candidates:
  `Normalize(HardValidate(Dedup(local complete ∪ stitched)))`.

### Runner
- Mặc định dùng `hdfs:///data/videographdb_balanced`.
- Mặc định tài nguyên Spark: driver 2G, executor 5G, 2 cores/executor,
  cores-max 8, shuffle partitions 16.
- Correctness so sánh cả candidate set và normalized final witness set.
- Gate PASS chính thức dựa trên witness-set equality.
- Ghi count, set differences, stable hashes và compare columns vào JSON gate.
- Hỗ trợ `--query-id ALL` để chạy tuần tự đủ 12 query.
- Kiểm tra timing decomposition và max partition completion offset.

## 3. Cài đặt trên master

```bash
cd ~/VideoGraphDB_Project/src/Vr15_profile_benchmark_C2

STAMP=$(date +%Y%m%d_%H%M%S)
mkdir -p "backup_before_vr15_complete_fix_${STAMP}/query_specs"

cp baseline.py proposed_distributed.py run_experiment.py \
  "backup_before_vr15_complete_fix_${STAMP}/"

cp query_specs/common.py query_specs/q11.py query_specs/q13.py \
   query_specs/q22.py query_specs/q23.py query_specs/q43.py \
  "backup_before_vr15_complete_fix_${STAMP}/query_specs/"
```

Chép các tệp trong gói vào đúng vị trí tương ứng.

## 4. Kiểm tra tĩnh

```bash
python3 -m py_compile \
  baseline.py \
  proposed_distributed.py \
  run_experiment.py \
  query_specs/*.py
```

Kiểm tra không còn Q14/tên cũ:

```bash
grep -RInE \
  "Q14|q14_|max_nexttw_hops|derived_max_time_window_gap|Vr06|vr06" \
  --include="*.py" \
  --include="*.sh" \
  --include="*.md" \
  . \
  --exclude="*.bak*"
```

## 5. Gate chạy thử đầu tiên

Chỉ chạy Q1.1 trên `dataset_1m` trước. Không chạy ALL ngay.

```bash
OUTPUT_BASE=./query_outputs_vr15_balanced
EXPERIMENT_DIR="${OUTPUT_BASE}/correctness/q11_1m"
rm -rf "${EXPERIMENT_DIR}"

python3 run_experiment.py \
  --hdfs-root hdfs:///data/videographdb_balanced \
  --output-root "${EXPERIMENT_DIR}" \
  --query-id Q1.1 \
  --datasets dataset_1m \
  --mode correctness \
  --repeats 1 \
  --require-nonzero \
  --spark-master spark://master:7077 \
  --driver-memory 2G \
  --executor-memory 5G \
  --executor-cores 2 \
  --cores-max 8 \
  --shuffle-partitions 16 \
  --time-window-duration-seconds 10 \
  --max-time-gap-seconds 120 \
  --quick-exit-max-seconds 120 \
  --exit-person-role P1 \
  --minimum-partition-count 1 \
  --quick-exit-reference-role CARRY2
```

Chỉ khi Q1.1 PASS và boundary metrics hợp lý mới chạy Q4.3; sau đó mới chạy
`--query-id ALL` trên `dataset_1m`.

## 6. Phạm vi kiểm chứng

Các tệp trong gói đã qua `python3 -m py_compile`. Chúng chưa được thực thi trên
Spark/HDFS của cluster; correctness và performance phải được xác nhận theo các gate
Q1.1 → Q4.3 → 12 query.
