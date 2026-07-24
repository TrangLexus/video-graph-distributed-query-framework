# Vr15 C2 — bộ mã tuân thủ đặc tả và tối ưu độ phức tạp

## 1. Phạm vi

Bộ mã này được chỉnh theo tài liệu **Vr15_Dac_ta_Benchmark_C2_Cong_thuc_va_Rang_buoc.pdf** cho pipeline:

```text
LocalEval(G_i,Q)
→ phân loại fragment
→ C_L và C_S
→ C_M = Deduplicate(C_L ∪ C_S)
→ W_V = HardValidate(C_M,Q)
→ W_F = Deduplicate(Normalize(W_V))
```

Các tệp thay thế:

```text
baseline.py
proposed_distributed.py
run_experiment.py
query_specs/common.py
query_specs/q11.py ... query_specs/q43.py
```

Các tệp kiểm toán đi kèm:

```text
audit_vr15_static.py
check_project_compatibility.py
STATIC_AUDIT_REPORT.json
```

## 2. Các sửa đổi chính

### 2.1. Baseline

- Một global physical plan: Merge/Union → materialize global base graph → query → Hard Validation → normalization.
- Không dựng cạnh `NEXT_TW_*` vật lý.
- Step 1 và Step 2 kết thúc sau Spark action.
- Tổng thuật toán đo bằng outer wall timer.
- Dataset validation chỉ bật trong correctness và không thuộc algorithm runtime.
- Báo cáo riêng `num_valid_witnesses_before_normalization` và `num_final_valid_witnesses`.

### 2.2. Proposed

- LocalEval cho năm graph partition được submit gần đồng thời.
- Thời gian LocalEval chính thức là `max(partition_completion_offset_seconds)`.
- `partition_work_sum` chỉ là diagnostic.
- True boundary dùng bounded equi-join theo:

```text
interface_id + interface_key + target_tw_num
```

- Kiểm tra khác partition, logical fragment khác nhau, loại replicated temporal instance, role/state interface, TimeWindow pruning và timestamp thực.
- Không self-join bằng điều kiện OR trên nhiều ID.
- Tách `is_true_boundary` và `is_stitch_support`; support không bị ghi nhãn nhầm thành boundary.
- Chỉ truyền query-scoped atomic boundary/support fragments lên GlobalEval.
- Không dựng compact entity index O(|V|) vì cả 12 distributed evaluator hiện không đọc tham số `vertices`; static audit kiểm tra bất biến này.
- Không union DataFrame fragment wide chỉ để đếm trong runtime-only.
- Một action cần thiết để materialize mỗi ranh giới phase.

### 2.3. Candidate và witness

- Candidate dedup dùng `candidate_id` query-specific để giữ các evidence path khác nhau đến sau Hard Validation.
- Witness normalization dùng đúng `correctness_compare_columns`.
- Final witness chỉ được tạo từ `merged_candidates`.
- Correctness gate dựa trên normalized witness set, không dựa trên count.

### 2.4. Runner C2

Chỉ có ba mode:

```text
correctness
profiling
profiling_runtime_only
```

`profiling_runtime_only` mới tính speedup. Mode này:

- bắt buộc có correctness gate cùng source hash và parameter signature;
- không cho phép query diagnostics;
- không lưu witness CSV;
- không chạy dataset validation;
- không count dataset/intermediate ngoài metric phase bắt buộc.

Speedup đại diện dùng:

```text
median(Baseline runtime) / median(Proposed runtime)
```

không dùng trung bình các speedup từng lần.

## 3. Độ phức tạp

Đặt:

- `F`: số atomic query fragments;
- `I`: số boundary interface của QuerySpec, là hằng số nhỏ;
- `K = ceil(max_time_gap_seconds / time_window_duration_seconds) = 12`;
- `J`: số cặp fragment thực sự khớp boundary join;
- `C`: tổng số candidates được tạo.

Boundary detection:

```text
Thời gian: O(I × K × F + J)
Bộ nhớ:    O(I × K × F + J)
```

Vì `I` và `K=12` là hằng số nhỏ, phần chuẩn bị tuyến tính theo `F`. Thiết kế tránh full self-join O(F²). Trong trường hợp dữ liệu cực lệch mà mọi record cùng khóa và cùng cửa sổ thời gian, `J` tự nó có thể lớn; đây là output-size lower bound, không thể loại bỏ mà vẫn giữ đầy đủ kết quả.

Typed-identity support:

```text
O(F + B)
```

với tối đa năm identity record trên một atomic fragment. Query-specific joins tiếp tục dùng equi-binding và bounded temporal predicates; độ phức tạp là output-sensitive thay vì Cartesian product toàn cục.

## 4. Kiểm toán đã thực hiện

Trong môi trường tạo bundle:

```bash
python3 -m py_compile \
  baseline.py \
  proposed_distributed.py \
  run_experiment.py \
  query_specs/*.py \
  audit_vr15_static.py \
  check_project_compatibility.py

python3 audit_vr15_static.py
```

Kết quả:

```text
status = PASS
num_query_specs = 12
errors = []
```

Static audit xác nhận:

- đủ 12 QuerySpec;
- hợp đồng automaton/boundary interface hợp lệ;
- explicit candidate/witness keys;
- Proposed không global-union base graph;
- không có physical NEXT_TW builder;
- không nới `minimum_partition_count` trong LocalEval;
- dùng bounded equi-join;
- final witness xuất phát từ merged candidates;
- runner kiểm tra bốn lớp fragment, candidate/witness pipeline và timing decomposition.

## 5. Giới hạn xác nhận

Môi trường tạo bundle không có PySpark và không truy cập cluster/HDFS của dự án. Vì vậy:

- đã xác nhận cú pháp, hợp đồng metadata và cấu trúc thuật toán bằng static audit;
- chưa thể xác nhận Spark logical/physical plan thực tế;
- chưa thể xác nhận schema runtime của `config.py`, `graph_io.py`, `queries.py` hiện có;
- chưa thể chứng nhận correctness trên dữ liệu thật trước khi chạy gate.

Không dùng số liệu benchmark trước khi hoàn tất:

```text
compatibility PASS
→ Q1.1 correctness PASS
→ Q4.3 correctness PASS
→ 12/12 query correctness PASS trên dataset_1m
→ profiling_runtime_only
```

## 6. Áp dụng an toàn

### Bước A — giải nén ở thư mục tạm

```bash
cd ~/VideoGraphDB_Project/src/Vr15_profile_benchmark_C2
mkdir -p _vr15_v4_review
unzip -o ~/Downloads/Vr15_spec_compliant_optimized_v4.zip -d _vr15_v4_review
```

### Bước B — kiểm tra tương thích, chưa chép đè

```bash
python3 _vr15_v4_review/check_project_compatibility.py \
  --project-root ~/VideoGraphDB_Project/src/Vr15_profile_benchmark_C2
```

Dừng tại đây và xem kết quả. Chỉ tiếp tục khi `status = PASS`.

### Bước C — sao lưu và chép đè

```bash
cd ~/VideoGraphDB_Project/src/Vr15_profile_benchmark_C2
STAMP=$(date +%Y%m%d_%H%M%S)
mkdir -p "backup_before_vr15_v4_${STAMP}/query_specs"

cp baseline.py proposed_distributed.py run_experiment.py \
  "backup_before_vr15_v4_${STAMP}/"
cp query_specs/*.py "backup_before_vr15_v4_${STAMP}/query_specs/"

cp _vr15_v4_review/baseline.py .
cp _vr15_v4_review/proposed_distributed.py .
cp _vr15_v4_review/run_experiment.py .
cp _vr15_v4_review/query_specs/*.py query_specs/
```

### Bước D — compile và static audit trên cluster

```bash
python3 -m py_compile \
  baseline.py \
  proposed_distributed.py \
  run_experiment.py \
  query_specs/*.py

python3 _vr15_v4_review/audit_vr15_static.py
```

### Bước E — correctness Q1.1

```bash
cd ~/VideoGraphDB_Project/src/Vr15_profile_benchmark_C2

OUTPUT_BASE=./query_outputs_vr15_balanced
EXPERIMENT_DIR="${OUTPUT_BASE}/correctness_v4/q11_1m"
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

Chỉ khi Q1.1 PASS mới chạy Q4.3. Không chạy đủ 12 query hoặc benchmark trước khi kiểm tra log và metrics Q1.1.
