# VideoGraphDB Vr15 — LocalEval–GlobalEval đồng bộ với đặc tả Vr05

## 1. Mục tiêu

Vr15 đồng bộ mã nguồn với pipeline:

```text
Partitioned graph
→ LocalEval độc lập trên từng graph partition
→ complete-local fragments + true-boundary fragments
→ Stitching
→ MergeCandidates
→ Hard Validation
→ Witness Normalization
→ all final valid witnesses
```

Mã hỗ trợ hai mục tiêu:

- **Correctness**: xuất và đối sánh normalized witness set giữa Baseline và Proposed.
- **Benchmark/Profiling**: đo rõ thời gian từng phase, cardinality fragment/candidate/witness và tổng thời gian.

## 2. Danh sách truy vấn

Chỉ có đúng 12 query:

```text
Q1.1 Q1.2 Q1.3
Q2.1 Q2.2 Q2.3
Q3.1 Q3.2 Q3.3
Q4.1 Q4.2 Q4.3
```

Không có query ID hoặc alias nào khác.

## 3. Công thức thời gian

### Baseline

```text
T_baseline_algorithm
= T_global_merge
+ T_global_query_validation
```

Baseline sử dụng wall-clock time của một global Spark plan. Không lấy `max` và không cộng runtime riêng của input partition.

### Proposed

Với cùng mốc bắt đầu `t0`, LocalEval của partition `i` hoàn thành tại `t_i_end`:

```text
T_localeval_parallel = max_i(t_i_end - t0)
```

```text
T_proposed_algorithm
= T_boundary_index
+ T_localeval_parallel
+ T_fragment_classification
+ T_globaleval
```

`sum(partition job elapsed)` chỉ là tổng lượng công việc chẩn đoán, không được dùng tính runtime, speedup hoặc time reduction.

## 4. Thực thi song song

- Tất cả graph partition được submit đồng thời bằng các driver thread.
- Spark dùng `spark.scheduler.mode=FAIR`.
- Số job LocalEval được submit bằng số graph partition.
- Số Spark task chạy thực tế bị giới hạn bởi `spark.cores.max` và executor slots.
- Mỗi LocalEval có Spark job group và scheduler pool riêng.

## 5. NEXT_TW_*

Vr15 không tạo cạnh `NEXT_TW_*` vật lý toàn cục. Continuity được kiểm tra bằng:

- identity/binding compatibility;
- temporal order;
- `candidate_pruning_max_time_window_gap` để pruning;
- `max_time_gap_seconds` để Hard Validation theo timestamp.

## 6. Thuật ngữ output

- **Fragment**: output LocalEval.
- **Candidate**: cấu trúc đã lắp ghép nhưng chưa chắc hợp lệ cuối cùng.
- **Valid witness**: candidate đã vượt Hard Validation.
- **Final valid witness**: valid witness sau normalization và deduplication.

## 7. Chạy correctness

```bash
bash scripts/run_correctness_representative.sh
```

Hoặc:

```bash
python3 run_experiment.py \
  --mode correctness \
  --query-id Q1.1 \
  --datasets dataset_1m \
  --output-root query_outputs_vr15_correctness
```

Correctness PASS khi normalized witness set của Baseline và Proposed bằng nhau. Count bằng nhau nhưng nội dung khác vẫn là FAIL.

## 8. Chạy profiling một query

```bash
bash scripts/run_one_query_profile.sh Q1.1
```

Runtime-only:

```bash
bash scripts/run_one_query_profile_runtime_only.sh Q1.1
```

## 9. Chạy 12 query × 6 dataset × 3 lần

Từ thư mục dự án:

```bash
bash scripts/run_benchmark_12queries_6datasets_r3.sh
```

## 10. Các metric chính

### Baseline

```text
time_step1_global_merge_seconds
time_step2_global_query_validation_seconds
time_algorithm_total_seconds
time_end_to_end_total_seconds
```

### Proposed

```text
time_step1_boundary_index_preparation_seconds
time_step1_localeval_parallel_seconds
time_step1_localeval_wall_seconds
time_step1_localeval_partition_work_sum_seconds
time_step1_fragment_classification_seconds
time_step2_globaleval_seconds
time_algorithm_total_seconds
time_end_to_end_total_seconds
```

Cardinality:

```text
num_query_relevant_atomic_evidence
num_complete_local_fragments
num_true_boundary_fragments
num_complete_and_boundary_fragments
num_dead_end_fragments
num_stitched_candidates
num_merged_candidates
num_final_valid_witnesses
```

## 11. Bất biến được runner kiểm tra

```text
Baseline algorithm ≈ Baseline Step1 + Baseline Step2
Proposed LocalEval = max(partition completion offset)
Proposed algorithm ≈ index + parallel LocalEval + classification + GlobalEval
End-to-end ≥ input discovery + algorithm
Baseline final witness set = Proposed final witness set trong correctness mode
```
