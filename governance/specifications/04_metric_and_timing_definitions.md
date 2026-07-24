# SPEC-METRIC — Định nghĩa metric và timing

## 1. Runtime

- `baseline_total_s`: actual wall-clock của phạm vi Baseline đã công bố.
- `proposed_total_s`: actual wall-clock của phạm vi Proposed đã công bố.
- `step_i_s`: runtime của phase được materialize trong C2.
- `partition_parallel_estimate_s`: ước lượng theo thời gian partition, không đồng nhất với wall-clock.
- `phase_sum_s`: tổng các phase đã đo.
- `unaccounted_time_s = actual_wall_time_s - phase_sum_s`.

## 2. Hiệu năng

`speedup = baseline_total_s / proposed_total_s`

`time_reduction_pct = (baseline_total_s - proposed_total_s) / baseline_total_s × 100%`

## 3. Correctness

- `candidate_count`: số candidate theo định nghĩa cụ thể.
- `witness_count`: số witness trước/sau duplicate handling phải ghi rõ.
- `normalized_witness_count`: số phần tử của tập witness chuẩn hóa.
- `result_match`: equality của normalized witness set.

## 4. Thống kê repeats

Khai báo trước:

- Số repeats.
- Dùng mean hay median làm đại diện.
- Báo cáo standard deviation, min/max hoặc IQR.
- Quy tắc warm-up.
- Quy tắc xử lý outlier.
