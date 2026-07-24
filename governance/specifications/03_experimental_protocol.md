# SPEC-EXP — Giao thức thực nghiệm

## 1. Correctness mode

- Baseline và Proposed dùng cùng input/config.
- Thu thập kết quả đủ để so sánh normalized witness set.
- Missing, timeout, exception hoặc skipped không được coi là PASS.
- `require-nonzero` chỉ bật khi ngữ nghĩa/dataset bảo đảm có kết quả.

## 2. C1 — Official end-to-end benchmark

- Mục tiêu: đo runtime end-to-end chính thức.
- Spark lazy execution được giữ đến final action.
- Timer bao phủ phạm vi đã công bố.
- Không đưa correctness diagnostics nặng vào timing-only nếu hai thuật toán đã qua correctness và việc loại bỏ là đối xứng.
- Baseline và Proposed chạy tuần tự, dưới tài nguyên tương đương.
- Dùng làm số liệu runtime chính trong bài báo.

## 3. C2 — Profiling benchmark

- Mục tiêu: phân rã chi phí.
- Materialize ở phase boundary.
- Báo cáo actual wall-time và phase metrics riêng.
- `partition_parallel_estimate` chỉ là ước lượng.
- Báo cáo unaccounted overhead.
- Dùng để giải thích bottleneck, không tự động thay thế C1.

## 4. Ma trận chạy

| Mode | Dataset | Query | Repeats | Output |
|---|---|---|---:|---|
| Correctness | 1M mặc định | Toàn bộ registry | 1 | witness/gate/log |
| C1 | 1M–10M | Toàn bộ registry | Theo kế hoạch | end-to-end runtime |
| C2 | 1M–10M hoặc tập chọn | Toàn bộ/tập chọn | Theo kế hoạch | phase metrics |

## 5. Quy tắc lỗi

- Giữ nguyên run lỗi trong manifest.
- Không tính trung bình trên tập run đã âm thầm loại lỗi.
- Retry phải được ghi rõ và không trộn với lần chạy hợp lệ ban đầu.
