# SPEC-ALG — Đặc tả thuật toán

## 1. Baseline

Mô tả pipeline logic, input, output và các phép biến đổi bắt buộc.

### ALG-B-01 — Input
Baseline và Proposed phải nhận cùng snapshot dữ liệu và cùng query configuration.

### ALG-B-02 — Evaluation
Ghi rõ physical plan được chấp nhận, các phép join/traversal và nơi thực hiện validation.

### ALG-B-03 — Finalization
Hard Validation và Witness Normalization phải được áp dụng theo cùng định nghĩa với Proposed.

## 2. Proposed

Pipeline chuẩn:

`Partition lookup → LocalEval → Fragment classification → Stitching → Hard Validation → Witness Normalization`

### ALG-P-01 — LocalEval
LocalEval chỉ sử dụng dữ liệu thuộc phạm vi cục bộ được định nghĩa. Việc thực thi có thể dùng DataFrame joins, traversal, automaton hoặc physical plan khác, nhưng phải giữ nguyên ngữ nghĩa query.

### ALG-P-02 — Complete local fragment
Fragment đã thỏa mãn đầy đủ query trong phạm vi cục bộ; không cần nối với partition khác.

### ALG-P-03 — Boundary fragment
Fragment chưa hoàn thành query và chứa thông tin cần thiết để nối với dữ liệu ngoài phạm vi cục bộ.

### ALG-P-04 — Stitching
Chỉ ghép các fragment tương thích theo khóa thực thể, quan hệ, thời gian và điều kiện query.

### ALG-P-05 — Hard Validation
Loại candidate không thỏa đầy đủ ngữ nghĩa query sau bước ghép.

### ALG-P-06 — Normalization
Chuyển witness về biểu diễn chuẩn để so sánh tập hợp, khử khác biệt do thứ tự, alias hoặc duplicate không có ý nghĩa.

## 3. Temporal semantics

### ALG-T-01 — Temporal instance
Ví dụ `P20260423_0001_TW2550` là temporal instance. Một temporal instance phải ánh xạ nhất quán với TimeWindow liên quan.

### ALG-T-02 — Tham số thời gian
Không đồng nhất `time_window_duration_seconds`, `max_time_gap_seconds`, `max_nexttw_hops` và `quick_exit_max_seconds`.

## 4. Quick-exit

Ghi rõ query nào cho phép quick-exit, vai trò thực thể, mốc tham chiếu, ngưỡng và điều kiện partition tối thiểu.

## 5. Các bất biến

- Cùng query semantics.
- Cùng final validation.
- Cùng normalization.
- Không bỏ evidence hợp lệ để tối ưu runtime.
- Không dùng diagnostics thay thế kết quả khoa học.
