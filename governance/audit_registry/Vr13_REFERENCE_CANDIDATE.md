# Vr13 Reference Candidate

## 1. Phạm vi

Vr13 được đóng băng dưới dạng một bộ gồm hai thành phần:

1. `src/Vr13_official_benchmark_C1`
   - Official end-to-end benchmark.
   - Dùng cho runtime chính thức.

2. `src/Vr13_profile_benchmark_C2`
   - Profiling benchmark.
   - Dùng phân tích chi phí từng bước.

## 2. Quan hệ giữa C1 và C2

C1 và C2 thuộc cùng thế hệ thuật toán Vr13 nhưng sử dụng giao thức đo khác nhau.

- C1 không được thay thế bằng C2 khi báo cáo runtime end-to-end.
- C2 không được diễn giải như runtime end-to-end nếu có materialization tại phase boundary.
- Baseline và Proposed trong cả hai thành phần phải được kiểm tra tính đồng bộ về query semantics, validation và normalization.

## 3. Trạng thái tại thời điểm đóng băng

- Source state: frozen.
- Audit state: `NOT_AUDITED`.
- Correctness state: cần xác minh lại bằng bằng chứng.
- Fairness state: chưa phê duyệt theo SOP mới.
- Timing-scope state: chưa phê duyệt theo SOP mới.
- Benchmark readiness: chưa được cấp.

## 4. Quy tắc thay đổi

Mọi thay đổi sau mốc reference candidate phải:

1. Có CHG-ID.
2. Có Change Impact Matrix.
3. Làm trạng thái quay lại `NOT_AUDITED`.
4. Tạo commit mới.
5. Không sửa trực tiếp tag reference candidate.
