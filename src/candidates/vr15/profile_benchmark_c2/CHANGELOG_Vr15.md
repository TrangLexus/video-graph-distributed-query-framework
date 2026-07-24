# CHANGELOG Vr15

## Thay đổi bắt buộc đã thực hiện

1. Loại bỏ hoàn toàn alias/query ID ngoài bộ 12 query.
2. Chuẩn hóa `max_time_gap_seconds`; bỏ `max_nexttw_hops` khỏi code và output.
3. Đổi TW pruning thành `candidate_pruning_max_time_window_gap`.
4. Tách `baseline_physical_plan` và `proposed_physical_plan` trong `QuerySpec`.
5. Bổ sung automaton metadata, fragment roles, stitch mode/key, hard constraints và runtime parameter schema cho 12 query.
6. Sửa parsing TimeWindow để lấy số sau `TW`, không lấy nhóm số đầu trong temporal instance ID.
7. Tách `persist_df` khỏi `repartition_df`, loại hidden shuffle trong persistence.
8. Baseline materialize global graph và đo wall-clock Step 1/Step 2 bằng timer đúng phạm vi.
9. Proposed không union global base edges; chỉ tạo compact entity–partition index.
10. LocalEval được submit đồng thời cho toàn bộ graph partition bằng FAIR scheduler.
11. Runtime LocalEval chính thức là max completion offset từ cùng mốc bắt đầu.
12. Phân loại fragment thành complete-local, complete-and-boundary, incomplete-boundary và dead-end.
13. GlobalEval hợp nhất complete-local candidates với stitched candidates.
14. Final witness gồm locally validated results và stitched valid witnesses, sau deduplication.
15. Runner hỗ trợ correctness, profiling, runtime-only, repeats và kiểm tra timing invariants.
16. Cập nhật toàn bộ shell script sang tên tham số Vr15.

## Nội dung giữ nguyên

- Dataset/HDFS layout.
- 12 logical query semantics.
- Spark resource defaults của benchmark hiện tại.
- Bounded temporal joins và Hard Validation trong query evaluator.
- Baseline và Proposed dùng cùng logical QuerySpec và witness comparison key.
