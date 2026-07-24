# Đối chiếu thiết kế Vr15 C2 và độ phức tạp

## Bất biến logic

```text
C_L = Deduplicate(ConvertToCandidates(F_C))
C_S = Deduplicate(Stitch(F_B,Q))
C_M = Deduplicate(C_L ∪ C_S)
W_V = HardValidate(C_M,Q)
W_F = Deduplicate(Normalize(W_V))
```

Trong code:

- `complete_local_candidates` biểu diễn 1-1 các complete-local accepting fragments và C_L.
- `stitched_candidates` là C_S.
- `merged_candidates` là C_M.
- `valid_before_normalization` là W_V.
- `final_valid_witnesses` là W_F.

## True boundary

Mỗi interface gồm:

```text
left_role
right_role
left_key_fields
right_key_fields
temporal_mode
left_exit_state
right_entry_state
```

Boundary pair chỉ tồn tại khi:

```text
role/interface compatible
∧ binding key compatible
∧ automaton state path compatible
∧ partition khác nhau
∧ logical fragment khác nhau
∧ không phải replicated same-role temporal instance
∧ 0 ≤ Δtw ≤ K
∧ 0 ≤ Δt ≤ max_time_gap_seconds
```

## Tối ưu

1. Bounded equi-join thay full OR self-join.
2. Chỉ projection các cột cần cho boundary join.
3. K=12 được dùng để tạo target-TimeWindow key hữu hạn.
4. Typed-identity support dùng left-semi join.
5. Không dựng entity index O(|V|) không được sử dụng.
6. Không union fragment schema rộng chỉ để tính metric trong runtime-only.
7. Một aggregate action cho mỗi phase boundary.
8. Persist output phase để Step 2 không chạy lại LocalEval lineage.
9. Candidate ID giữ evidence path; witness dedup thực hiện sau normalization.
10. Diagnostic count bị tắt trong profiling_runtime_only.

## Giới hạn tối ưu

Không thể chứng minh một triển khai là “nhỏ nhất tuyệt đối” trên mọi phân bố dữ liệu và mọi Spark physical plan. Bundle bảo đảm cận trên tốt hơn full self-join và loại các công việc O(|V|) không cần thiết. Khi số cặp hợp lệ J rất lớn, mọi thuật toán giữ đầy đủ kết quả đều phải trả ít nhất Ω(J) thời gian/không gian đầu ra.
