# SPEC-FAIR — Công bằng Baseline và Proposed

## 1. Trường phải bằng nhau

- Dataset root/snapshot.
- Query ID và parameters.
- Spark master.
- Driver memory.
- Executor memory.
- Executor cores.
- Cores max.
- Shuffle partitions.
- Timeout/retry.
- Validation policy.
- Normalization.
- Final action policy.
- Repeats/warm-up.
- Cache policy tương đương hoặc có giải trình.

## 2. Khác biệt được phép

- Physical plan.
- LocalEval.
- Fragment generation.
- Stitching.
- Communication pattern.
- Cấu trúc trung gian thuộc thuật toán.

## 3. Khác biệt không hợp lệ điển hình

- Một bên cache, một bên không.
- Timer bao phủ normalization ở một bên nhưng không ở bên kia.
- Một bên bỏ hard validation.
- Một bên dùng ít dữ liệu hoặc điều kiện lọc mạnh hơn.
- Một bên dùng wall-clock, bên kia dùng max partition estimate nhưng cùng nhãn runtime.
