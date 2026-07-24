# Static Validation Report — Vr15

## Đã kiểm tra

- Biên dịch cú pháp toàn bộ 20 file Python: PASS.
- Kiểm tra cú pháp 7 shell script bằng `bash -n`: PASS.
- Import `queries.py` bằng PySpark stub: PASS.
- Registry có đúng 12 query Q1.1–Q4.3: PASS.
- Mỗi registry key khớp `QuerySpec.query_id`: PASS.
- Mỗi QuerySpec có đủ trường logic, Baseline plan, Proposed plan, automaton/stitching/constraint metadata: PASS.
- Không còn query ID/alias Q14 trong mã nguồn và script: PASS.
- Không còn biến `max_nexttw_hops` hoặc `derived_max_time_window_gap`: PASS.
- Mọi truy cập `config.<field>` đều tồn tại trong `Stage2Config`: PASS.
- Runner CLI sử dụng tên tham số Vr15: PASS.

## Chưa thể kiểm tra trong môi trường tạo file

Môi trường tạo artifact không có PySpark/Spark Standalone/HDFS, vì vậy chưa chạy được:

- Spark job thật trên cluster `spark://master:7077`;
- concurrent LocalEval trên executor;
- correctness witness equality trên dataset HDFS;
- runtime/timing invariants từ Spark action thật;
- khả năng tương thích hiệu năng với GraphFrames/Spark 3.2 cụ thể của cluster.

Do đó, trước khi chạy toàn bộ benchmark cần chạy lần lượt:

1. Q1.1 + dataset_1m + correctness.
2. Q4.3 + dataset_1m + correctness.
3. Q1.1 + dataset_1m + profiling_runtime_only.
4. Sau khi ba bước trên PASS mới chạy 12 query × 6 dataset × 3 repeats.
