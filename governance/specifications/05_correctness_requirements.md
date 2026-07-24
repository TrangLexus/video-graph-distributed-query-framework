# SPEC-COR — Yêu cầu correctness

| ID | Yêu cầu | Bắt buộc |
|---|---|---|
| COR-01 | Baseline hoàn tất với exit code 0 | Có |
| COR-02 | Proposed hoàn tất với exit code 0 | Có |
| COR-03 | Cùng dataset snapshot | Có |
| COR-04 | Cùng query parameters | Có |
| COR-05 | Normalized witness set bằng nhau | Có |
| COR-06 | Missing/timeout/exception không được coi là PASS | Có |
| COR-07 | Duplicate handling đồng nhất | Có |
| COR-08 | Temporal consistency PASS | Có |
| COR-09 | `require-nonzero` được thực thi khi bật | Có |
| COR-10 | Query registry đầy đủ | Có |
| COR-11 | Output schema đúng | Có |
| COR-12 | Gate JSON phản ánh tất cả query | Có |

## Correctness gate

Gate cuối phải chứa:

- Danh sách query dự kiến.
- Danh sách query đã chạy.
- PASS/FAIL theo query.
- Lý do FAIL.
- Candidate/witness diagnostics.
- Đường dẫn log.
- Commit/config/dataset identity.
