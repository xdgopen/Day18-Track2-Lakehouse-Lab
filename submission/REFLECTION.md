# Reflection — Nguyễn Danh Thành (2A202600581)

Anti-pattern mà team em có nguy cơ gặp nhất là **Bronze becomes the analytics table**. Raw LLM logs trông đã đủ nên dashboard rất dễ đọc thẳng Bronze để triển khai nhanh. Điều này làm chi phí scan lớn, schema JSON không ổn định, duplicate do retry làm sai cost/usage, và có thể lộ prompt chứa PII cho người chỉ cần aggregate.

Pipeline của lab tách trách nhiệm: Bronze là immutable landing/audit; Silver parse kiểu dữ liệu và deduplicate theo `request_id`; Gold giữ aggregate ngày × model. Vì vậy p50/p95, error rate và chi phí tính trên request duy nhất. Delta history/time travel giúp so sánh và rollback khi thay đổi logic thay vì ghi đè âm thầm. Với production, em sẽ thêm data contract ở Bronze, masking PII ở Silver và alert khi duplicate hoặc malformed record tăng đột ngột.
