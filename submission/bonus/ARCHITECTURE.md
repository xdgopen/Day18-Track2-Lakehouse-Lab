# Lakehouse cho LLM observability 1B requests/ngày

**Tác giả:** Nguyễn Danh Thành — **MSSV:** 2A202600581  
**Topic:** A — LLM observability at 1B requests/day

## 1. Problem statement

Nền tảng foundation-model phục vụ **1 tỷ request/ngày**, trung bình 5 KB request/response: 5 TB/ngày raw, khoảng 150 TB/tháng trước replication. SRE và FinOps cần dashboard cost, p50/p95 latency, error rate theo tenant/model cập nhật tối đa 5 phút. Prompt/response đầy đủ chỉ giữ 7 ngày cho incident review; aggregate giữ một năm. PII phải được redact/tokenize trước khi analyst đọc dữ liệu. Tổng chi phí **storage** không vượt $5,000/tháng. Khó ở chỗ hot query phải nhanh trong khi raw payload lớn, retry tạo duplicate, model schema thay đổi và prompt rất nhạy cảm.

## 2. Architecture

```text
SDK/API gateway -> Kafka (tenant-hash partitions, schema registry)
       |  request_id, source offset, envelope encryption
       v
Bronze Delta: raw/ingest_date/hour [7d Standard, PII restricted]
  immutable JSON + tokenized tenant/user + checkpoint
       | micro-batch 60s; PII redactor; invalid -> quarantine
       v
Silver Delta: calls/event_date/hour [typed, dedup, CDF enabled]
  MERGE (tenant_id, request_id), schema validation, row/column policy
       | 5-minute aggregate                 | audit/lineage
       v                                    v
Gold Delta: tenant_model_5m        Catalog + OpenLineage + access audit
  p50/p95/cost/error; partition date, cluster tenant_id/model
       |
Trino/BI dashboard + SRE alerting

Lifecycle: Bronze delete day 8; Silver IA day 31; Gold 13 months then archive.
```

Bronze, Silver, Gold đều là Delta tables. Transaction log tạo ACID audit trail; catalog lưu ownership, classifications và grants; lineage nối gateway schema → job/version → dashboard.

## 3. Key decisions and rejected alternatives

### 3.1 Table format và write protocol

Tôi chọn **Delta Lake trên object storage**, streaming micro-batch 60 giây, ACID log, CDF ở Silver và time travel 7 ngày cho hot tables. Tôi loại **plain Parquet** vì concurrent writer, dedup MERGE, schema enforcement và rollback phải tự xây — rất rủi ro khi retry. Tôi loại **Iceberg** không vì nó kém: nếu multi-engine/catalog interop là mục tiêu số một thì Iceberg tốt; ở đây Spark/Trino cùng MERGE/CDF/runbook Delta có sẵn nên Delta giảm rủi ro vận hành.

### 3.2 Medallion, PII và retention

Tôi chọn **Bronze encrypted/restricted, redaction/tokenization trong ingestion trước Silver**. Prompt gốc chỉ tồn tại 7 ngày; Silver không có phone/email/IP thô, chỉ token ổn định để join. Tôi loại redact lúc analyst query vì một lỗi policy có thể lọt vào cache/export. Tôi loại xoá raw ngay sau parse vì incident review và reprocessing trong cửa sổ 7 ngày sẽ mất bằng chứng.

### 3.3 Physical layout

Tôi chọn partition Bronze/Silver theo `event_date/hour`; Gold theo `event_date`, compact file 256–512 MB và Z-order/cluster Gold theo `tenant_id, model`. Dashboard luôn filter tenant + time range nên file pruning giảm bytes scan. Tôi loại partition theo **tenant_id** vì high cardinality tạo small-file/skew. Tôi loại partition theo **minute** vì 1,440 partition/ngày làm metadata/maintenance đắt hơn latency đạt được.

### 3.4 Catalog, governance và lineage

Tôi chọn **Unity Catalog hoặc catalog tương đương có RBAC/audit** cùng **OpenLineage**; analyst bị masking prompt/token, SRE chỉ đọc Bronze theo incident ticket. Tôi loại IAM bucket policy đơn thuần vì không biểu đạt được column masking, ownership hoặc dashboard lineage. Tôi loại catalog tự phát của từng team vì không impact-analysis được khi đổi schema `model` hay xóa tenant.

### 3.5 Compression và lifecycle

Tôi chọn **ZSTD** cho Silver/Gold và raw Bronze nén ZSTD; lifecycle xóa Bronze ngày thứ 8, chuyển Silver Standard-IA sau 30 ngày, giữ Gold 13 tháng rồi archive. Tôi loại Snappy mọi nơi vì raw payload làm chi phí storage vượt cap. Tôi loại Glacier cho Silver đang phục vụ dashboard vì retrieval phá SLA 5 phút; Glacier chỉ hợp archive ngoài cửa sổ query.

### 3.6 Late events và retry

Tôi chọn `MERGE` Silver trên `(tenant_id, request_id)` với `src.event_ts >= tgt.event_ts`, watermark 48 giờ và checkpoint/source offset. Tôi loại append-only rồi `SELECT DISTINCT` trong dashboard vì cost bị double count mỗi retry. Tôi loại drop mọi event muộn vì mobile/region outage gây undercount; CDF kích hoạt recompute Gold 5-minute window.

## 4. Failure modes lúc 3 giờ sáng

| Failure | Detect | Contain / rollback |
|---|---|---|
| Redactor lỗi, PII vào Silver | DLP sample mỗi micro-batch; alert raw email/phone | Dừng Gold, revoke grants, `RESTORE` Silver về version tốt, sửa rule và replay Bronze 7 ngày; audit mọi read. |
| Producer đổi schema hoặc gửi latency string | Contract reject/quarantine/null-rate >0.1% | Không auto-merge field type; giữ Bronze immutable, version parser rồi replay quarantine. |
| Retry làm cost Gold tăng | Unique request_id/rows ratio, offset lag, cost anomaly | Idempotent MERGE và CDF recompute; release sai thì `RESTORE` Silver/Gold rồi replay checkpoint. |
| OPTIMIZE rewrite lớn làm dashboard chậm | Query p95, compaction backlog/file count | Optimize ngoài peak theo partition hôm qua; cancel job, đọc Gold version trước bằng time travel, retry partition nhỏ. |
| Grant nhầm Bronze | Catalog audit event + canary access test | Revoke role, rotate credential, dùng audit/lineage xác định phạm vi; 7-day retention giới hạn blast radius. |

`RESTORE` chỉ khôi phục table state; nó không thu hồi PII đã export. Revoke và evidence preservation phải chạy song song.

## 5. Back-of-the-envelope cost

Planning estimate (storage-only, giá rounded): 5 TB/ngày × 7 = **35 TB Bronze**. Silver typed/dedup nén khoảng 35% raw: 5 × 0.35 × 30 = **52.5 TB**. Gold + log hot 3 TB.

| Tier | Math | $/month |
|---|---:|---:|
| Bronze Standard, 7 ngày | 35 TB × $23/TB-month | $805 |
| Silver Standard-IA, 30 ngày | 52.5 TB × $12.5/TB-month | $656 |
| Silver prior 11 tháng archive | 577.5 TB × $1.5/TB-month | $866 |
| Gold + logs hot | 3 TB × $23/TB-month | $69 |
| PUT/LIST, retrieval, metadata buffer | reserve | $1,000 |
| **Total storage** |  | **$3,396/month** |

Estimate còn $1,604 dưới cap $5K. Compute không thuộc cap: 20 streaming worker trung bình $0.25/giờ + 10 worker compact 2 giờ/ngày ≈ **$3,750/tháng**. Nếu Silver 365 ngày ở Standard-IA, 577.5 TB × $12.5 = $7,219 riêng phần đó: lifecycle là bắt buộc, không phải tối ưu nhỏ.

## 6. MVP một tuần

Ngày 1–2 tạo catalog, Bronze Delta, envelope schema, RBAC và redactor/tokenizer fixture PII. Ngày 3 streaming 60 giây, checkpoint/idempotency/quarantine. Ngày 4 Silver MERGE dedup và Gold `(tenant, model, 5-minute)` cho một region/10 tenant pilot. Ngày 5 có dashboard p95/cost/error, DLP/lag/file-count alerts và runbook `RESTORE + replay`. Slice này chứng minh low-latency aggregate vẫn có lineage, PII boundary và rollback; multi-region, retention automation và chargeback để sprint sau.
