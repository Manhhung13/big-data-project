# Container smoke tests

The smoke tests live in `scripts/smoke/` and run **only** inside the isolated compose project
`stockbatch-verify` (`docker/verify/docker-compose.verify.yml`: own volumes, no published ports):

- `scripts/smoke/smoke_hdfs.py` — create-exclusive / atomic replace through py4j on a real NameNode, seed → B1–B7 →
  verify → publish-baseline → export → crash between commit log and pointer → rollback → gc dry-run.
- `scripts/smoke/smoke_kafka.py` — produce `examples/KAFKA_PAYLOADS.jsonl` + malformed payloads (with headers) to a
  real broker, ingest twice, incremental ingest, rollback + re-ingest without duplicated offsets, B1/B2 decode.

Run everything (build, pytest, both smokes, teardown of the isolated project only):

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\verify_all.ps1
```

Results of the last recorded run: `docs/BUILD_AUDIT.md`.
