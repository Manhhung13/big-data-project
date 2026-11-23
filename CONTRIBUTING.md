# Contributing

Four workstream owners: (1) Platform & Kafka/HDFS, (2) Silver & reference, (3) Gold B3–B5, (4) Events B6/B7 & QA. Before pull request: format/lint as chosen by team; `pytest tests/unit tests/contract tests/integration`, update `contracts/schemas` and ADR on breaking changes, and run 5-day synthetic smoke.

All changes to date/time, unit, alias, baseline horizon, anomaly rules, partition identity, and published schema require version bumps and regression tests. Never commit licensed news content, provider credentials, customer/ticker private information or production Kafka secrets.
