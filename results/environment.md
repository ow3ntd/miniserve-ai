# Benchmark environment

All benchmark results in this directory were measured on this machine
unless an entry states otherwise.

| Item | Value |
|---|---|
| CPU | Apple M4 |
| Cores | 10 |
| RAM | 24 GB |
| OS | macOS 26.2 |
| Python | Python 3.12.0 |
| Key packages | fastapi==0.139.0 httpx==0.28.1 torch==2.13.0 uvicorn==0.51.0  |
| Server invocation | uvicorn app.main:app --host 127.0.0.1 --port 8000 --no-access-log |
| Load generator | load_tests/load_test.py, closed-loop |
