# Member 2 — Apache Kafka & Streaming Infrastructure

See [`kafka_infra/healthcheck.py`](../kafka_infra/healthcheck.py) for the Python healthcheck & topic management implementation (named `kafka_infra` so it does not shadow the `kafka-python` library's `kafka` module import), and [`init-topics.sh`](./init-topics.sh) for container startup topic initialization.
