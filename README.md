# GitHub Challenge

<img src="https://octodex.github.com/images/Professortocat_v2.png" align="right" height="200px" />

Hey there!

Your challenge is ready.
Follow the instructions provided for this challenge and complete the required tasks in this repository.

Make sure your work is committed and pushed to your repository before submission.

Good luck!


---

&copy; 2025 GitHub &bull; [Code of Conduct](https://www.contributor-covenant.org/version/2/1/code_of_conduct/code_of_conduct.md) &bull; [MIT License](https://gh.io/mit)

# AIOps Simulation: Anomaly Detection and Event Flow

Repo: https://github.com/JustPrashansa/github-skills-challenge

## 1. Scenario

The service being monitored is `payment-service`. Every minute it produces metrics (response time, CPU, memory) and a log line (level + message).

The problem: nobody can watch all of this by hand. When the service gets slow or throws errors, the team should be told automatically.

In this assessment AIOps means: check the data automatically, turn every problem into an event, and pass that event through producer -> topic -> consumer to a final output. It is a small Python simulation, not real Kafka or Airflow.

```
Operational Data -> Anomaly Detection -> Event -> Producer -> Topic -> Consumer -> AIOps Output
```

## 2. Files

| Part | File |
|------|------|
| Data | `data/service_data.json` |
| Anomaly detection + event creation | `src/anomaly_detector.py` |
| Producer | `src/event_producer.py` |
| Topic | `src/event_topic.py` |
| Consumer | `src/event_consumer.py` |
| Pipeline / final output | `src/aiops_pipeline.py` |
| Tests | `tests/test_aiops_pipeline.py`, `tests/calculations_test.py` |

## 3. Data

10 records, one per minute, from 10:00 to 10:09 on 2026-09-20.

- Metrics: `response_time_ms`, `cpu_percent`, `memory_percent`
- Log fields: `log_level`, `message`
- Other: `service`, `timestamp` (the detector only copies the timestamp into the event so we know when it happened)

| Time | Response ms | CPU % | Memory % | Level | Message |
|------|-------------|-------|----------|-------|---------|
| 10:00 | 120 | 42 | 51 | INFO | Payment request processed successfully |
| 10:01 | 135 | 45 | 53 | INFO | Payment request processed successfully |
| 10:02 | 128 | 44 | 52 | INFO | Payment request processed successfully |
| 10:03 | 142 | 48 | 55 | INFO | Payment request processed successfully |
| 10:04 | 130 | 46 | 54 | INFO | Payment request processed successfully |
| 10:05 | 610 | 75 | 70 | ERROR | Payment service timeout |
| 10:06 | 640 | 94 | 91 | ERROR | Database connection timeout |
| 10:07 | 150 | 49 | 56 | INFO | Payment request processed successfully |
| 10:08 | 138 | 47 | 55 | INFO | Payment request processed successfully |
| 10:09 | 145 | 50 | 57 | INFO | Payment request processed successfully |

## 4. What I noticed

- Normal: 8 records with response time 120-150 ms, CPU 42-50%, memory 51-57% and INFO logs.
- Unusual: 10:05 and 10:06. Response time jumps to about 600 ms and both logs are ERROR. At 10:06 CPU (94%) and memory (91%) also spike. At 10:07 everything is back to normal.
- At 10:05 CPU and memory are still under 80%, so that record is only caught by response time and the error log.

## 5. Anomaly detection results

Run with `python src/aiops_pipeline.py`. The detector uses fixed rules (flag when the value is greater than the threshold):

- response time > 500 ms
- CPU > 80%
- memory > 80%
- log level is ERROR

| Time | Reasons |
|------|---------|
| 10:05 | High response time, Error log detected |
| 10:06 | High response time, High CPU utilization, High memory utilization, Error log detected |

- Missed anomalies: none.
- Normal records flagged by mistake: none (all 8 stay under every threshold).
- Limitation: the thresholds are fixed numbers, so they do not adapt to what is normal for the service. A rolling average with standard deviation would be better. Also only the exact level `ERROR` is checked.

## 6. Event flow

1. `AnomalyDetector.detect()` checks a record and returns an event, or `None` if it is normal.
2. `EventProducer.publish()` sends the event to the topic.
3. `EventTopic("anomaly-events")` keeps the events in a list.
4. `EventConsumer.consume()` reads the events from that same topic.
5. `run_pipeline()` returns the result and the script prints service, timestamp, type and reasons.

Event = the message about the anomaly. Producer = publishes it. Topic = holds it. Consumer = receives and processes it.

Example event (10:06):

```json
{
  "timestamp": "2026-09-20T10:06:00",
  "service": "payment-service",
  "type": "ANOMALY",
  "reasons": ["High response time", "High CPU utilization", "High memory utilization", "Error log detected"],
  "source": {"response_time_ms": 640, "cpu_percent": 94, "memory_percent": 91, "log_level": "ERROR", "message": "Database connection timeout"}
}
```

## 7. Issues found and fixed

Before fixing, the run showed 2 anomalies detected but 0 events consumed:

```text
Records processed: 10
Anomalies detected: 2
Events consumed: 0
```

The 8 existing tests passed even then, so the problems only showed up when running the whole pipeline.

| # | File | Problem | Cause | Fix |
|---|------|---------|-------|-----|
| 1 | `src/anomaly_detector.py` | "Error log detected" never appeared | Code checked for `WARNING`, but the data only has `INFO` and `ERROR` | Check `log_level == "ERROR"` |
| 2 | `src/aiops_pipeline.py` | Producer used the topic name `service-events` | Wrong topic name | Renamed to `anomaly-events` |
| 3 | `src/aiops_pipeline.py` | 0 events consumed | Consumer was on a different, empty topic object | Producer and consumer now share one `EventTopic("anomaly-events")` |

Tests: the disabled Fibonacci test expected `fib(10) == 89`, but it is 55, so I re-enabled it with 55. I also added tests for negative inputs, the empty event, the topic `clear()` and a full pipeline test.

## 8. Final run

`python src/aiops_pipeline.py`

```text
==================================================
AIOps Pipeline Result
==================================================
Records processed: 10
Anomalies detected: 2
Events consumed: 2

Detected Events:

Service: payment-service
Timestamp: 2026-09-20T10:05:00
Type: ANOMALY
Reasons: High response time, Error log detected

Service: payment-service
Timestamp: 2026-09-20T10:06:00
Type: ANOMALY
Reasons: High response time, High CPU utilization, High memory utilization, Error log detected
```

All 10 records were processed, 2 anomalies were found, and both events went through the producer, topic and consumer to the output.

## 9. Test results

`python -m pytest --cov=src --verbose`
```text
============================= test session starts ==============================
platform linux -- Python 3.13.15, pytest-8.4.1, pluggy-1.6.0 -- /workspaces/github-skills-challenge/.venv/calculations/bin/python
cachedir: .pytest_cache
rootdir: /workspaces/github-skills-challenge
plugins: cov-7.1.0
collecting ... collected 15 items

tests/calculations_test.py::test_area_of_circle_positive_radius PASSED   [  6%]
tests/calculations_test.py::test_area_of_circle_zero_radius PASSED       [ 13%]
tests/calculations_test.py::test_get_nth_fibonacci_zero PASSED           [ 20%]
tests/calculations_test.py::test_get_nth_fibonacci_one PASSED            [ 26%]
tests/calculations_test.py::test_get_nth_fibonacci_ten PASSED            [ 33%]
tests/calculations_test.py::test_area_of_circle_negative_radius PASSED   [ 40%]
tests/calculations_test.py::test_get_nth_fibonacci_negative PASSED       [ 46%]
tests/test_aiops_pipeline.py::test_normal_record_is_not_anomaly PASSED   [ 53%]
tests/test_aiops_pipeline.py::test_anomalous_record_is_detected PASSED   [ 60%]
tests/test_aiops_pipeline.py::test_producer_publishes_event PASSED       [ 66%]
tests/test_aiops_pipeline.py::test_consumer_receives_event PASSED        [ 73%]
tests/test_aiops_pipeline.py::test_pipeline_end_to_end PASSED            [ 80%]
tests/test_aiops_pipeline.py::test_producer_ignores_empty_event PASSED   [ 86%]
tests/test_aiops_pipeline.py::test_topic_clear_empties_messages PASSED   [ 93%]
tests/test_aiops_pipeline.py::test_pipeline_script_runs_as_main PASSED   [100%]

================================ tests coverage ================================
_______________ coverage: platform linux, python 3.13.15-final-0 _______________

Name                      Stmts   Miss  Cover   Missing
-------------------------------------------------------
src/aiops_pipeline.py        36      0   100%
src/anomaly_detector.py      18      0   100%
src/calculations.py          16      0   100%
src/event_consumer.py         6      0   100%
src/event_producer.py         9      0   100%
src/event_topic.py           10      0   100%
-------------------------------------------------------
TOTAL                        95      0   100%
============================== 15 passed in 0.19s ==============================
```

## 10. How to reproduce

```bash
git clone https://github.com/JustPrashansa/github-skills-challenge.git
cd github-skills-challenge
python -m venv .venv/calculations
source .venv/calculations/bin/activate
pip install -r requirements.txt
pip install pytest coverage pytest-cov

python src/aiops_pipeline.py          
python -m pytest --cov=src --verbose   
```

Expected: 10 records processed, 2 anomalies, 2 events consumed, all tests passing.

## 11. Limitation

Fixed thresholds and an exact `ERROR` match will not keep up with a changing baseline or new log wording. A statistical baseline would reduce misses and false alarms.