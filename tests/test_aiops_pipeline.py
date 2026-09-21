from pathlib import Path
import runpy
from src.anomaly_detector import AnomalyDetector
from src.aiops_pipeline import run_pipeline
from src.event_consumer import EventConsumer
from src.event_producer import EventProducer
from src.event_topic import EventTopic


def test_normal_record_is_not_anomaly():
    detector = AnomalyDetector()

    record = {
        "timestamp": "2026-09-20T10:00:00",
        "service": "payment-service",
        "response_time_ms": 120,
        "cpu_percent": 42,
        "memory_percent": 51,
        "log_level": "INFO",
        "message": "Payment request processed successfully"
    }

    assert detector.detect(record) is None


def test_anomalous_record_is_detected():
    detector = AnomalyDetector()

    record = {
        "timestamp": "2026-09-20T10:05:00",
        "service": "payment-service",
        "response_time_ms": 610,
        "cpu_percent": 75,
        "memory_percent": 70,
        "log_level": "ERROR",
        "message": "Payment service timeout"
    }

    event = detector.detect(record)

    assert event is not None
    assert event["type"] == "ANOMALY"


def test_producer_publishes_event():
    topic = EventTopic("anomaly-events")
    producer = EventProducer(topic)

    event = {
        "type": "ANOMALY",
        "service": "payment-service"
    }

    assert producer.publish(event)
    assert len(topic.get_messages()) == 1


def test_consumer_receives_event():
    topic = EventTopic("anomaly-events")
    producer = EventProducer(topic)
    consumer = EventConsumer(topic)

    event = {
        "type": "ANOMALY",
        "service": "payment-service"
    }

    producer.publish(event)

    messages = consumer.consume()

    assert len(messages) == 1

def test_pipeline_end_to_end():
    data_file = Path(__file__).resolve().parent.parent / "data" / "service_data.json"
    result = run_pipeline(str(data_file))

    assert result["records_processed"] > 0
    assert len(result["anomalies_detected"]) > 0
    assert result["events_consumed"] == result["anomalies_detected"]


def test_producer_ignores_empty_event():
    producer = EventProducer(EventTopic("anomaly-events"))
    assert producer.publish(None) is False


def test_topic_clear_empties_messages():
    topic = EventTopic("anomaly-events")
    topic.publish({"type": "ANOMALY"})
    topic.clear()
    assert topic.get_messages() == []
def test_pipeline_script_runs_as_main(monkeypatch, capsys):
    root = Path(__file__).resolve().parent.parent
    monkeypatch.chdir(root)
    monkeypatch.syspath_prepend(str(root / "src"))

    runpy.run_path(str(root / "src" / "aiops_pipeline.py"), run_name="__main__")

    output = capsys.readouterr().out
    assert "AIOps Pipeline Result" in output
    assert "Events consumed: 2" in output