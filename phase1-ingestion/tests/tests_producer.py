import json
import sys
import os
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../producer"))

from producer import make_transaction, on_delivery, DEVICES, USERS


class TestMakeTransaction:
    def test_required_fields_present(self):
        tx = make_transaction()
        required = [
            "transaction_id", "user_id", "amount", "currency",
            "merchant_id", "merchant_name", "country", "city",
            "device_id", "device_type", "event_time", "is_international", "metadata",
        ]
        for field in required:
            assert field in tx, f"Missing field: {field}"

    def test_amount_is_positive(self):
        for _ in range(50):
            assert make_transaction()["amount"] > 0

    def test_device_type_is_valid(self):
        for _ in range(50):
            assert make_transaction()["device_type"] in DEVICES

    def test_user_id_from_pool(self):
        for _ in range(50):
            assert make_transaction()["user_id"] in USERS

    def test_event_time_is_recent_epoch_ms(self):
        import time
        tx = make_transaction()
        now_ms = int(time.time() * 1000)
        assert tx["event_time"] <= now_ms
        assert tx["event_time"] > now_ms - 5000

    def test_metadata_is_dict(self):
        assert isinstance(make_transaction()["metadata"], dict)

    def test_is_international_is_bool(self):
        assert isinstance(make_transaction()["is_international"], bool)

    def test_transaction_ids_are_unique(self):
        ids = {make_transaction()["transaction_id"] for _ in range(100)}
        assert len(ids) == 100


class TestOnDelivery:
    def test_no_error_is_silent(self):
        on_delivery(None, MagicMock())  # should not raise

    def test_logs_on_error(self, caplog):
        import logging
        with caplog.at_level(logging.ERROR):
            on_delivery("broker unavailable", MagicMock())
        assert "delivery_failed" in caplog.text


class TestBuildSerializer:
    def test_loads_schema_file(self):
        schema_path = os.path.join(
            os.path.dirname(__file__), "../producer/schemas/transaction.avsc"
        )
        with open(schema_path) as f:
            schema = json.load(f)
        assert schema["name"] == "Transaction"
        assert schema["namespace"] == "com.fraudplatform"
        fields = {f["name"] for f in schema["fields"]}
        assert "transaction_id" in fields
        assert "event_time" in fields
        assert "amount" in fields