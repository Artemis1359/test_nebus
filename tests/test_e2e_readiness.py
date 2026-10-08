import httpx
import pytest
from test_e2e import _wait_for_consumer


def test_readiness_waits_for_queues_and_consumer(monkeypatch):
    polls = 0

    def management(request):
        nonlocal polls
        if request.url.path.endswith("payments.new"):
            polls += 1
        if polls == 1:
            return httpx.Response(404)
        return httpx.Response(200, json={"consumers": 0 if polls == 2 else 1})

    monkeypatch.setattr("test_e2e.time.sleep", lambda delay: None)
    with httpx.Client(
        transport=httpx.MockTransport(management), base_url="http://rabbit"
    ) as client:
        _wait_for_consumer(client)
    assert polls == 3


def test_missing_queues_report_readiness_error():
    with httpx.Client(
        transport=httpx.MockTransport(lambda request: httpx.Response(404)),
        base_url="http://rabbit",
    ) as client:
        with pytest.raises(pytest.fail.Exception, match="payments.new=404"):
            _wait_for_consumer(client, timeout=0)


def test_authentication_error_fails_without_waiting():
    with httpx.Client(
        transport=httpx.MockTransport(lambda request: httpx.Response(401)),
        base_url="http://rabbit",
    ) as client:
        with pytest.raises(pytest.fail.Exception, match="authentication failed"):
            _wait_for_consumer(client)
