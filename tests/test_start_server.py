from __future__ import annotations

import os
from types import SimpleNamespace

from ldaca_web_app import main


def test_start_server_background_creates_task(
    monkeypatch,
) -> None:
    monkeypatch.setattr(main, "_server", None)
    monkeypatch.setattr(main, "_server_task", None)
    monkeypatch.setattr(os, "environ", os.environ.copy())

    created_config: dict[str, object] = {}
    task_callbacks: list[object] = []

    class FakeTask:
        def add_done_callback(self, callback) -> None:
            task_callbacks.append(callback)

    task_marker = FakeTask()

    class FakeLoop:
        def create_task(self, coroutine):
            coroutine.close()
            return task_marker

    class FakeServer:
        def __init__(self, config) -> None:
            self.config = config
            self.started = False

        async def serve(self) -> None:
            return None

    def fake_config(app, host, port, reload, log_level):
        created_config.update(
            {
                "app": app,
                "host": host,
                "port": port,
                "reload": reload,
                "log_level": log_level,
            }
        )
        return SimpleNamespace(port=port)

    monkeypatch.setattr(main.uvicorn, "Config", fake_config)
    monkeypatch.setattr(main.uvicorn, "Server", FakeServer)
    monkeypatch.setattr(main.asyncio, "get_running_loop", lambda: FakeLoop())

    result = main.start_server(port=8123, frontend=False, background=True)

    assert result is task_marker
    assert created_config["port"] == 8123
    assert task_callbacks == [main._clear_server_state]


def test_start_server_background_reuses_existing_task(monkeypatch) -> None:
    existing_task = SimpleNamespace(done=lambda: False)

    monkeypatch.setattr(main, "_server", SimpleNamespace(started=False))
    monkeypatch.setattr(main, "_server_task", existing_task)
    monkeypatch.setattr(os, "environ", os.environ.copy())
    monkeypatch.setattr(
        main.uvicorn,
        "Config",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("unexpected")),
    )

    result = main.start_server(port=9000, frontend=False, background=True)

    assert result is existing_task
