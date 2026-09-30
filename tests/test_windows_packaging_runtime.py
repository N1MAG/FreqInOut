from __future__ import annotations

import os
from pathlib import Path
import runpy
import sys


ROOT = Path(__file__).resolve().parents[1]
HOOK = ROOT / "packaging" / "pyinstaller_runtime_qt.py"


def _run_hook(monkeypatch, tmp_path: Path, *, platform: str = "win32") -> tuple[Path, Path]:
    qml = tmp_path / "PySide6" / "Qt" / "qml"
    plugins = tmp_path / "PySide6" / "Qt" / "plugins"
    qml.mkdir(parents=True)
    plugins.mkdir(parents=True)
    monkeypatch.setattr(sys, "_MEIPASS", str(tmp_path), raising=False)
    monkeypatch.setattr(sys, "platform", platform)
    runpy.run_path(str(HOOK))
    return qml, plugins


def test_runtime_hook_sanitizes_external_python_and_qt_paths(monkeypatch, tmp_path: Path) -> None:
    external = {
        "QT_PLUGIN_PATH": "/host/qt/plugins",
        "QT_QPA_PLATFORM_PLUGIN_PATH": "/host/qt/platforms",
        "QML2_IMPORT_PATH": "/host/qt/qml",
        "QML_IMPORT_PATH": "/host/qml",
        "QTWEBENGINEPROCESS_PATH": "/host/QtWebEngineProcess",
        "PYTHONHOME": "/host/python",
        "PYTHONPATH": "/host/python/site-packages",
    }
    for key, value in external.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("FREQINOUT_ALLOW_EXTERNAL_RUNTIME_ENV", raising=False)

    qml, plugins = _run_hook(monkeypatch, tmp_path)

    assert os.environ["QML2_IMPORT_PATH"] == str(qml)
    assert os.environ["QT_PLUGIN_PATH"] == str(plugins)
    for key in external.keys() - {"QML2_IMPORT_PATH", "QT_PLUGIN_PATH"}:
        assert key not in os.environ
    assert set(os.environ["FREQINOUT_SANITIZED_ENV_VARS"].split(",")) == set(external)


def test_runtime_hook_applies_safe_windows_rendering_defaults(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("QT_OPENGL", raising=False)
    monkeypatch.delenv("QT_QUICK_BACKEND", raising=False)
    monkeypatch.setenv("QTWEBENGINE_CHROMIUM_FLAGS", "--no-sandbox")

    _run_hook(monkeypatch, tmp_path)

    assert os.environ["QT_OPENGL"] == "software"
    assert os.environ["QT_QUICK_BACKEND"] == "software"
    assert os.environ["QTWEBENGINE_CHROMIUM_FLAGS"].split().count("--disable-gpu") == 1
    assert "--no-sandbox" in os.environ["QTWEBENGINE_CHROMIUM_FLAGS"].split()


def test_runtime_hook_override_preserves_operator_environment(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("FREQINOUT_ALLOW_EXTERNAL_RUNTIME_ENV", "1")
    monkeypatch.setenv("PYTHONHOME", "/operator/python")
    monkeypatch.setenv("QT_PLUGIN_PATH", "/operator/plugins")

    _, plugins = _run_hook(monkeypatch, tmp_path)

    assert os.environ["PYTHONHOME"] == "/operator/python"
    assert os.environ["QT_PLUGIN_PATH"].split(os.pathsep) == [str(plugins), "/operator/plugins"]


def test_spec_disables_upx_and_main_exposes_packaged_smoke_diagnostics() -> None:
    spec = (ROOT / "FreqInOut.spec").read_text(encoding="utf-8")
    main = (ROOT / "freqinout" / "main.py").read_text(encoding="utf-8")

    assert spec.count("upx=False") == 2
    assert "packaging/pyinstaller_runtime_qt.py" in spec
    assert '"--smoke-test"' in main
    assert "_finish_packaged_smoke_test(lockfile)" in main
    assert "startup-error.log" in main


def test_packaged_smoke_completion_unlocks_and_exits_without_qt_teardown(monkeypatch) -> None:
    import freqinout.main as main_module

    calls: list[object] = []

    class Lock:
        def unlock(self) -> None:
            calls.append("unlock")

    monkeypatch.setattr(
        main_module,
        "shutdown_perf_metrics",
        lambda *, timeout: calls.append(("shutdown", timeout)),
    )
    monkeypatch.setattr(main_module.os, "_exit", lambda code: calls.append(("exit", code)))

    main_module._finish_packaged_smoke_test(Lock())

    assert calls == ["unlock", ("shutdown", 1.0), ("exit", 0)]


def test_spec_packages_all_operator_runtime_data_without_js8net_examples() -> None:
    spec = (ROOT / "FreqInOut.spec").read_text(encoding="utf-8")

    for required_path in (
        "config/leaflet",
        "config/net_resources",
        "config/propagation",
        "config/resource_catalog",
        "config/shortwave/eibi",
        "docs/guide.html",
        "third_party/js8net/js8net-main/js8net.py",
    ):
        assert required_path in spec
    assert "('third_party/js8net', 'third_party/js8net')" not in spec
    assert "third_party/js8net/js8net-main/example.py" not in spec
    assert "third_party/js8net/js8net-main/README.md" not in spec


def test_logger_tolerates_windowed_runtime_without_stdout(monkeypatch) -> None:
    from freqinout.core import logger as logger_module

    monkeypatch.setattr(sys, "stdout", None)
    assert logger_module._supports_color() is False

    isolated = logger_module.setup_logger(
        "freqinout.tests.windowed-runtime",
        log_to_console=True,
    )
    assert all(not isinstance(handler, __import__("logging").StreamHandler) or hasattr(handler, "baseFilename") for handler in isolated.handlers)
