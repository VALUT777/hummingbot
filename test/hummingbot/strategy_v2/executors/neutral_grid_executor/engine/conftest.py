"""Engine tests never touch the real host-wide lock/marker directory (``~/.hummingbot/neutral_grid``)."""
import pytest


@pytest.fixture(autouse=True)
def _isolated_neutral_grid_host_dir(tmp_path, monkeypatch):
    host_dir = tmp_path / "ng-host"
    monkeypatch.setenv("HUMMINGBOT_NEUTRAL_GRID_HOST_DIR", str(host_dir))
    return host_dir
