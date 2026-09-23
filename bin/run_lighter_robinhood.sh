#!/bin/sh
set -eu

REPO_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
RUNTIME_ROOT=${HB_RUNTIME_ROOT:-/Users/mikhail/.cache/codex/hummingbot-robinhood-v217-9af100d}
PYTHON="$RUNTIME_ROOT/env/bin/python"
MAMBA="$RUNTIME_ROOT/tools/bin/micromamba"
ACTION=${1:-preflight}
CONFIG=${2:-$REPO_ROOT/conf/scripts/lighter_robinhood_neutral_grid.yml.example}

export MAMBA_ROOT_PREFIX="$RUNTIME_ROOT/mamba-root"
cd "$REPO_ROOT"

case "$ACTION" in
  preflight)
    exec "$PYTHON" bin/lighter_robinhood_preflight.py --public-only --config "$CONFIG"
    ;;
  start)
    "$PYTHON" bin/lighter_robinhood_preflight.py --public-only --config "$CONFIG"
    "$PYTHON" - "$CONFIG" <<'PY'
import sys
import yaml

with open(sys.argv[1], encoding="utf-8") as stream:
    config = yaml.safe_load(stream) or {}
if config.get("enabled") is not True:
    raise SystemExit("Refusing to start: set enabled: true deliberately after preflight.")
if config.get("lower_price") in (None, "") or config.get("upper_price") in (None, ""):
    raise SystemExit("Refusing to start: lower_price and upper_price are required.")
if config.get("margin_reserve_usdg") in (None, ""):
    raise SystemExit("Refusing to start: margin_reserve_usdg must be chosen explicitly.")
PY
    case "$CONFIG" in
      "$REPO_ROOT"/conf/scripts/*.yml) ;;
      *) echo "Refusing to start: config must be a .yml file in conf/scripts." >&2; exit 2 ;;
    esac
    exec "$MAMBA" run -p "$RUNTIME_ROOT/env" hbot start "$(basename -- "$CONFIG")" --v2-script
    ;;
  stop)
    exec "$MAMBA" run -p "$RUNTIME_ROOT/env" hbot stop
    ;;
  *)
    echo "Usage: $0 [preflight [config] | start config | stop]" >&2
    exit 2
    ;;
esac
