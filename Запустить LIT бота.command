#!/bin/zsh
set -u

REPO_DIR="${0:A:h}"
PYTHON="$HOME/.cache/codex/hummingbot-robinhood-v217-9af100d/env/bin/python"

cd "$REPO_DIR" || exit 1
if [[ ! -x "$PYTHON" ]]; then
  print "Не найдена подготовленная среда Python: $PYTHON"
  print "См. docs/lighter-robinhood-macos.md"
  print -n "Нажмите Enter, чтобы закрыть окно..."
  read
  exit 1
fi

"$PYTHON" -m bin.lighter_robinhood_setup
rc=$?
print
print "Мастер завершён (код $rc)."
print -n "Нажмите Enter, чтобы закрыть окно..."
read
exit $rc
