#!/bin/sh
cd -- "$(dirname -- "$0")" || exit 1
RUNTIME_ROOT=${HB_RUNTIME_ROOT:-"$HOME/.cache/codex/hummingbot-robinhood-v217-9af100d"}
"$RUNTIME_ROOT/env/bin/python" bin/hbot stop
RESULT=$?
printf '\nПроверьте результат остановки выше. Остановка не закрывает позицию LIT.\nНажмите Enter, чтобы закрыть окно.\n'
read -r ANSWER
exit "$RESULT"
