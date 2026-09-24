"""Minimal Chrome DevTools Protocol driver for offline browser acceptance (Playwright is not in the env).

Launches a locally installed Chrome/Chromium headless with every non-loopback request blocked (dead proxy +
host resolver rules), attaches to one page over the DevTools websocket (aiohttp) and offers the handful of
operations the UI tests need: navigate, evaluate, keyboard, viewport and colour-scheme emulation.
Set ``NGWEB_CHROME`` to a browser binary to override discovery; tests skip when none is found.
"""
from __future__ import annotations

import asyncio
import base64
import glob
import itertools
import json
import os
import shutil
from pathlib import Path
from typing import Any, Dict, Optional

import aiohttp

_CANDIDATES = [
    os.environ.get("NGWEB_CHROME", ""),
    *sorted(glob.glob(os.path.expanduser(
        "~/Library/Caches/ms-playwright/chromium-*/chrome-mac*/Google Chrome for Testing.app/Contents/MacOS/"
        "Google Chrome for Testing")), reverse=True),
    *sorted(glob.glob(os.path.expanduser("~/.cache/ms-playwright/chromium-*/chrome-linux*/chrome")), reverse=True),
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    shutil.which("chromium") or "", shutil.which("google-chrome") or "",
]

_KEYS = {
    "Tab": (9, "Tab"), "Enter": (13, "Enter"), "Escape": (27, "Escape"), " ": (32, "Space"),
    "ArrowRight": (39, "ArrowRight"), "ArrowLeft": (37, "ArrowLeft"), "Home": (36, "Home"), "End": (35, "End"),
}


def find_chrome() -> Optional[str]:
    for candidate in _CANDIDATES:
        if candidate and Path(candidate).is_file() and os.access(candidate, os.X_OK):
            return candidate
    return None


class CdpError(RuntimeError):
    pass


class Browser:
    def __init__(self, proc, http: aiohttp.ClientSession, ws: aiohttp.ClientWebSocketResponse):
        self._proc = proc
        self._http = http
        self._ws = ws
        self._ids = itertools.count(1)
        self._pending: Dict[int, asyncio.Future] = {}
        self._events: Dict[str, list] = {}
        self._reader = asyncio.create_task(self._read())

    @classmethod
    async def launch(cls, binary: str, profile_dir: Path) -> "Browser":
        args = [
            binary, "--headless=new", "--remote-debugging-port=0", f"--user-data-dir={profile_dir}",
            "--no-first-run", "--no-default-browser-check", "--disable-background-networking",
            "--disable-component-update", "--disable-sync", "--disable-extensions", "--disable-default-apps",
            "--metrics-recording-only", "--no-pings", "--disable-features=Translate,OptimizationHints,MediaRouter",
            "--proxy-server=http://127.0.0.1:9", "--host-resolver-rules=MAP * ~NOTFOUND , EXCLUDE 127.0.0.1",
            "--window-size=1280,900", "about:blank",
        ]
        proc = await asyncio.create_subprocess_exec(*args, stdout=asyncio.subprocess.DEVNULL,
                                                    stderr=asyncio.subprocess.PIPE)
        ws_url = None
        try:
            while ws_url is None:
                line = await asyncio.wait_for(proc.stderr.readline(), 30)
                if not line:
                    raise CdpError("browser exited before DevTools was ready")
                text = line.decode(errors="replace").strip()
                if text.startswith("DevTools listening on "):
                    ws_url = text.split("DevTools listening on ", 1)[1]
        except BaseException:
            proc.kill()
            raise
        asyncio.create_task(_drain(proc.stderr))
        http = aiohttp.ClientSession()
        ws = await http.ws_connect(ws_url, max_msg_size=0)
        return cls(proc, http, ws)

    async def _read(self) -> None:
        async for msg in self._ws:
            if msg.type != aiohttp.WSMsgType.TEXT:
                continue
            data = json.loads(msg.data)
            if "id" in data and data["id"] in self._pending:
                fut = self._pending.pop(data["id"])
                if not fut.done():
                    if "error" in data:
                        fut.set_exception(CdpError(json.dumps(data["error"])))
                    else:
                        fut.set_result(data.get("result", {}))
            elif "method" in data:
                self._events.setdefault(data["method"], []).append(data)

    async def send(self, method: str, params: Optional[Dict[str, Any]] = None,
                   session_id: Optional[str] = None) -> Dict[str, Any]:
        msg_id = next(self._ids)
        fut = asyncio.get_running_loop().create_future()
        self._pending[msg_id] = fut
        payload: Dict[str, Any] = {"id": msg_id, "method": method, "params": params or {}}
        if session_id:
            payload["sessionId"] = session_id
        await self._ws.send_str(json.dumps(payload))
        return await asyncio.wait_for(fut, 30)

    async def new_page(self) -> "Page":
        target = await self.send("Target.createTarget", {"url": "about:blank"})
        attached = await self.send("Target.attachToTarget", {"targetId": target["targetId"], "flatten": True})
        page = Page(self, attached["sessionId"])
        await page.send("Page.enable")
        await page.send("Runtime.enable")
        return page

    async def close(self) -> None:
        try:
            await self.send("Browser.close")
        except Exception:  # noqa: BLE001
            pass
        self._reader.cancel()
        await self._ws.close()
        await self._http.close()
        try:
            await asyncio.wait_for(self._proc.wait(), 10)
        except asyncio.TimeoutError:
            self._proc.kill()


async def _drain(stream) -> None:
    while await stream.readline():
        pass


class Page:
    def __init__(self, browser: Browser, session_id: str):
        self._browser = browser
        self.session_id = session_id

    async def send(self, method: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return await self._browser.send(method, params, self.session_id)

    async def navigate(self, url: str) -> None:
        await self.send("Page.navigate", {"url": url})
        await self.wait_for("document.readyState === 'complete'")

    async def eval(self, expression: str) -> Any:
        result = await self.send("Runtime.evaluate", {"expression": expression, "awaitPromise": True,
                                                      "returnByValue": True})
        if "exceptionDetails" in result:
            raise CdpError(json.dumps(result["exceptionDetails"])[:500])
        return result.get("result", {}).get("value")

    async def wait_for(self, expression: str, timeout: float = 15.0, interval: float = 0.1) -> Any:
        deadline = asyncio.get_running_loop().time() + timeout
        last = None
        while asyncio.get_running_loop().time() < deadline:
            try:
                last = await self.eval(expression)
            except CdpError:
                last = None
            if last:
                return last
            await asyncio.sleep(interval)
        raise AssertionError(f"timeout waiting for: {expression} (last={last!r})")

    async def key(self, key: str) -> None:
        code, name = _KEYS[key]
        base = {"key": key, "code": name, "windowsVirtualKeyCode": code, "nativeVirtualKeyCode": code}
        text = {"Enter": "\r", " ": " "}.get(key)
        await self.send("Input.dispatchKeyEvent", dict(base, type="keyDown", **({"text": text} if text else {})))
        await self.send("Input.dispatchKeyEvent", dict(base, type="keyUp"))

    async def type(self, text: str) -> None:
        await self.send("Input.insertText", {"text": text})

    async def viewport(self, width: int, height: int, mobile: bool) -> None:
        await self.send("Emulation.setDeviceMetricsOverride", {"width": width, "height": height,
                                                               "deviceScaleFactor": 1, "mobile": mobile})

    async def color_scheme(self, scheme: str) -> None:
        await self.send("Emulation.setEmulatedMedia",
                        {"features": [{"name": "prefers-color-scheme", "value": scheme}]})

    async def screenshot(self, path: Path) -> None:
        shot = await self.send("Page.captureScreenshot", {"format": "png"})
        path.write_bytes(base64.b64decode(shot["data"]))


CONTRAST_JS = r"""
(() => {
  function rgb(s) { const m = s.match(/rgba?\(([^)]+)\)/); if (!m) return null;
    const p = m[1].split(',').map(x => parseFloat(x)); return {r: p[0], g: p[1], b: p[2], a: p.length > 3 ? p[3] : 1}; }
  function lum(c) { const f = v => { v /= 255; return v <= 0.03928 ? v / 12.92 : Math.pow((v + 0.055) / 1.055, 2.4); };
    return 0.2126 * f(c.r) + 0.7152 * f(c.g) + 0.0722 * f(c.b); }
  function bg(el) { while (el) { const c = rgb(getComputedStyle(el).backgroundColor); if (c && c.a > 0.5) return c;
    el = el.parentElement; } return rgb(getComputedStyle(document.body).backgroundColor); }
  function ratio(el) { const fg = rgb(getComputedStyle(el).color), b = bg(el); const l1 = lum(fg), l2 = lum(b);
    return (Math.max(l1, l2) + 0.05) / (Math.min(l1, l2) + 0.05); }
  const sel = SELECTORS;
  const out = {};
  sel.forEach(s => { const el = document.querySelector(s); if (el && el.offsetParent !== null) out[s] = Math.round(ratio(el) * 100) / 100; });
  return out;
})()
"""
