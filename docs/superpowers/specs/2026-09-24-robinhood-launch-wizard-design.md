# Robinhood Lighter local launch wizard

The user has the API-key fields shown by Robinhood Lighter and wants to paste them into a simple local launcher. Extend the prepared native Mac installation with a Russian terminal wizard opened by one `.command` file. Reuse the existing connector, encrypted Hummingbot keystore and authenticated read-only preflight; no browser service or new secret store is needed.

The wizard asks for a Hummingbot storage password, exact Account Index and API Key Index, and a hidden API Private Key. Public Key is not required. Require confirmation that Maker Only is disabled, because service-operation compatibility has not been verified. Offer reuse of existing encrypted connector credentials without displaying secrets. Do not send secrets in argv, logs, source files or plaintext configuration. Refuse hidden input when no terminal is available.

Reject concurrent wizard instances with a local nonblocking lock. Do not replace existing valid stored credentials on a failed candidate check; persist only verified candidates, then reload to confirm identity and secret round-trip. Supply the Hummingbot storage password to the native launch command through its supported stdin mechanism, never through argv.

Ask locally for lower/upper LIT prices and explicit USDG reserve. Preserve the agreed LIT-USDG, 5× leverage, 1000 LIT maximum absolute net position and existing grid defaults. Validate the complete proposed configuration. A disabled working configuration is persisted atomically; a live-enabled candidate is used in memory for read-only preflight so readiness checks remain meaningful without enabling disk configuration prematurely.

Only after authenticated readiness succeeds, show account, range, position cap, leverage and margin requirement, and require an explicit local START action. Immediately before launch use the same credentials/configuration that passed checks, enable the configuration and start Hummingbot. Cancel, validation failure or launch failure must leave this wizard's configuration disabled. Do not overwrite a running bot's configuration or automatically replace an existing bot. Explain how to check status and stop, and that stopping retains position inventory.

Tests must exercise encrypted credential persistence, strict input handling, preflight gating, canceled/failed launches, exact runtime/config binding and secret redaction without real private API calls. Deliver a clickable local launcher and update the existing draft PR. No live exchange operation is performed by the development session.
