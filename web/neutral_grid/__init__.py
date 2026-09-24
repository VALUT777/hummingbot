"""Local web UI/API adapter for the neutral fixed-cell grid (spec NG-UI-001..003).

The backend is a thin boundary: it reads committed, versioned snapshots and enqueues commands for the
single engine. The browser never receives credentials, never talks to the exchange and never writes
engine state directly.
"""
