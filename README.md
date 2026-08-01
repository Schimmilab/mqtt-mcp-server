# mqtt-mcp-server

An MCP server for MQTT that **remembers**. It subscribes to a broker, stores every
message with a timestamp in SQLite, and lets an AI agent ask questions about the
past — not just about the next message that happens to arrive.

## Why another MQTT MCP server

The existing ones answer *"wait for the next message on this topic"*. That is the
wrong question for diagnosing home automation:

- A **battery-powered sensor** (Shelly H&T) sleeps for hours. Waiting for its next
  message means waiting for hours.
- A **broken device** sends nothing at all. Waiting tells you nothing — you need to
  know *when it last spoke*.
- **"No data" is ambiguous.** Was the device silent, or was the collector not
  listening? Without a record of connection state, both look identical.

This server answers all three.

## Tools

| Tool | Purpose |
|---|---|
| `list_topics(pattern, seit_stunden)` | Topic inventory — what exists, how many messages, last seen |
| `get_last(topic)` | Last known value **immediately**, no waiting |
| `get_history(topic, seit_stunden, limit)` | Timestamped history — the core feature |
| `get_tree(prefix, tiefe)` | Topic tree, like MQTT Explorer |
| `find_silent(still_seit_stunden)` | Topics that **stopped** reporting |
| `get_gaps(seit_stunden)` | When was the collector disconnected? |
| `status()` | Connection, data volume, write mode |
| `publish(topic, payload, qos, retain)` | Send a message — **disabled by default** |

### `find_silent` and `get_gaps` belong together

`find_silent` deliberately warns you when connection gaps exist in the queried
period. A topic can look "silent" simply because nobody was listening. The server
refuses to let you confuse the two.

## Install

```bash
git clone https://github.com/Schimmilab/mqtt-mcp-server.git
cd mqtt-mcp-server
python3 -m venv .venv
.venv/bin/pip install -e .
```

Register with Claude Code:

```bash
claude mcp add mqtt --scope user \
  --env MQTT_MCP_HOST=192.168.1.10 \
  -- /ABSOLUTE/PATH/TO/mqtt-mcp-server/.venv/bin/mqtt-mcp-server
```

A newly registered server is only picked up by a **new** session — MCP connections
are fixed at session start.

## Configuration

| Variable | Default | |
|---|---|---|
| `MQTT_MCP_HOST` | `localhost` | broker host |
| `MQTT_MCP_PORT` | `1883` | |
| `MQTT_MCP_USERNAME` / `_PASSWORD` | — | optional auth |
| `MQTT_MCP_TOPICS` | `#` | comma-separated subscription filters |
| `MQTT_MCP_DB` | `~/.local/share/mqtt-mcp/history.db` | |
| `MQTT_MCP_RETENTION_TAGE` | `30` | delete messages older than this |
| `MQTT_MCP_MAX_DB_MB` | `2048` | hard cap, triggers oldest-first deletion |
| `MQTT_MCP_ALLOW_PUBLISH` | `false` | **write mode** |
| `MQTT_MCP_BLOCKED_TOPICS` | see `config.py` | never published to, even in write mode |

## Writing is off by default — on purpose

`publish` requires **two** conditions: write mode enabled *and* the topic not on the
block list. The default block list covers power switches and device restarts.

> ⚠️ **The block list is a guard rail, not a security boundary.** Anyone with access
> to the server can change it. Real enforcement requires a broker-side ACL.

The default exists because of a real incident: a switched socket in front of two
servers failed and took the whole home automation down for nine days, costing seven
weeks of measurement data. Measuring is safe; switching is not.

## Retained messages

On connect, a broker delivers its entire retained backlog at once — potentially
tens of thousands of messages with **old content but a fresh arrival time**. Storing
those naively corrupts every history from the first second.

This server stores them, but flags them: `aus_startschwall: true`. `get_last` uses
them (that is how a sleeping sensor still has a value); `get_history` can exclude
them.

## Known limitations

**Broker authentication is implemented but untested against a real broker.**
`MQTT_MCP_USERNAME` / `MQTT_MCP_PASSWORD` are passed to `username_pw_set()`, and
unit tests verify they reach the client — but no authenticating broker was
available during development. If you use auth, verify it works before relying on it.

**Two behaviours are only covered by unit tests, not by integration tests:**
connection loss (`get_gaps`) and devices going quiet (`find_silent`). Both are hard
to trigger on demand without a controllable broker. A built-in traffic simulator is
the obvious fix and is planned.

**History only covers times when the server was running.** It is a debugging tool
started on demand, not a 24/7 collector. If something breaks while you are away and
no session is open, nothing is recorded.

## Retention

Runs at startup and hourly: delete older than N days, then — if still over the size
cap — delete oldest-first. **Every cleanup reports what it removed** to stderr,
including the oldest remaining timestamp. Silent deletion would quietly destroy the
answer to *"since when has this device been quiet?"*.

## License

MIT
