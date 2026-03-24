# Dodge AI — Order-to-Cash graph + chat

SAP O2C **JSONL** is ingested into **SQLite**, exposed as an in-memory **graph** for visualization, and queried through a **FastAPI** backend plus **Groq** LLM chat. The **React** UI shows the graph and a sidebar chat; highlights on the graph come from structured tool/SQL results, not from prose in the model reply.

---

## Quick start

1. Put **`sap-o2c-data/`** at the repo root (JSONL per entity folder).

2. **Backend** — `cd backend`, create venv, `pip install -r requirements.txt`, copy `.env.example` → `.env`, set **`GROQ_API_KEY`**.

   ```bash
   .\.venv\Scripts\python -m uvicorn app.main:app --reload --host 127.0.0.1 --port 8080
   ```

   First run creates **`backend/data/sap_o2c.db`**. Rebuild: `POST /api/admin/reingest`.

3. **Frontend** — `cd frontend`, `npm install`, `npm run dev`. Vite proxies **`/api`** to **`http://127.0.0.1:8080`** (override with **`VITE_API_URL`** if needed).

On Windows, **port 8000** is often blocked (**WinError 10013**); **8080** is the default in this repo.

**Deploy (free):** see **[DEPLOY_RENDER.md](./DEPLOY_RENDER.md)** — Render Blueprint (`render.yaml`) runs **API + static frontend**; commit **`sap-o2c-data/`** (not ignored) and set **`GROQ_API_KEY`** on the service (never commit **`.env`**).

---

## Architecture (why it’s built this way)

| Piece | Decision | Rationale |
|--------|-----------|-----------|
| **API** | FastAPI + SQLite connection per app | Simple local app; async for LLM HTTP calls without blocking the event loop unnecessarily. |
| **Ingest** | One SQLite table per JSONL **folder**, columns from union of keys | Matches messy real exports; no hand-written DDL for every field. |
| **Graph** | Built **in memory** from SQLite on load | O2C relationships are explicit joins (order lines → deliveries → billing → journal). The graph is a **view** for exploration; **truth stays relational**. |
| **UI** | React + `react-force-graph-2d` | Interactive layout; chat and graph side by side. Highlights are **data-driven** (`highlight_node_ids`), not parsed from assistant text. |
| **LLM** | **Two calls** per user turn (planner → executor → answer) | Separates **routing** (tool vs SQL vs off-topic) from **wording**. Reduces hallucinated document numbers and keeps answers tied to retrieved JSON. |
| **Ranked “top N” UX** | Server derives **which graph nodes to highlight** from result rows | For “highest billing / top product” style answers, only **top-tier** rows (ties included) are highlighted—not every row in a long ranked list. **Trace** tools keep their own full highlight sets. |

---

## Database choice: SQLite

- **Single file**, no separate DB server—good for a demo, laptop demos, and reproducible ingest.
- **SQL** is ideal for ad-hoc O2C questions (filters, joins, aggregates) and matches how SAP-style data is usually reasoned about.
- Full result sets are returned (no artificial row cap in the guard); the graph API can still be heavy on very large datasets—this stack targets **moderate** JSONL volumes.

---

## LLM prompting strategy

1. **Planner (first model call)**  
   - System prompt lists **intents**: `off_topic`, `tool`, or `sql`.  
   - The live **table/column summary** from SQLite is appended so the model can ground SQL and tool choice in real schema.  
   - Recent chat turns are included for context, but each plan is still **one JSON object** (no markdown).

2. **Execution (no LLM)**  
   - **`tool`** → Python in **`analytics.py`** (traces, ranked lists, broken flows, etc.).  
   - **`sql`** → **`sql_guard.py`** validates and runs **SELECT** only.  
   - **Post-processing** (examples):  
     - If the user asked for a **billing end-to-end trace** but the planner used SQL only to **find** the billing document (e.g. highest amount), the server **chains** **`trace_billing_flow`** and adds **`billing_flow_traces`** to evidence so the answer model sees a full `path`, not just one SQL row.  
     - Highlights for aggregate/ranked SQL are reduced to **top-tier** matches where applicable.

3. **Answer (second model call)**  
   - Receives **`question`** + **`evidence`** only.  
   - Instructed to use **only** those facts, cite **business keys** in plain language, and use **Markdown** (no HTML).  
   - **`highlight_node_ids`** (and nested copies inside traces) are **stripped** from evidence before this call so the model does not echo internal graph ids or “turn on highlights” in text—the UI applies highlights from the API payload.

4. **Failure handling**  
   - Groq **connection/DNS/timeouts** are caught and returned as a normal chat **`reply`** (e.g. unreachable / check network) instead of a raw **500**, so the client always gets JSON.

---

## Guardrails

| Layer | What |
|--------|------|
| **Planner** | **`off_topic`** → fixed short message; no tool/SQL run. |
| **SQL** | **SELECT-only**; **single** statement; **no** DML/DDL/PRAGMA-style abuse; table names must be in the **allow-list** derived from the DB. |
| **Answer** | Must not invent IDs/amounts; must not discuss UI highlighting or internal node-id syntax (`billh:`, `soi:`, etc.). |
| **Transport** | Missing/invalid **API key** and **Groq** errors are surfaced as user-readable **`RuntimeError`** messages mapped to HTTP responses the frontend already handles. |

---

## API (short)

| Method | Path | Role |
|--------|------|------|
| GET | `/api/graph` | Nodes + edges for the force graph |
| GET | `/api/node/{id}` | Node metadata (modal) |
| POST | `/api/chat` | `{ "messages": [{ "role", "content" }] }` → `reply`, `highlight_node_ids`, `evidence`, … |
| GET | `/api/schema`, `/api/health` | Discovery / liveness |

---

## Troubleshooting

- **Port 8000 / WinError 10013** — Use **8080** (or set **`VITE_API_URL`** to match).  
- **Empty graph** — Check **`sap-o2c-data`**, delete **`sap_o2c.db`**, restart or **reingest**.  
- **Vite build / Rolldown on Windows** — Repo pins **Vite 5**; reinstall from **`package.json`** if you upgraded.

Default model is configured in **`backend/app/config.py`** (Groq OpenAI-compatible URL).
