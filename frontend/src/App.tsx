import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import ForceGraph2D from 'react-force-graph-2d'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import './App.css'

type GraphNode = { id: string; label?: string; group?: string; x?: number; y?: number }
type GraphLink = { source: string | GraphNode; target: string | GraphNode; label?: string }

function linkEndId(x: string | GraphNode): string {
  if (typeof x === 'string') return x
  return x.id
}

type ChatMsg = {
  role: 'user' | 'assistant'
  content: string
  /** From API highlight_node_ids; assistant messages only */
  graphHighlightIds?: string[]
  /** When true, these ids are merged into graph highlights with other toggled replies */
  showGraphHighlights?: boolean
}

/** Dev: relative `/api` (Vite proxy). Production: set `VITE_API_BASE_URL` at build time to backend origin, e.g. `https://your-api.onrender.com` */
const API = (() => {
  const raw = import.meta.env.VITE_API_BASE_URL as string | undefined
  const base = raw?.trim().replace(/\/$/, '')
  return base ? `${base}/api` : '/api'
})()
const CHAT_HISTORY_KEY = 'dodge-o2c-chat-messages'
const MAX_STORED_MESSAGES = 300

const DEFAULT_CHAT_MESSAGES: ChatMsg[] = [
  {
    role: 'assistant',
    content:
      'Hi! I can help you analyze the Order to Cash process. Try asking about billing documents, journal entries, product billing counts, or broken flows.',
  },
]

function loadChatHistory(): ChatMsg[] {
  try {
    const raw = localStorage.getItem(CHAT_HISTORY_KEY)
    if (!raw) return DEFAULT_CHAT_MESSAGES
    const parsed = JSON.parse(raw) as unknown
    if (!Array.isArray(parsed) || parsed.length === 0) return DEFAULT_CHAT_MESSAGES
    const out: ChatMsg[] = []
    for (const item of parsed) {
      if (!item || typeof item !== 'object') continue
      const role = (item as { role?: string }).role
      const content = (item as { content?: unknown }).content
      if (role !== 'user' && role !== 'assistant') continue
      if (typeof content !== 'string') continue
      const gh = (item as { graphHighlightIds?: unknown }).graphHighlightIds
      const graphHighlightIds = Array.isArray(gh)
        ? gh.filter((x): x is string => typeof x === 'string' && x.length > 0)
        : undefined
      const sgh = (item as { showGraphHighlights?: unknown }).showGraphHighlights
      if (graphHighlightIds?.length) {
        out.push({
          role,
          content,
          graphHighlightIds,
          showGraphHighlights: typeof sgh === 'boolean' ? sgh : true,
        })
      } else {
        out.push({ role, content })
      }
    }
    if (!out.length) return DEFAULT_CHAT_MESSAGES
    return out.length > MAX_STORED_MESSAGES ? out.slice(-MAX_STORED_MESSAGES) : out
  } catch {
    return DEFAULT_CHAT_MESSAGES
  }
}

const OFFLINE_MSG =
  'No internet connection. Check your network and try again.'

function isLikelyOffline(): boolean {
  return typeof navigator !== 'undefined' && !navigator.onLine
}

/** LLMs often emit HTML line breaks; react-markdown shows them as literal text unless normalized. */
function normalizeAssistantMarkdown(raw: string): string {
  let s = raw
  s = s.replace(/<br\s*\/?>/gi, '\n')
  s = s.replace(/<\/?p>/gi, '\n')
  return s
    .split('\n')
    .map((line) => line.trimEnd())
    .join('\n')
    .trim()
}

function AssistantContent({ text }: { text: string }) {
  const md = normalizeAssistantMarkdown(text)
  return (
    <div className="md-root">
      <ReactMarkdown remarkPlugins={[remarkGfm]}>{md}</ReactMarkdown>
    </div>
  )
}

function GraphHighlightGlyph({ on }: { on: boolean }) {
  return (
    <svg
      className="graph-highlight-glyph"
      width="14"
      height="14"
      viewBox="0 0 16 16"
      aria-hidden
    >
      <circle
        cx="8"
        cy="8"
        r="5.5"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.25"
        opacity={on ? 1 : 0.4}
      />
      <circle cx="8" cy="8" r="2.25" fill="currentColor" opacity={on ? 1 : 0.35} />
    </svg>
  )
}

export default function App() {
  const fgRef = useRef<any>(null)
  const graphWrapRef = useRef<HTMLDivElement>(null)
  const [dims, setDims] = useState({ w: 800, h: 600 })
  const [graphData, setGraphData] = useState<{ nodes: GraphNode[]; links: GraphLink[] }>({
    nodes: [],
    links: [],
  })
  const [totals, setTotals] = useState<{ nodes: number; edges: number } | null>(null)
  const [detail, setDetail] = useState<Record<string, string>>({})
  const [detailOpen, setDetailOpen] = useState(false)
  const [detailLoading, setDetailLoading] = useState(false)
  const [detailTitle, setDetailTitle] = useState('')
  /** 0–1 for pulsing ring around highlighted nodes */
  const [highlightPulse, setHighlightPulse] = useState(0)
  const [granular, setGranular] = useState(true)

  const [messages, setMessages] = useState<ChatMsg[]>(loadChatHistory)
  const chatMessagesRef = useRef<HTMLDivElement>(null)
  const chatScrollDidInitial = useRef(false)
  const [input, setInput] = useState('')
  const [chatBusy, setChatBusy] = useState(false)
  const [chatWidth, setChatWidth] = useState(() => {
    try {
      const v = localStorage.getItem('dodge-chat-width')
      const n = v ? parseInt(v, 10) : 380
      return Number.isFinite(n) && n >= 260 ? n : 380
    } catch {
      return 380
    }
  })
  const resizeRef = useRef<{ startX: number; startW: number } | null>(null)
  const widthToPersistRef = useRef<number | null>(null)
  const splitterRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const onMove = (e: MouseEvent) => {
      const r = resizeRef.current
      if (!r) return
      // Splitter is on the chat’s left edge: drag left → wider chat, drag right → narrower.
      const delta = r.startX - e.clientX
      const maxW = Math.floor(window.innerWidth * 0.58)
      const next = Math.min(Math.max(r.startW + delta, 260), maxW)
      widthToPersistRef.current = next
      setChatWidth(next)
    }
    const onUp = () => {
      if (resizeRef.current && widthToPersistRef.current != null) {
        try {
          localStorage.setItem('dodge-chat-width', String(widthToPersistRef.current))
        } catch {
          /* ignore */
        }
      }
      widthToPersistRef.current = null
      resizeRef.current = null
      document.body.classList.remove('col-resizing')
      splitterRef.current?.classList.remove('dragging')
    }
    window.addEventListener('mousemove', onMove)
    window.addEventListener('mouseup', onUp)
    return () => {
      window.removeEventListener('mousemove', onMove)
      window.removeEventListener('mouseup', onUp)
    }
  }, [])

  useEffect(() => {
    try {
      const slice =
        messages.length > MAX_STORED_MESSAGES
          ? messages.slice(-MAX_STORED_MESSAGES)
          : messages
      localStorage.setItem(CHAT_HISTORY_KEY, JSON.stringify(slice))
    } catch {
      try {
        localStorage.setItem(
          CHAT_HISTORY_KEY,
          JSON.stringify(messages.slice(-80)),
        )
      } catch {
        /* quota or private mode */
      }
    }
  }, [messages])

  useEffect(() => {
    const el = chatMessagesRef.current
    if (!el) return
    const instant = !chatScrollDidInitial.current
    chatScrollDidInitial.current = true
    requestAnimationFrame(() => {
      requestAnimationFrame(() => {
        el.scrollTo({
          top: el.scrollHeight,
          behavior: instant ? 'auto' : 'smooth',
        })
      })
    })
  }, [messages])

  const loadGraph = useCallback(async () => {
    const r = await fetch(`${API}/graph`)
    if (!r.ok) throw new Error('Failed to load graph')
    const j = await r.json()
    const nodes = (j.nodes as GraphNode[]).map((n) => ({ ...n }))
    const links = (j.edges as GraphLink[]).map((e) => ({
      source: e.source,
      target: e.target,
      label: e.label,
    }))
    setGraphData({ nodes, links })
    setTotals(j.totals)
  }, [])

  useEffect(() => {
    loadGraph().catch(console.error)
  }, [loadGraph])

  useEffect(() => {
    const el = graphWrapRef.current
    if (!el) return
    const ro = new ResizeObserver(() => {
      const r = el.getBoundingClientRect()
      const w = Math.max(200, Math.floor(r.width))
      const h = Math.max(200, Math.floor(r.height))
      setDims({ w, h })
    })
    ro.observe(el)
    return () => ro.disconnect()
  }, [])

  const mergedHighlightIds = useMemo(() => {
    const s = new Set<string>()
    for (const m of messages) {
      if (m.role !== 'assistant') continue
      if (!m.showGraphHighlights || !m.graphHighlightIds?.length) continue
      for (const id of m.graphHighlightIds) {
        if (id) s.add(id)
      }
    }
    return s
  }, [messages])

  const filteredGraph = useMemo(() => {
    if (granular) return graphData
    const hide = new Set(['sales_order_item', 'delivery_item', 'billing_item', 'journal_entry'])
    const nodes = graphData.nodes.filter((n) => !hide.has(String(n.group)))
    const ids = new Set(nodes.map((n) => n.id))
    const links = graphData.links.filter((l) => {
      const s = linkEndId(l.source)
      const t = linkEndId(l.target)
      return ids.has(s) && ids.has(t)
    })
    return { nodes, links }
  }, [graphData, granular])

  const openNode = useCallback(async (id: string) => {
    setDetailOpen(true)
    setDetailLoading(true)
    setDetail({})
    setDetailTitle(id)
    const r = await fetch(`${API}/node/${encodeURIComponent(id)}`)
    if (!r.ok) {
      setDetail({ nodeId: id })
      setDetailLoading(false)
      return
    }
    const text = await r.text()
    let j: { metadata?: Record<string, unknown>; group?: string; label?: string }
    try {
      j = text ? (JSON.parse(text) as typeof j) : {}
    } catch {
      setDetail({
        message: isLikelyOffline()
          ? OFFLINE_MSG
          : 'Could not load node details. Please try again.',
      })
      setDetailLoading(false)
      return
    }
    const meta = j.metadata || {}
    setDetailTitle(String(meta.entity || j.group || j.label || id))
    const flat: Record<string, string> = {}
    for (const [k, v] of Object.entries(meta)) {
      flat[k] = v === null || v === undefined ? '' : String(v)
    }
    if (!Object.keys(flat).length && j.label) flat.label = String(j.label)
    if (!Object.keys(flat).length && j.group) flat.group = String(j.group)
    setDetail(flat)
    setDetailLoading(false)
  }, [])

  const sendChat = useCallback(async () => {
    const text = input.trim()
    if (!text || chatBusy) return
    setInput('')
    const next: ChatMsg[] = [...messages, { role: 'user', content: text }]
    setMessages(next)
    setChatBusy(true)
    try {
      const r = await fetch(`${API}/chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          messages: next.map((m) => ({ role: m.role, content: m.content })),
        }),
      })
      const text = await r.text()
      let j: { reply?: string; highlight_node_ids?: string[] }
      try {
        j = text ? (JSON.parse(text) as typeof j) : {}
      } catch {
        setMessages((m) => [
          ...m,
          {
            role: 'assistant',
            content: isLikelyOffline()
              ? OFFLINE_MSG
              : r.ok
                ? 'The server sent a response that could not be read. Please try again.'
                : `Server error (${r.status}). Please try again later.`,
          },
        ])
        return
      }
      const reply = String(j.reply || 'No response.')
      const ids: string[] = (j.highlight_node_ids || []).filter(
        (x): x is string => typeof x === 'string' && x.length > 0,
      )
      setMessages((m) => [
        ...m,
        {
          role: 'assistant',
          content: reply,
          ...(ids.length
            ? { graphHighlightIds: ids, showGraphHighlights: true }
            : {}),
        },
      ])
      if (ids.length) {
        setTimeout(() => fgRef.current?.zoomToFit?.(400, 40), 100)
      }
    } catch (e) {
      const msg = String(e instanceof Error ? e.message : e).toLowerCase()
      const netFail =
        e instanceof TypeError &&
        (msg.includes('fetch') ||
          msg.includes('network') ||
          msg.includes('failed to load'))
      setMessages((m) => [
        ...m,
        {
          role: 'assistant',
          content:
            isLikelyOffline() || netFail
              ? OFFLINE_MSG
              : `Request failed: ${e instanceof Error ? e.message : e}`,
        },
      ])
    } finally {
      setChatBusy(false)
    }
  }, [input, chatBusy, messages])

  useEffect(() => {
    if (mergedHighlightIds.size === 0) {
      setHighlightPulse(0)
      return
    }
    const t0 = performance.now()
    const id = window.setInterval(() => {
      setHighlightPulse((Math.sin((performance.now() - t0) / 280) + 1) / 2)
    }, 48)
    return () => window.clearInterval(id)
  }, [mergedHighlightIds])

  const nodeColor = useCallback(
    (n: GraphNode) => {
      if (mergedHighlightIds.has(n.id)) return '#ea580c'
      const g = String(n.group || '')
      if (g === 'customer') return '#fca5a5'
      if (g === 'product') return '#fcd34d'
      if (g === 'plant') return '#86efac'
      return '#93c5fd'
    },
    [mergedHighlightIds],
  )

  const nodeVal = useCallback(
    (n: GraphNode) => (mergedHighlightIds.has(n.id) ? 2.4 : 1),
    [mergedHighlightIds],
  )

  const nodeCanvasObject = useCallback(
    (node: GraphNode, ctx: CanvasRenderingContext2D, globalScale: number) => {
      if (!mergedHighlightIds.has(node.id)) return
      const x = node.x ?? 0
      const y = node.y ?? 0
      const p = highlightPulse
      for (let i = 0; i < 3; i++) {
        const base = 10 + i * 7 + p * 10
        const r = base / globalScale
        ctx.beginPath()
        ctx.arc(x, y, r, 0, 2 * Math.PI, false)
        ctx.strokeStyle = `rgba(234, 88, 12, ${0.55 - i * 0.14 + p * 0.2})`
        ctx.lineWidth = (3.2 - i * 0.6) / globalScale
        ctx.stroke()
      }
    },
    [mergedHighlightIds, highlightPulse],
  )

  const linkWidth = useCallback(
    (link: GraphLink) => {
      const s = linkEndId(link.source)
      const t = linkEndId(link.target)
      if (mergedHighlightIds.has(s) || mergedHighlightIds.has(t)) return 2.2
      return 0.55
    },
    [mergedHighlightIds],
  )

  const linkColor = useCallback(
    (link: GraphLink) => {
      const s = linkEndId(link.source)
      const t = linkEndId(link.target)
      if (mergedHighlightIds.has(s) || mergedHighlightIds.has(t)) return '#fb923c'
      return '#bfdbfe'
    },
    [mergedHighlightIds],
  )

  return (
    <div className="app-shell">
      <header className="top-bar">
        <span>
          Mapping / <strong>Order to Cash</strong>
        </span>
        {totals && (
          <span style={{ marginLeft: 'auto', fontSize: 12, color: 'var(--text-muted)' }}>
            Graph: {filteredGraph.nodes.length.toLocaleString()} / {totals.nodes.toLocaleString()} nodes ·{' '}
            {filteredGraph.links.length.toLocaleString()} links in view
          </span>
        )}
      </header>

      <div className="main-row">
        <section className="graph-pane">
          <div className="graph-toolbar">
            <button type="button" onClick={() => setGranular((v) => !v)}>
              {granular ? 'Hide granular overlay' : 'Show granular overlay'}
            </button>
            <button
              type="button"
              title="Turn off graph highlights for all assistant replies"
              disabled={mergedHighlightIds.size === 0}
              onClick={() => {
                setMessages((prev) =>
                  prev.map((x) =>
                    x.role === 'assistant' && x.graphHighlightIds?.length
                      ? { ...x, showGraphHighlights: false }
                      : x,
                  ),
                )
              }}
            >
              Reset highlights
            </button>
          </div>
          <div ref={graphWrapRef} className="graph-canvas-wrap">
            {filteredGraph.nodes.length > 0 && (
              <ForceGraph2D
                ref={fgRef}
                width={dims.w}
                height={dims.h}
                graphData={filteredGraph}
                nodeLabel="label"
                nodeRelSize={5}
                nodeVal={nodeVal}
                nodeColor={nodeColor}
                nodeCanvasObjectMode="after"
                nodeCanvasObject={nodeCanvasObject}
                linkColor={linkColor}
                linkWidth={linkWidth}
                onNodeClick={(n: GraphNode) => openNode(n.id)}
                cooldownTicks={120}
                onEngineStop={() => fgRef.current?.zoomToFit?.(400, 60)}
              />
            )}
          </div>
        </section>

        <div
          className="chat-splitter"
          role="separator"
          aria-orientation="vertical"
          aria-label="Resize chat panel"
          ref={splitterRef}
          onMouseDown={(e) => {
            e.preventDefault()
            widthToPersistRef.current = null
            resizeRef.current = { startX: e.clientX, startW: chatWidth }
            document.body.classList.add('col-resizing')
            e.currentTarget.classList.add('dragging')
          }}
        />

        <aside className="chat-pane" style={{ width: chatWidth }}>
          <div className="chat-inner">
            <div className="chat-header">
              <h2>Chat with Graph</h2>
              <p>Order to Cash</p>
            </div>
            <div className="chat-messages" ref={chatMessagesRef}>
              {messages.map((m, i) => (
                <div key={i} className={`bubble ${m.role === 'user' ? 'user' : 'agent'}`}>
                  {m.role === 'assistant' && (
                    <div className="bubble-meta">Dodge AI · Graph Agent</div>
                  )}
                  {m.role === 'assistant' ? (
                    <AssistantContent text={m.content} />
                  ) : (
                    m.content
                  )}
                  {m.role === 'assistant' && (m.graphHighlightIds?.length ?? 0) > 0 && (
                    <div className="bubble-highlight-row">
                      <button
                        type="button"
                        className={`bubble-highlight-btn${m.showGraphHighlights ? ' is-on' : ''}`}
                        aria-pressed={Boolean(m.showGraphHighlights)}
                        title={
                          m.showGraphHighlights
                            ? 'Hide this reply’s highlights on the graph'
                            : 'Show this reply’s highlights on the graph (combined with other toggled replies)'
                        }
                        onClick={() => {
                          const turningOn = !m.showGraphHighlights
                          setMessages((prev) =>
                            prev.map((x, idx) =>
                              idx === i && x.role === 'assistant'
                                ? { ...x, showGraphHighlights: turningOn }
                                : x,
                            ),
                          )
                          if (turningOn && m.graphHighlightIds?.length) {
                            setTimeout(() => fgRef.current?.zoomToFit?.(400, 40), 100)
                          }
                        }}
                      >
                        <GraphHighlightGlyph on={Boolean(m.showGraphHighlights)} />
                        <span>Show on graph</span>
                      </button>
                    </div>
                  )}
                </div>
              ))}
            </div>
            <div className="chat-input-row">
              <div className="status-line">
                <span className="status-dot" />
                {chatBusy ? 'Dodge AI is thinking…' : 'Dodge AI is awaiting instructions'}
              </div>
              <div className="input-row">
                <input
                  placeholder="Analyze anything"
                  value={input}
                  onChange={(e) => setInput(e.target.value)}
                  onKeyDown={(e) => e.key === 'Enter' && !e.shiftKey && (e.preventDefault(), sendChat())}
                />
                <button type="button" className="send-btn" onClick={sendChat} disabled={chatBusy}>
                  Send
                </button>
              </div>
            </div>
          </div>
        </aside>
      </div>

      {detailOpen && (
        <div
          className="modal-backdrop"
          role="presentation"
          onClick={() => {
            setDetailOpen(false)
            setDetail({})
          }}
        >
          <div className="modal" role="dialog" onClick={(e) => e.stopPropagation()}>
            <header>
              <span>{detailTitle}</span>
              <button
                type="button"
                onClick={() => {
                  setDetailOpen(false)
                  setDetail({})
                }}
                aria-label="Close"
              >
                ×
              </button>
            </header>
            <div className="modal-body">
              {detailLoading ? (
                <p style={{ margin: 0, color: 'var(--text-muted)' }}>Loading…</p>
              ) : Object.keys(detail).length === 0 ? (
                <p style={{ margin: 0, color: 'var(--text-muted)' }}>No metadata for this node.</p>
              ) : (
                <dl>
                  {Object.entries(detail).map(([k, v]) => (
                    <div key={k} style={{ display: 'contents' }}>
                      <dt>{k}</dt>
                      <dd>{v}</dd>
                    </div>
                  ))}
                </dl>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
