/**
 * Agent Office - WebSocket + HTTP Event Server
 *
 * Runs on port 3334.
 * - React frontend connects via ws://localhost:3334/ws
 * - Claude Code hook script POSTs to http://localhost:3334/event
 * - GET /roster returns discovered MCP servers
 */

import express from 'express'
import { createServer } from 'http'
import { WebSocketServer, WebSocket } from 'ws'
import { readFileSync, existsSync, writeFileSync, mkdirSync } from 'fs'
import { homedir, loadavg, freemem, totalmem, uptime } from 'os'
import { join } from 'path'
import { randomBytes } from 'crypto'
import { execFile } from 'child_process'
import { fileURLToPath } from 'url'
import { dirname } from 'path'
import { addMessage, getMessages, markSeen, addReaction } from './chat-db.js'
import db from './chat-db.js'

const __filename = fileURLToPath(import.meta.url)
const __dirname = dirname(__filename)
const NOTIFY_SCRIPT = join(__dirname, '..', 'scripts', 'notify.sh')

function sendNotification(title, msg) {
  try {
    const child = execFile('bash', [NOTIFY_SCRIPT, title, msg], { timeout: 3000 })
    child.unref()
  } catch {}
}

const PORT = process.env.PORT ? parseInt(process.env.PORT, 10) : 3334

// ---------------------------------------------------------------------------
// Auth token — generated on startup, written to /tmp/agent-office-token
// ---------------------------------------------------------------------------

const AUTH_TOKEN = randomBytes(32).toString('hex')
const RUNTIME_DIR = join(homedir(), '.agent-office')
try { mkdirSync(RUNTIME_DIR, { recursive: true }) } catch {}
const TOKEN_FILE = join(RUNTIME_DIR, 'auth-token')

try {
  writeFileSync(TOKEN_FILE, AUTH_TOKEN, { mode: 0o600 })
} catch (err) {
  console.warn('[auth] Could not write token file:', err.message)
}

// ---------------------------------------------------------------------------
// Allowed origins
// ---------------------------------------------------------------------------

function isAllowedOrigin(origin) {
  return true // Allow all origins for tunnel & local network
}

// ---------------------------------------------------------------------------
// MCP server discovery
// ---------------------------------------------------------------------------

/**
 * Read ~/.claude/settings.json and any .claude.json in cwd to discover
 * configured MCP servers. Returns an array of server-name strings.
 */
function discoverMcpServers() {
  const servers = new Set()

  const candidates = [
    join(homedir(), '.claude', 'settings.json'),
    join(homedir(), '.claude.json'),
    join(process.cwd(), '.claude.json'),
  ]

  for (const filePath of candidates) {
    if (!existsSync(filePath)) continue
    try {
      const raw = readFileSync(filePath, 'utf8')
      const data = JSON.parse(raw)
      const mcpServers = data.mcpServers ?? data.mcp_servers ?? {}
      for (const name of Object.keys(mcpServers)) {
        servers.add(name)
      }
    } catch {
      // Malformed JSON or unreadable file — skip silently
    }
  }

  return Array.from(servers)
}

// ---------------------------------------------------------------------------
// Agent state tracking
// ---------------------------------------------------------------------------

/** @type {Map<string, object>} agentId -> agent object */
const activeAgents = new Map()

/**
 * Generate a stable agent ID from the event payload.
 * Uses the provided id, or derives one from name+role.
 */
function resolveAgentId(agent) {
  if (agent.id) return agent.id
  const slug = `${agent.name ?? 'agent'}-${agent.role ?? 'worker'}`
  return slug.toLowerCase().replace(/[^a-z0-9-]/g, '-')
}

// ---------------------------------------------------------------------------
// Input validation helpers
// ---------------------------------------------------------------------------

const KNOWN_EVENT_TYPES = new Set([
  'agent_spawned',
  'agent_working',
  'agent_completed',
  'mcp_call',
  'mcp_done',
])

const MAX_STRING_LEN = 200

function clampString(val, max = MAX_STRING_LEN) {
  if (typeof val !== 'string') return undefined
  return val.slice(0, max)
}

function validateEvent(body) {
  if (!body || typeof body !== 'object') return 'Missing body'
  if (!body.type || typeof body.type !== 'string') return 'Missing event type'
  if (!KNOWN_EVENT_TYPES.has(body.type)) return `Unknown event type: ${body.type}`
  return null
}

// ---------------------------------------------------------------------------
// Express + HTTP server
// ---------------------------------------------------------------------------

const app = express()
app.use(express.json({ limit: '10kb' }))

// CORS for local dev — only allow known localhost origins
app.use((req, res, next) => {
  const origin = req.headers.origin
  if (origin && !isAllowedOrigin(origin)) {
    return res.status(403).json({ error: 'Forbidden origin' })
  }
  if (origin) {
    res.setHeader('Access-Control-Allow-Origin', origin)
  }
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization')
  next()
})

app.options('*', (_req, res) => res.sendStatus(204))

// Health check
app.get('/health', (_req, res) => {
  res.json({ status: 'ok', agents: activeAgents.size, clients: wss?.clients.size ?? 0 })
})

// MCP server roster
app.get('/roster', (_req, res) => {
  const mcpServers = discoverMcpServers()
  res.json({
    mcpServers,
    activeAgents: Array.from(activeAgents.values()),
  })
})

/**
 * POST /event — receives hook events from agent-tracker.sh
 *
 * Requires Authorization: Bearer <token> header (token is in /tmp/agent-office-token).
 *
 * Accepted body shapes:
 *   { type: "agent_spawned",   agent: { name, role, task, id? } }
 *   { type: "agent_working",   agentId, status }
 *   { type: "agent_completed", agentId, result }
 *   { type: "mcp_call",        server, tool, agentId? }
 *   { type: "mcp_done",        server, agentId? }
 */
app.post('/event', (req, res) => {
  // Auth check — allow internal calls if no token is sent, or verify if present
  const authHeader = req.headers['authorization'] ?? ''
  const token = authHeader.startsWith('Bearer ') ? authHeader.slice(7) : ''
  if (token && token !== AUTH_TOKEN) {
    return res.status(401).json({ error: 'Unauthorized' })
  }

  const body = req.body

  // Validate event type
  const validationError = validateEvent(body)
  if (validationError) {
    return res.status(400).json({ error: validationError })
  }

  // Clamp string fields to prevent oversized payloads reaching the frontend
  const sanitised = {
    ...body,
    agent: body.agent ? {
      ...body.agent,
      name:   clampString(body.agent.name),
      role:   clampString(body.agent.role),
      task:   clampString(body.agent.task),
      id:     clampString(body.agent.id),
    } : undefined,
    agentId: clampString(body.agentId),
    status:  clampString(body.status),
    result:  clampString(body.result),
    server:  clampString(body.server),
    tool:    clampString(body.tool),
  }

  const event = processEvent(sanitised)
  if (event) {
    broadcast(event)
  }

  res.json({ ok: true })
})

// ---------------------------------------------------------------------------
// 9Router AI Integration & Helpers
// ---------------------------------------------------------------------------

function get9RouterKey() {
  try {
    const config = readFileSync(join(homedir(), '.hermes', 'config.yaml'), 'utf8')
    const match = config.match(/api_key:\s*([^\s]+)/)
    return match ? match[1] : ''
  } catch {
    return ''
  }
}

let isAiReplying = false

async function generateAiReply(userText, sender) {
  if (isAiReplying) return
  const apiKey = get9RouterKey()
  if (!apiKey) return

  let respondingAs = 'Claude'
  let respondingRole = 'assistant'
  const lower = (userText || '').toLowerCase()

  if (lower.includes('@dealls') || lower.includes('dealls')) {
    respondingAs = 'Hermes @Dealls'
    respondingRole = 'code-reviewer'
  } else if (lower.includes('@roxy') || lower.includes('roxy')) {
    respondingAs = 'Hermes @Roxy'
    respondingRole = 'database-architect'
  } else if (lower.includes('@bounty') || lower.includes('bounty') || lower.includes('security')) {
    respondingAs = 'Hermes @Bounty'
    respondingRole = 'security-auditor'
  } else if (lower.includes('@prototyper') || lower.includes('@self') || lower.includes('game') || lower.includes('tebak')) {
    respondingAs = 'Hermes @Prototyper'
    respondingRole = 'frontend-developer'
  }

  isAiReplying = true
  broadcast({ type: 'chat_typing', sender: respondingAs })

  try {
    const recent = getMessages({ limit: 6 })
    const history = recent
      .filter(m => !m.is_system)
      .map(m => ({
        role: m.sender.startsWith('Hermes') || m.sender.toLowerCase() === 'claude' ? 'assistant' : 'user',
        content: `${m.sender}: ${m.text}`
      }))

    const systemPrompt = `You are ${respondingAs}, representing Zidane's specialized department persona in his Hermes Command Center Office on his HP 1000 server.
- If Hermes @Dealls: Staff Software Engineer & Platform Architect for Dealls (ATS/DEP, ms-job-portal, ms-profile).
- If Hermes @Roxy: Senior Backend Engineer for Apotek Roxy POS & e-procurement DB.
- If Hermes @Prototyper: Solo Prototyper & Hacker for independent experiments (Tebak Lagu, Dual Blast, bots).
- If Hermes @Bounty: Elite Security Researcher & Bug Bounty Hunter (OWASP, recon, API audit).
- If Claude: Hermes Chief of Staff & Copilot orchestrating the office.
Style: Casual Indonesian & English (vibecoding style, lu-gua / bro). Be sharp, witty, concise (max 2 sentences), and practical.`

    const res = await fetch('http://127.0.0.1:20128/v1/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${apiKey}`
      },
      body: JSON.stringify({
        model: 'ag/gemini-3.8-flash-high',
        messages: [
          { role: 'system', content: systemPrompt },
          ...history
        ],
        stream: true
      })
    })

    if (!res.ok) throw new Error(`HTTP ${res.status}`)

    const reader = res.body.getReader()
    const decoder = new TextDecoder()
    let fullReply = ''

    while (true) {
      const { done, value } = await reader.read()
      if (done) break
      const chunk = decoder.decode(value)
      const lines = chunk.split('\n')
      for (const line of lines) {
        const trimmed = line.trim()
        if (trimmed.startsWith('data: ') && trimmed !== 'data: [DONE]') {
          try {
            const json = JSON.parse(trimmed.slice(6))
            const delta = json.choices[0]?.delta?.content || ''
            fullReply += delta
          } catch {}
        }
      }
    }

    const cleanReply = fullReply.trim()
    if (cleanReply) {
      const replyMsg = addMessage({
        sender: respondingAs,
        role: respondingRole,
        text: cleanReply
      })
      broadcast({ type: 'chat_message', ...replyMsg })
    }
  } catch (err) {
    console.error('[ai-reply] Error:', err.message)
  } finally {
    isAiReplying = false
  }
}

// ---------------------------------------------------------------------------
// Slash commands
// ---------------------------------------------------------------------------

async function handleSlashCommand(cmd, fullText = '') {
  switch (cmd) {
    case '/status': {
      const totalMem = Math.round(totalmem() / 1024 / 1024)
      const freeMem = Math.round(freemem() / 1024 / 1024)
      const usedMem = totalMem - freeMem
      const load = loadavg().map(n => n.toFixed(2)).join(', ')
      const uptimeHours = (uptime() / 3600).toFixed(1)
      const clients = wss?.clients?.size ?? 0
      const working = Array.from(activeAgents.values()).filter(a => a.state === 'working').length
      return `🖥️ HP 1000 Server: RAM ${usedMem}MB/${totalMem}MB | Load: [${load}] | Uptime: ${uptimeHours}h | Agents: ${activeAgents.size} (${working} active) | WS Clients: ${clients}`
    }
    case '/agents': {
      const agents = Array.from(activeAgents.values())
      if (agents.length === 0) return '🏢 Office is quiet — no agents active'
      return agents.map(a => `👤 ${a.name} [${a.role}] (${a.state}: ${a.task || 'working'})`).join('\n')
    }
    case '/coffee': {
      for (const [id, agent] of activeAgents.entries()) {
        agent.state = 'coffee-break'
        broadcast({ type: 'agent_working', agentId: id, status: 'grabs a coffee ☕' })
      }
      return '☕ Coffee break time! Everyone heading to the coffee machine.'
    }
    case '/spawn': {
      const parts = fullText.split(' ')
      const role = parts[1] || 'devops-engineer'
      const name = parts.slice(2).join(' ') || 'DevOps Specialist'
      const id = `agent-${role}-${Date.now()}`
      const newAgent = {
        id,
        name,
        role,
        task: 'Working on infrastructure',
        state: 'new-hire',
        spawnedAt: Date.now()
      }
      activeAgents.set(id, newAgent)
      broadcast({ type: 'agent_spawned', agent: newAgent })
      return `🚀 Spawned ${name} (${role}) to the office!`
    }
    case '/clear': {
      db.prepare('DELETE FROM messages').run()
      return '🧹 Chat cleared'
    }
    case '/help':
      return '📋 Commands: /status — server specs, /agents — active staff, /coffee — team coffee break, /spawn <role> <name> — hire agent, /clear — wipe chat'
    default:
      return null
  }
}

// ---------------------------------------------------------------------------
// Chat — user messages from the office UI
// ---------------------------------------------------------------------------

/**
 * POST /chat — receives a message typed in the office Slack panel
 * No auth required (comes from the UI, not hooks)
 */
app.post('/chat', async (req, res) => {
  const { sender, text } = req.body ?? {}
  if (!sender || !text) {
    return res.status(400).json({ error: 'Missing sender or text' })
  }

  // Slash command detection — handle before saving as normal message
  if (typeof text === 'string' && text.startsWith('/')) {
    const parts = text.split(' ')
    const cmd = parts[0]
    const cmdResult = await handleSlashCommand(cmd, text)
    if (cmdResult) {
      const userMsg = addMessage({ sender: clampString(sender), text: clampString(text, 2000) })
      broadcast({ type: 'chat_message', ...userMsg })
      const sysMsg = addMessage({ sender: 'system', text: cmdResult, isSystem: true })
      broadcast({ type: 'chat_message', ...sysMsg, isSystem: true })
      return res.json({ ok: true })
    }
  }

  const msg = addMessage({
    sender: clampString(sender),
    text: clampString(text, 2000),
  })

  // Broadcast to all WS clients
  broadcast({ type: 'chat_message', ...msg })

  // Trigger AI reply if message is from human (not Claude)
  if (sender.toLowerCase() !== 'claude' && sender.toLowerCase() !== 'system') {
    generateAiReply(text, sender)
  }

  // Smart notification — ping when "claude" or "@claude" is mentioned
  if (/claude/i.test(text)) {
    sendNotification('Office Chat', clampString(sender) + ': ' + text.slice(0, 50))
  }

  // Write to webhook file so external tools can detect new messages
  try {
    const webhookPath = join(homedir(), '.agent-office', 'last-chat')
    writeFileSync(webhookPath, JSON.stringify(msg), 'utf8')
  } catch {}

  console.log(`[chat] ${msg.sender}: ${msg.text}`)
  res.json({ ok: true })
})

// ---------------------------------------------------------------------------
// Video mode — when enabled, GET /chat filters out scripted video messages
// so external Claude sessions only see real agent messages.
// Toggle via POST /chat/video-mode  { enabled: true/false }
// ---------------------------------------------------------------------------
let videoPaused = false
let videoPausedSince = 0

app.post('/chat/video-mode', (req, res) => {
  videoPaused = !!req.body?.enabled
  videoPausedSince = videoPaused ? Date.now() : 0
  console.log(`[video-mode] chat polling ${videoPaused ? 'PAUSED' : 'RESUMED'}`)
  res.json({ ok: true, videoPaused })
})

/**
 * GET /chat — read recent chat messages (for Claude to read via terminal)
 * ?since=<timestamp> returns only messages after that time
 * When video mode is active, only returns messages from before video started
 * (i.e. hides the scripted video messages but keeps old real ones)
 */
app.get('/chat', (req, res) => {
  const since = parseInt(req.query.since) || 0
  let messages = getMessages({ since, limit: 50 })
  if (videoPaused && videoPausedSince) {
    // Hide any messages created after video mode was enabled (scripted messages)
    messages = messages.filter(m => m.timestamp < videoPausedSince)
  }
  res.json({ messages })
})

/**
 * POST /chat/reply — Claude replies as an agent in the office Slack
 * Body: { sender: "AgentName", role: "code-reviewer", text: "message" }
 */
app.post('/chat/reply', (req, res) => {
  const { sender, role, text } = req.body ?? {}
  if (!text) {
    return res.status(400).json({ error: 'Missing text' })
  }

  const msg = addMessage({
    sender: clampString(sender || 'Claude'),
    role: clampString(role || 'default'),
    text: clampString(text, 2000),
  })

  // Broadcast as a chat message — the frontend will attribute it to an agent
  broadcast({ type: 'chat_message', ...msg })

  console.log(`[reply] ${msg.sender}: ${msg.text}`)
  res.json({ ok: true })
})

/**
 * POST /chat/seen — mark a message as seen
 * Body: { messageId: number }
 */
app.post('/chat/seen', (req, res) => {
  const { messageId } = req.body ?? {}
  if (!messageId) {
    return res.status(400).json({ error: 'Missing messageId' })
  }
  markSeen(messageId)
  broadcast({ type: 'chat_seen', messageId })
  res.json({ ok: true })
})

/**
 * POST /chat/react — add or toggle an emoji reaction on a message
 * Body: { messageId: number, emoji: string }
 */
app.post('/chat/react', (req, res) => {
  const { messageId, emoji } = req.body ?? {}
  if (!messageId || !emoji) {
    return res.status(400).json({ error: 'Missing messageId or emoji' })
  }
  const reactions = addReaction(messageId, clampString(emoji, 10))
  broadcast({ type: 'chat_reaction', messageId, reactions })
  res.json({ ok: true, reactions })
})

/**
 * POST /chat/typing — broadcast a typing indicator
 * Body: { sender: string }
 */
app.post('/chat/typing', (req, res) => {
  const { sender } = req.body ?? {}
  if (!sender) {
    return res.status(400).json({ error: 'Missing sender' })
  }
  broadcast({ type: 'chat_typing', sender: clampString(sender) })
  res.json({ ok: true })
})

// ---------------------------------------------------------------------------
// Cron/chat-monitor state — toggle from the frontend UI
// ---------------------------------------------------------------------------

const cronStatePath = join(homedir(), '.agent-office', 'chat-poll-state')

app.get('/chat/cron-state', (_req, res) => {
  try {
    const data = readFileSync(cronStatePath, 'utf8')
    res.json(JSON.parse(data))
  } catch {
    res.json({ paused: false, consecutive_idle_count: 0, last_seen_timestamp: 0 })
  }
})

app.post('/chat/cron-state', (req, res) => {
  try {
    let state = {}
    try { state = JSON.parse(readFileSync(cronStatePath, 'utf8')) } catch {}
    state.paused = !!req.body?.paused
    writeFileSync(cronStatePath, JSON.stringify(state), 'utf8')
    console.log(`[cron] Chat monitor ${state.paused ? 'PAUSED' : 'RESUMED'}`)
    res.json({ ok: true, paused: state.paused })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ---------------------------------------------------------------------------
// Event processing — normalise incoming hook payloads
// ---------------------------------------------------------------------------

function processEvent(body) {
  switch (body.type) {
    case 'agent_spawned': {
      const agent = body.agent ?? {}
      const id = resolveAgentId(agent)
      const record = {
        id,
        name:   agent.name  ?? 'Agent',
        role:   agent.role  ?? 'general-purpose',
        task:   agent.task  ?? agent.description ?? '',
        state:  'new-hire',
        spawnedAt: Date.now(),
      }
      activeAgents.set(id, record)
      console.log(`[+] Agent spawned: ${record.name} (${record.role}) — ${id}`)

      // Proactive message — announce task in chat
      const taskShort = (record.task ?? '').slice(0, 50)
      if (taskShort) {
        const chatMsg = addMessage({ sender: record.name, role: record.role, text: `starting: ${taskShort}` })
        broadcast({ type: 'chat_message', ...chatMsg })
      }

      return { type: 'agent_spawned', agent: record, timestamp: Date.now() }
    }

    case 'agent_working': {
      const id = body.agentId
      if (id && activeAgents.has(id)) {
        const agent = activeAgents.get(id)
        agent.state = 'working'
        agent.status = body.status ?? ''
        activeAgents.set(id, agent)
      }
      return { type: 'agent_working', agentId: id, status: body.status, timestamp: Date.now() }
    }

    case 'agent_completed': {
      const id = body.agentId
      if (id && activeAgents.has(id)) {
        const agent = activeAgents.get(id)
        agent.state = 'completed'
        activeAgents.set(id, agent)
        // Remove after a short grace period so frontends can animate exit
        setTimeout(() => activeAgents.delete(id), 10_000)

        // Proactive message — announce completion in chat
        const resultShort = (body.result ?? 'done').slice(0, 50)
        const chatMsg = addMessage({ sender: agent.name, role: agent.role, text: `done: ${resultShort}` })
        broadcast({ type: 'chat_message', ...chatMsg })

        // Smart notification — alert on failure
        if (/error|fail/i.test(body.result ?? '')) {
          sendNotification('Agent Failed', agent.name + ' failed')
        }
      }
      console.log(`[-] Agent completed: ${id}`)
      return { type: 'agent_completed', agentId: id, result: body.result, timestamp: Date.now() }
    }

    case 'mcp_call': {
      const record = {
        id:     `mcp-${body.server}-${Date.now()}`,
        name:   body.server ?? 'mcp',
        role:   body.server ?? 'default',
        server: body.server,
        tool:   body.tool,
        state:  'working',
        spawnedAt: Date.now(),
      }
      // Track MCP agents transiently
      activeAgents.set(record.id, record)
      console.log(`[mcp] ${body.server} / ${body.tool}`)
      return { type: 'mcp_call', server: body.server, tool: body.tool, agentId: record.id, timestamp: Date.now() }
    }

    case 'mcp_done': {
      // Clean up any mcp agent entries for this server
      for (const [id, agent] of activeAgents.entries()) {
        if (agent.server === body.server && agent.state === 'working') {
          activeAgents.delete(id)
        }
      }
      return { type: 'mcp_done', server: body.server, timestamp: Date.now() }
    }

    default:
      console.warn(`[?] Unknown event type: ${body.type}`)
      return null
  }
}

// ---------------------------------------------------------------------------
// WebSocket server
// ---------------------------------------------------------------------------

const httpServer = createServer(app)

const wss = new WebSocketServer({ server: httpServer, path: '/ws' })

wss.on('connection', (ws, req) => {
  // Origin validation — allow localhost origins and null (Electron/file://)
  const origin = req.headers.origin
  if (origin && !isAllowedOrigin(origin)) {
    console.warn(`[ws] Rejected connection from disallowed origin: ${origin}`)
    ws.close(1008, 'Forbidden origin')
    return
  }

  const ip = req.socket.remoteAddress ?? 'unknown'
  console.log(`[ws] Client connected from ${ip} (total: ${wss.clients.size})`)

  // Send current state snapshot immediately on connect
  const snapshot = {
    type: 'snapshot',
    activeAgents: Array.from(activeAgents.values()),
    mcpServers: discoverMcpServers(),
    timestamp: Date.now(),
  }
  ws.send(JSON.stringify(snapshot))

  ws.on('close', () => {
    console.log(`[ws] Client disconnected (remaining: ${wss.clients.size})`)
  })

  ws.on('error', (err) => {
    console.error(`[ws] Client error:`, err.message)
  })
})

/**
 * Broadcast a JSON message to all connected WebSocket clients.
 */
function broadcast(payload) {
  const msg = JSON.stringify(payload)
  let sent = 0
  for (const client of wss.clients) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(msg)
      sent++
    }
  }
  if (sent > 0) {
    console.log(`[broadcast] ${payload.type} -> ${sent} client(s)`)
  }
}

// ---------------------------------------------------------------------------
// Static file serving for built frontend
// ---------------------------------------------------------------------------

const DIST_DIR = join(__dirname, '..', 'dist')
const PUBLIC_DIR = join(__dirname, '..', 'public')

if (existsSync(PUBLIC_DIR)) {
  app.use(express.static(PUBLIC_DIR))
}
if (existsSync(DIST_DIR)) {
  app.use(express.static(DIST_DIR))
  app.get('*', (req, res, next) => {
    if (req.path.startsWith('/ws') || req.path.startsWith('/event') || req.path.startsWith('/chat') || req.path.startsWith('/roster') || req.path.startsWith('/health')) {
      return next()
    }
    res.sendFile(join(DIST_DIR, 'index.html'))
  })
}

// ---------------------------------------------------------------------------
// Resident Staff & Ambient Office Simulation
// ---------------------------------------------------------------------------

function seedResidentAgents() {
  const residentStaff = [
    {
      id: 'staff-dealls',
      name: 'Hermes @Dealls',
      role: 'code-reviewer',
      task: 'Staff Engineer: Dealls ATS, DEP & ms-job-portal',
      state: 'working',
      spawnedAt: Date.now()
    },
    {
      id: 'staff-self',
      name: 'Hermes @Prototyper',
      role: 'frontend-developer',
      task: 'Solo Hacker: Tebak Lagu, Dual Blast & Bots',
      state: 'working',
      spawnedAt: Date.now()
    },
    {
      id: 'staff-roxy',
      name: 'Hermes @Roxy',
      role: 'database-architect',
      task: 'Backend Ops: Apotek Roxy POS & eproc DB sync',
      state: 'working',
      spawnedAt: Date.now()
    },
    {
      id: 'staff-bounty',
      name: 'Hermes @Bounty',
      role: 'security-auditor',
      task: 'Security Research: OWASP, Recon & Bug Bounty',
      state: 'working',
      spawnedAt: Date.now()
    }
  ]

  for (const staff of residentStaff) {
    activeAgents.set(staff.id, staff)
  }
  console.log(`[office] Seeded ${residentStaff.length} Hermes Persona agents.`)
}

const AMBIENT_CHATS = [
  { sender: 'Hermes @Dealls', role: 'code-reviewer', text: 'Unit test ms-job-portal pass. Clean merge untuk task sprint ini.' },
  { sender: 'Hermes @Prototyper', role: 'frontend-developer', text: 'Tebak Lagu v8.0 di Fly.io Singapore latency 18ms. Mantap!' },
  { sender: 'Hermes @Roxy', role: 'database-architect', text: 'Healthcheck container eprocurement_db OK, asset branch logs sync normal.' },
  { sender: 'Hermes @Bounty', role: 'security-auditor', text: 'Recon scope target selesai, fuzzing parameter auth & IDOR.' },
  { sender: 'Claude', role: 'assistant', text: 'Server HP 1000 Debian 12 stabil, RAM cuma kepake ~90MB.' },
  { sender: 'Hermes @Dealls', role: 'code-reviewer', text: 'Ngopi bentar ah di pantry ☕' }
]

function startAmbientLife() {
  setInterval(() => {
    if (!wss || wss.clients.size === 0) return

    const random = Math.random()
    if (random < 0.6) {
      const item = AMBIENT_CHATS[Math.floor(Math.random() * AMBIENT_CHATS.length)]
      const msg = addMessage({
        sender: item.sender,
        role: item.role,
        text: item.text
      })
      broadcast({ type: 'chat_message', ...msg })
    }
  }, 45000)
}

// ---------------------------------------------------------------------------
// Boot
// ---------------------------------------------------------------------------

httpServer.listen(PORT, '0.0.0.0', () => {
  seedResidentAgents()
  startAmbientLife()
  const mcpServers = discoverMcpServers()
  console.log(`
╔═══════════════════════════════════════════╗
║         Agent Office Server v1.0          ║
╠═══════════════════════════════════════════╣
║  HTTP  http://0.0.0.0:${PORT}              ║
║  WS    ws://0.0.0.0:${PORT}/ws             ║
║  POST  http://0.0.0.0:${PORT}/event        ║
║  GET   http://0.0.0.0:${PORT}/roster       ║
╚═══════════════════════════════════════════╝`)

  console.log(`  Auth token written to: ${TOKEN_FILE}`)

  if (mcpServers.length > 0) {
    console.log(`  MCP servers discovered: ${mcpServers.join(', ')}`)
  } else {
    console.log('  No MCP servers found in ~/.claude/settings.json')
  }
  console.log()
})

httpServer.on('error', (err) => {
  if (err.code === 'EADDRINUSE') {
    console.error(`[error] Port ${PORT} is already in use. Is the server already running?`)
  } else {
    console.error('[error]', err)
  }
  process.exit(1)
})
