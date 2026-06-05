// ── Shared state ──────────────────────────────────────────────────────────────
let currentJobId = null;      // set when coordinator clicks "Use this job now"
let currentJobContent = null; // set when a file is uploaded
let discoveredGames = [];     // active game list (used for job generation)
let discoverySnapshot = null; // saved result from Run Discovery (to restore after Show Pending)
let eventSource = null;
let jobTotal = 0;
let autoScroll = true;

// ── Helpers ───────────────────────────────────────────────────────────────────
function $(id) { return document.getElementById(id); }

function setMsg(id, text, cls = '') {
  const el = $(id);
  if (!el) return;
  el.textContent = text;
  el.className = 'msg' + (cls ? ' ' + cls : '');
}

function ts() {
  return new Date().toLocaleTimeString('en-GB', { hour12: false });
}

function appendLog(text, cls = '') {
  const box = $('log-box');
  if (!box) return;
  const line = document.createElement('div');
  line.className = 'log-line' + (cls ? ' ' + cls : '');
  line.textContent = text;
  box.appendChild(line);
  if (autoScroll) box.scrollTop = box.scrollHeight;
}

function updateProgress(dl, fail, skip, total) {
  if ($('cnt-dl')) $('cnt-dl').textContent = dl;
  if ($('cnt-fail')) $('cnt-fail').textContent = fail;
  if ($('cnt-skip')) $('cnt-skip').textContent = skip;
  const done = dl + fail + skip;
  const rem = total > 0 ? Math.max(0, total - done) : '—';
  if ($('cnt-rem')) $('cnt-rem').textContent = rem;
  const pct = total > 0 ? Math.round((done / total) * 100) : 0;
  const bar = $('prog-bar');
  if (bar) bar.style.width = pct + '%';
}

function setRunning(running) {
  const start = $('start-btn');
  const pause = $('pause-btn');
  if (start) start.disabled = running;
  if (pause) pause.disabled = !running;
}

// ── SSE connection ────────────────────────────────────────────────────────────
function connectSSE() {
  if (eventSource) { eventSource.close(); eventSource = null; }
  eventSource = new EventSource('/api/events');

  eventSource.onmessage = function(e) {
    const evt = JSON.parse(e.data);

    if (evt.type === 'connected') {
      appendLog(`${ts()} [connected to server]`, 'info');
      return;
    }

    if (evt.type === 'done') {
      const c = evt.counts || {};
      appendLog(
        `${ts()} ✓ done — downloaded:${c.downloaded||0}  failed:${c.failed||0}  skipped:${c.skipped||0}`,
        'ok'
      );
      setRunning(false);
      setMsg('start-msg', 'Session complete. Start again to resume (skips already-downloaded games).', 'ok');
      eventSource.close(); eventSource = null;

      // Show sidecar download button on friend page
      const sidecarWrap = $('sidecar-wrap');
      const sidecarLink = $('sidecar-link');
      if (sidecarWrap && sidecarLink && currentJobId) {
        const fname = currentJobId + '.progress.json';
        sidecarLink.href = `/api/sidecar/${encodeURIComponent(currentJobId)}`;
        sidecarLink.download = fname;
        sidecarWrap.style.display = 'block';
      }
      return;
    }

    if (evt.type === 'paused') {
      appendLog(`${ts()} ⏸ paused`, 'warn');
      setRunning(false);
      setMsg('start-msg', 'Paused. Click Start Download to resume.', '');
      return;
    }

    if (evt.type === 'sleep') {
      appendLog(`${ts()} sleeping ${evt.seconds}s…`, 'sleep');
      return;
    }

    // log event (downloaded / failed / skipped / rate_limited)
    if (evt.type === 'log') {
      const status = evt.status || '';
      const gameId = evt.game_id || '';
      const idx = evt.index != null ? evt.index + 1 : '?';
      const total = evt.total || jobTotal || '?';

      let cls = '';
      let icon = '';
      let detail = '';

      if (status === 'downloaded') {
        cls = 'ok'; icon = '✓';
      } else if (status === 'failed') {
        cls = 'fail'; icon = '✗';
        detail = evt.error ? ` (${evt.error})` : '';
      } else if (status === 'skipped') {
        cls = 'skip'; icon = '↷';
      } else if (status === 'rate_limited') {
        cls = 'warn'; icon = '⚠';
        detail = evt.message ? ` ${evt.message}` : '';
      } else if (status === 'warn') {
        cls = 'warn'; icon = '⚠';
        detail = evt.message ? ` ${evt.message}` : (evt.error ? ` (${evt.error})` : '');
      }

      appendLog(`${ts()} [${idx}/${total}] ${icon} ${gameId}${detail}`, cls);

      const c = evt.counts || {};
      updateProgress(c.downloaded || 0, c.failed || 0, c.skipped || 0, total);
    }
  };

  eventSource.onerror = function() {
    appendLog(`${ts()} [connection lost — will retry]`, 'warn');
  };
}

// ── User-Agent builder ────────────────────────────────────────────────────────
const UA_PLACEHOLDERS = {
  email: 'your@email.com',
  Discord: 'Username or @username',
  GitHub: 'github.com/username',
  other: 'your contact info',
};

function buildUserAgent() {
  const method = $('ua-method')?.value || 'email';
  const contact = $('ua-contact')?.value?.trim() || '';
  return `AOE4ReplayHarvest/0.1 (${method}: ${contact || '?'})`;
}

function updateUserAgent() {
  const method = $('ua-method')?.value || 'email';
  const contactEl = $('ua-contact');
  if (contactEl) contactEl.placeholder = UA_PLACEHOLDERS[method] || 'contact info';
  const preview = $('ua-preview');
  if (preview) preview.textContent = buildUserAgent();
}

function toggle429Sleep() {
  const val = $('on-429')?.value;
  const wrap = $('on-429-sleep-wrap');
  if (wrap) wrap.style.display = val === 'stop' ? 'none' : 'flex';
}

// auto-scroll toggle
document.addEventListener('DOMContentLoaded', () => {
  updateEstimate();
  const box = $('log-box');
  if (box) {
    box.addEventListener('scroll', () => {
      autoScroll = box.scrollTop + box.clientHeight >= box.scrollHeight - 10;
    });
  }

  // Poll current status on load in case a session is already running
  fetch('/api/status').then(r => r.json()).then(s => {
    if (s.running) {
      currentJobId = s.job_id;
      updateProgress(s.downloaded, s.failed, s.skipped, s.total);
      jobTotal = s.total;
      setRunning(true);
      connectSSE();
      appendLog(`${ts()} [resumed — session already running]`, 'info');
    }
  }).catch(() => {});
});

// ── Job file upload (friends / coordinator via file) ──────────────────────────
function onJobFileChange() {
  const input = $('job-file');
  if (!input || !input.files.length) return;
  const file = input.files[0];
  const reader = new FileReader();
  reader.onload = (e) => {
    try {
      currentJobContent = JSON.parse(e.target.result);
      currentJobId = null;  // uploaded file overrides coordinator shortcut
      setMsg('job-msg', `Loaded: ${currentJobContent.job_id || file.name}  (${currentJobContent.total_games || '?'} games)`, 'ok');
    } catch {
      currentJobContent = null;
      setMsg('job-msg', 'Failed to parse JSON file.', 'error');
    }
  };
  reader.readAsText(file);
}

// ── Coordinator: "Use this job now" ──────────────────────────────────────────
function useJobNow(jobId, totalGames) {
  currentJobId = jobId;
  currentJobContent = null;

  // Clear file input if present
  const fileInput = $('job-file');
  if (fileInput) fileInput.value = '';

  // Show loaded banner, hide upload input
  const banner = $('job-loaded-banner');
  const uploadWrap = $('job-upload-wrap');
  if (banner) banner.style.display = 'block';
  if (uploadWrap) uploadWrap.style.display = 'none';
  const label = $('job-loaded-label');
  if (label) label.textContent = `${jobId}  (${totalGames} games)`;

  setMsg('job-msg', '', '');
  // Scroll to section 2
  const s2 = $('s2');
  if (s2) s2.scrollIntoView({ behavior: 'smooth', block: 'start' });
}

function clearLoadedJob() {
  currentJobId = null;
  currentJobContent = null;
  const banner = $('job-loaded-banner');
  const uploadWrap = $('job-upload-wrap');
  if (banner) banner.style.display = 'none';
  if (uploadWrap) uploadWrap.style.display = 'block';
  setMsg('job-msg', '', '');
}

// ── Start / Pause ─────────────────────────────────────────────────────────────
async function startDownload() {
  if (!currentJobId && !currentJobContent) {
    setMsg('start-msg', 'No job loaded. Generate jobs above or upload a job file.', 'error');
    return;
  }

  const sleepMin = parseFloat($('sleep-min')?.value ?? 40);
  const sleepMax = parseFloat($('sleep-max')?.value ?? 70);
  const userAgent = buildUserAgent();
  const on429 = $('on-429')?.value || 'sleep';
  const on429Minutes = parseFloat($('on-429-minutes')?.value ?? 15);

  const body = {
    sleep_min: sleepMin,
    sleep_max: sleepMax,
    user_agent: userAgent,
    on_429: on429,
    on_429_minutes: on429Minutes,
  };
  if (currentJobId) {
    body.job_id = currentJobId;
  } else {
    body.job_content = currentJobContent;
  }

  setMsg('start-msg', 'Starting…', '');
  try {
    const resp = await fetch('/api/start', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const data = await resp.json();
    if (!resp.ok) {
      setMsg('start-msg', data.detail || 'Error starting download.', 'error');
      return;
    }
    currentJobId = data.job_id;
    jobTotal = data.total || 0;
    updateProgress(0, 0, 0, jobTotal);
    setRunning(true);
    setMsg('start-msg', `Running — job: ${data.job_id}`, '');
    const box = $('log-box');
    if (box) box.innerHTML = '';
    // Hide any previous sidecar button
    const sw = $('sidecar-wrap');
    if (sw) sw.style.display = 'none';
    connectSSE();
  } catch (err) {
    setMsg('start-msg', `Request failed: ${err}`, 'error');
  }
}

async function pauseDownload() {
  try {
    await fetch('/api/pause', { method: 'POST' });
    setMsg('start-msg', 'Pause requested…', '');
  } catch (err) {
    setMsg('start-msg', `Pause failed: ${err}`, 'error');
  }
}

// ── Discovery helpers ─────────────────────────────────────────────────────────

const TIER_LABELS = {
  'tier:top50': 'top50', 'tier:elite': 'elite', 'tier:high': 'high',
  'tier:mid': 'mid', 'tier:low_mid': 'low_mid', 'tier:low': 'low',
  'recent_player_games': 'legacy',
};

function formatTier(raw) {
  if (!raw) return '—';
  return TIER_LABELS[raw] || raw.replace('tier:', '');
}

function renderTierSummary(result, label) {
  const el = $('tier-summary');
  if (!el) return;
  const parts = [];
  if (result.top50) parts.push(`Top50: ${result.top50.games_found}`);
  for (const t of ['elite', 'high', 'mid', 'low_mid', 'low']) {
    if (result[t] != null) parts.push(`${t}: ${result[t].games_found}`);
  }
  const totalNew = result.total_new != null ? `  |  ${result.total_new} new` : '';
  const pending = result.total_pending != null ? ` / ${result.total_pending} pending` : '';
  el.textContent = parts.join('  |  ') + totalNew + pending + (label ? `  [${label}]` : '');
  el.style.display = 'block';
}

function renderDiscoveryTable(games) {
  const tbody = document.querySelector('#disc-table tbody');
  if (!tbody) return;
  tbody.innerHTML = '';
  const preview = games.slice(0, 20);
  for (const g of preview) {
    const tr = document.createElement('tr');
    tr.innerHTML = `<td>${g.game_id}</td><td>${g.profile_id ?? '—'}</td><td>${formatTier(g.tier)}</td><td>${g.map ?? '—'}</td><td>${g.season ?? '—'}</td><td>${(g.started_at || '—').slice(0, 19)}</td>`;
    tbody.appendChild(tr);
  }
  const sumEl = $('disc-summary');
  if (sumEl) {
    sumEl.textContent = `${games.length} games total${games.length > 20 ? ' (showing first 20)' : ''}`;
  }
}

function showBackToDiscovery() {
  let btn = $('back-to-discovery-btn');
  if (!btn) {
    btn = document.createElement('button');
    btn.id = 'back-to-discovery-btn';
    btn.textContent = '← Back to discovery results';
    btn.style.cssText = 'margin-top:8px;font-size:12px;';
    btn.onclick = () => {
      if (!discoverySnapshot) return;
      discoveredGames = discoverySnapshot.games;
      setMsg('disc-msg', `Restored discovery results. ${discoverySnapshot.total_new} new games found.`, 'ok');
      renderTierSummary(discoverySnapshot, 'discovery');
      renderDiscoveryTable(discoveredGames);
      btn.remove();
    };
    const msgEl = $('disc-msg');
    if (msgEl) msgEl.after(btn);
  }
}

// ── View mode helpers ─────────────────────────────────────────────────────────
// 'discovery' | 'pending' | 'assigned'
let currentViewMode = 'discovery';

function setViewMode(mode) {
  currentViewMode = mode;
  const assignedActions = $('assigned-actions');
  const splitControls = $('split-controls');
  if (assignedActions) assignedActions.style.display = mode === 'assigned' ? 'block' : 'none';
  if (splitControls)   splitControls.style.display   = mode !== 'assigned' ? 'block' : 'none';
}

// ── Coordinator: "Show Pending" ───────────────────────────────────────────────
async function showPending() {
  const btn = $('pending-btn');
  if (btn) btn.disabled = true;
  setMsg('disc-msg', 'Loading pending games from DB…', '');

  try {
    const resp = await fetch('/api/pending');
    const data = await resp.json();
    if (!resp.ok) {
      setMsg('disc-msg', data.detail || 'Failed to load pending.', 'error');
      return;
    }
    discoveredGames = data.games || [];
    setMsg('disc-msg', `${data.total} games pending (not yet downloaded).`, 'ok');
    renderTierSummary(data, 'pending');
    renderDiscoveryTable(discoveredGames);
    setViewMode('pending');
    $('disc-results').style.display = 'block';
    if (discoverySnapshot) showBackToDiscovery();
  } catch (err) {
    setMsg('disc-msg', `Error: ${err}`, 'error');
  } finally {
    if (btn) btn.disabled = false;
  }
}

// ── Coordinator: "Show Assigned" ─────────────────────────────────────────────
async function showAssigned() {
  const btn = $('assigned-btn');
  if (btn) btn.disabled = true;
  setMsg('disc-msg', 'Loading assigned games from DB…', '');

  try {
    const resp = await fetch('/api/assigned');
    const data = await resp.json();
    if (!resp.ok) {
      setMsg('disc-msg', data.detail || 'Failed to load assigned.', 'error');
      return;
    }
    discoveredGames = data.games || [];
    if (data.total === 0) {
      setMsg('disc-msg', 'No games currently assigned — nothing has been handed out yet.', '');
      $('disc-results').style.display = 'none';
      return;
    }
    setMsg('disc-msg', `${data.total} games currently assigned to active jobs.`, 'ok');
    const ts_ = $('tier-summary');
    if (ts_) ts_.style.display = 'none';
    renderDiscoveryTable(discoveredGames);
    setViewMode('assigned');
    setMsg('assigned-msg', '', '');
    $('disc-results').style.display = 'block';
    const old = $('back-to-discovery-btn');
    if (old) old.remove();
  } catch (err) {
    setMsg('disc-msg', `Error: ${err}`, 'error');
  } finally {
    if (btn) btn.disabled = false;
  }
}

async function useAssignedAsJob() {
  if (!discoveredGames.length) return;
  setMsg('assigned-msg', 'Saving job file to disk…', '');

  try {
    const resp = await fetch('/api/save-job', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        games: discoveredGames.map(g => ({ game_id: g.game_id, profile_id: g.profile_id })),
        group: 'recent_rm_1v1',
      }),
    });
    const data = await resp.json();
    if (!resp.ok) {
      setMsg('assigned-msg', data.detail || 'Failed to save job.', 'error');
      return;
    }

    // Track by job_id (file on disk) rather than in-memory content
    currentJobId = data.job_id;
    currentJobContent = null;

    const banner = $('job-loaded-banner');
    const uploadWrap = $('job-upload-wrap');
    if (banner) banner.style.display = 'block';
    if (uploadWrap) uploadWrap.style.display = 'none';
    const label = $('job-loaded-label');
    if (label) label.textContent = `${data.job_id}  (${data.total_games} games)`;
    setMsg('job-msg', `Job saved to disk as ${data.job_id}.json — you can re-upload this file to resume after a server restart.`, 'ok');
    setMsg('assigned-msg', '', '');

    const s2 = $('s2');
    if (s2) s2.scrollIntoView({ behavior: 'smooth', block: 'start' });
  } catch (err) {
    setMsg('assigned-msg', `Error: ${err}`, 'error');
  }
}

async function resetAssigned() {
  if (!discoveredGames.length) return;
  const gameIds = discoveredGames.map(g => g.game_id);
  setMsg('assigned-msg', `Resetting ${gameIds.length} games to pending…`, '');
  try {
    const resp = await fetch('/api/reset-assigned', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ game_ids: gameIds }),
    });
    const data = await resp.json();
    if (!resp.ok) {
      setMsg('assigned-msg', data.detail || 'Reset failed.', 'error');
      return;
    }
    setMsg('assigned-msg', `${data.reset} games reset to pending. They will appear in the next job generation.`, 'ok');
    discoveredGames = [];
    setTimeout(() => { $('disc-results').style.display = 'none'; }, 1500);
  } catch (err) {
    setMsg('assigned-msg', `Error: ${err}`, 'error');
  }
}

// ── Coordinator: Discovery ────────────────────────────────────────────────────
let discoverPollTimer = null;

const PHASE_NAMES = {
  starting: 'Preparing…',
  top50: 'Top 50 players ✓',
  elite: 'Elite tier ✓',
  high: 'High tier ✓',
  mid: 'Mid tier ✓',
  low_mid: 'Low-mid tier ✓',
  low: 'Low tier ✓',
};

const NEXT_PHASE = {
  starting: 'Fetching top 50 players…',
  top50: 'Sampling elite tier…',
  elite: 'Sampling high tier…',
  high: 'Sampling mid tier…',
  mid: 'Sampling low-mid tier…',
  low_mid: 'Sampling low tier…',
  low: 'Finalising…',
};

function formatDuration(seconds) {
  if (seconds < 60) return `~${seconds}s`;
  const m = Math.floor(seconds / 60);
  const s = seconds % 60;
  return s > 0 ? `~${m}m ${s}s` : `~${m}m`;
}

function updateEstimate() {
  const el = $('disc-estimate');
  if (!el) return;
  const target = parseInt($('disc-target')?.value || '100');
  const perPlayer = 25;  // backend default
  const sleep = parseFloat($('disc-sleep')?.value || '1.5');
  // Top50: 1 leaderboard + 50 player calls
  const top50Calls = 51;
  // Per tier: ~2 leaderboard pages + players needed (assume avg 40% of per_player games available)
  const avgGamesPerPlayer = Math.max(1, perPlayer * 0.4);
  const playersPerTier = Math.ceil(target / avgGamesPerPlayer);
  const callsPerTier = 2 + playersPerTier;
  const totalCalls = top50Calls + 5 * callsPerTier;
  const secs = Math.round(totalCalls * sleep);
  el.textContent = `Estimated runtime: ${formatDuration(secs)} (${totalCalls} API calls × ${sleep}s)`;
}

async function startDiscover() {
  const days = parseInt($('disc-days')?.value || '7');
  const targetPerTier = parseInt($('disc-target')?.value || '100');
  const sleepSeconds = parseFloat($('disc-sleep')?.value || '1.5');

  $('disc-btn').disabled = true;
  $('disc-results').style.display = 'none';
  const ts_ = $('tier-summary');
  if (ts_) ts_.style.display = 'none';

  // Show progress bar
  const pw = $('disc-progress-wrap');
  const pb = $('disc-prog-bar');
  const pl = $('disc-prog-label');
  if (pw) pw.style.display = 'block';
  if (pb) pb.style.width = '0%';
  if (pl) pl.textContent = 'Fetching top 50 players…';

  setMsg('disc-msg', 'Discovery running…', '');

  try {
    const resp = await fetch('/api/discover/start', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ days, target_per_tier: targetPerTier, sleep_seconds: sleepSeconds }),
    });
    if (!resp.ok) {
      const d = await resp.json();
      setMsg('disc-msg', d.detail || 'Failed to start discovery.', 'error');
      $('disc-btn').disabled = false;
      if (pw) pw.style.display = 'none';
      return;
    }
    discoverPollTimer = setInterval(pollDiscovery, 2000);
  } catch (err) {
    setMsg('disc-msg', `Error: ${err}`, 'error');
    $('disc-btn').disabled = false;
    if (pw) pw.style.display = 'none';
  }
}

async function pollDiscovery() {
  try {
    const resp = await fetch('/api/discover/status');
    const data = await resp.json();

    if (data.status === 'running') {
      // Update progress bar
      const done = data.phases_done || 0;
      const total = data.phases_total || 6;
      const pct = Math.round((done / total) * 100);
      const pb = $('disc-prog-bar');
      const pl = $('disc-prog-label');
      if (pb) pb.style.width = pct + '%';
      if (pl) {
        const completedLabel = PHASE_NAMES[data.phase] || data.phase || '';
        const nextLabel = NEXT_PHASE[data.phase] || '';
        pl.textContent = `${done}/${total} phases — ${completedLabel}${nextLabel ? ' · ' + nextLabel : ''}`;
      }
      return;
    }

    clearInterval(discoverPollTimer);
    discoverPollTimer = null;
    $('disc-btn').disabled = false;
    const pw = $('disc-progress-wrap');

    if (data.status === 'error') {
      setMsg('disc-msg', `Discovery failed: ${data.error}`, 'error');
      if (pw) pw.style.display = 'none';
      return;
    }

    if (pw) pw.style.display = 'none';
    const result = data.result;
    discoveredGames = result.games || [];
    discoverySnapshot = result;
    const old = $('back-to-discovery-btn');
    if (old) old.remove();
    setMsg('disc-msg', `Discovery complete — ${result.total_new} new games, ${result.total_pending} total pending.`, 'ok');
    renderTierSummary(result, 'discovery');
    renderDiscoveryTable(discoveredGames);
    setViewMode('discovery');
    $('disc-results').style.display = 'block';
  } catch (err) {
    clearInterval(discoverPollTimer);
    discoverPollTimer = null;
    setMsg('disc-msg', `Poll error: ${err}`, 'error');
    $('disc-btn').disabled = false;
  }
}


// ── Coordinator: Generate job files ──────────────────────────────────────────
async function generateJobs() {
  if (!discoveredGames.length) {
    setMsg('split-msg', 'Discover games first (or Show Pending).', 'error');
    return;
  }
  const k = parseInt($('split-k')?.value || '2');
  setMsg('split-msg', 'Generating…', '');
  try {
    const resp = await fetch('/api/generate-jobs', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        games: discoveredGames,
        splits: k,
        group: 'recent_rm_1v1',
      }),
    });
    const data = await resp.json();
    if (!resp.ok) {
      setMsg('split-msg', data.detail || 'Failed.', 'error');
      return;
    }
    setMsg('split-msg', `Generated ${data.jobs.length} job files.`, 'ok');
    renderJobList(data.jobs);
  } catch (err) {
    setMsg('split-msg', `Error: ${err}`, 'error');
  }
}

function renderJobList(jobs) {
  const list = $('job-list');
  if (!list) return;
  list.innerHTML = '';
  for (const job of jobs) {
    const item = document.createElement('div');
    item.className = 'job-item';
    item.innerHTML = `
      <span>${job.job_id} <em style="color:#888">(${job.total_games} games)</em></span>
      <a href="/api/jobs/${encodeURIComponent(job.job_id)}" download="${job.job_id}.json">
        <button>⬇ Download</button>
      </a>
      <button class="primary" onclick="useJobNow('${job.job_id}', ${job.total_games})">Use this job now →</button>
    `;
    list.appendChild(item);
  }
}

// ── Coordinator: Reconcile disk with DB ──────────────────────────────────────
async function reconcileDisk() {
  setMsg('reconcile-msg', 'Scanning replay folders…', '');
  try {
    const resp = await fetch('/api/reconcile-disk', { method: 'POST' });
    const data = await resp.json();
    if (!resp.ok) {
      setMsg('reconcile-msg', data.detail || 'Scan failed.', 'error');
      return;
    }
    const { scanned, newly_marked, already_known, pending_reduced } = data;
    if (scanned === 0) {
      setMsg('reconcile-msg', 'No replay files found on disk.', '');
    } else if (newly_marked === 0) {
      setMsg('reconcile-msg',
        `All ${scanned} files on disk are already recorded in the database — everything is aligned.`, 'ok');
    } else if (pending_reduced === 0) {
      setMsg('reconcile-msg',
        `Scanned ${scanned} files — ${newly_marked} newly recorded (${already_known} already known). ` +
        `None of these were in the pending list (downloaded outside the discovery system), so pending count is unchanged.`,
        'ok');
    } else {
      setMsg('reconcile-msg',
        `Scanned ${scanned} files — ${newly_marked} newly recorded, ${pending_reduced} removed from pending list. Click "Show Pending" to see the updated count.`,
        'ok');
    }
  } catch (err) {
    setMsg('reconcile-msg', `Request failed: ${err}`, 'error');
  }
}

// ── Coordinator: Import friend's progress ─────────────────────────────────────
async function importFriendProgress() {
  const input = $('import-file');
  if (!input || !input.files.length) {
    setMsg('import-msg', 'Select a .progress.json file first.', 'error');
    return;
  }
  const file = input.files[0];
  const reader = new FileReader();
  reader.onload = async (e) => {
    let sidecar;
    try {
      sidecar = JSON.parse(e.target.result);
    } catch {
      setMsg('import-msg', 'Failed to parse file.', 'error');
      return;
    }

    const jobId = sidecar.job_id || file.name.replace('.progress.json', '');
    const results = sidecar.results || {};
    const downloaded = [];
    const failed = [];
    for (const [gidStr, rec] of Object.entries(results)) {
      const gid = parseInt(gidStr);
      if (rec.status === 'downloaded') downloaded.push(gid);
      else if (rec.status === 'failed') failed.push(gid);
    }

    setMsg('import-msg', `Importing ${downloaded.length} downloaded + ${failed.length} failed…`, '');
    try {
      const resp = await fetch('/api/import-progress', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ job_id: jobId, downloaded, failed }),
      });
      const data = await resp.json();
      if (!resp.ok) {
        setMsg('import-msg', data.detail || 'Import failed.', 'error');
        return;
      }
      setMsg('import-msg',
        `Imported: ${data.imported_downloaded} downloaded, ${data.imported_failed} failed. They won't be re-queued.`,
        'ok');
    } catch (err) {
      setMsg('import-msg', `Request failed: ${err}`, 'error');
    }
  };
  reader.readAsText(file);
}
