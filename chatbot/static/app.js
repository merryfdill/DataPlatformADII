/* ADII Assistant - frontend only (Phase 2.26 visual redesign).
 * Same backend contract as before: POST /api/chat { message, history } ->
 * { reply, tool_calls }. No new endpoints, no fabricated data - every number
 * rendered here comes straight from that response.
 */

const threadEl = document.getElementById('thread');
const conversationWrap = document.getElementById('conversationWrap');
const formEl = document.getElementById('form');
const inputEl = document.getElementById('input');
const sendBtn = document.getElementById('sendBtn');
const errorBanner = document.getElementById('errorBanner');
const newConvBtn = document.getElementById('newConvBtn');
const suggestionCards = document.querySelectorAll('.suggestion-card');
const helpBtn = document.getElementById('helpBtn');
const helpBackdrop = document.getElementById('helpBackdrop');
const helpClose = document.getElementById('helpClose');
const notifBtn = document.getElementById('notifBtn');

let history = [];
let dayDividerShown = false;

/* ---------- Minimal, safe Markdown rendering ---------- */

function escapeHtml(str) {
  return str
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

function inlineFormat(escaped) {
  return escaped
    .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
    .replace(/`([^`]+)`/g, '<code>$1</code>')
    .replace(/(^|[^*])\*([^*]+)\*/g, '$1<em>$2</em>');
}

function verdictCellHtml(rawText) {
  const text = escapeHtml(rawText.trim());
  const upper = text.toUpperCase();
  if (upper.includes('NORMAL')) return '<span class="badge-dot normal"></span>' + text;
  if (upper.includes('MINORE') || upper.includes('MINORÉ')) return '<span class="badge-dot minore"></span>' + text;
  if (upper.includes('MAJORE') || upper.includes('MAJORÉ')) return '<span class="badge-dot majore"></span>' + text;
  return text;
}

function pctCellClass(rawText) {
  // Colors a percentage cell based on sibling verdict wording elsewhere in the row - best effort, purely cosmetic.
  return '';
}

function renderTable(lines) {
  const rows = lines
    .map(l => l.trim())
    .filter(l => l.length)
    .filter(l => !/^\|?[\s:|-]+\|?$/.test(l)); // drop the "|---|---|" separator row

  if (!rows.length) return '';

  const splitRow = (line) => {
    let cells = line.trim();
    if (cells.startsWith('|')) cells = cells.slice(1);
    if (cells.endsWith('|')) cells = cells.slice(0, -1);
    return cells.split('|').map(c => c.trim());
  };

  const header = splitRow(rows[0]);
  const bodyRows = rows.slice(1).map(splitRow);

  let html = '<div class="data-table-wrap"><table class="data-table"><thead><tr>';
  header.forEach(h => { html += '<th>' + inlineFormat(escapeHtml(h)) + '</th>'; });
  html += '</tr></thead><tbody>';

  bodyRows.forEach(cells => {
    const isTotal = (cells[0] || '').trim().toUpperCase().startsWith('TOTAL');
    html += '<tr class="' + (isTotal ? 'total-row' : '') + '">';
    cells.forEach((c, idx) => {
      const content = idx === 0 ? verdictCellHtml(c) : inlineFormat(escapeHtml(c));
      html += '<td>' + content + '</td>';
    });
    html += '</tr>';
  });

  html += '</tbody></table></div>';
  return html;
}

function renderMarkdown(raw) {
  const lines = (raw || '').replace(/\r\n/g, '\n').split('\n');
  let html = '';
  let i = 0;
  let paragraphBuf = [];
  let listBuf = [];

  const flushParagraph = () => {
    if (paragraphBuf.length) {
      html += '<p>' + inlineFormat(escapeHtml(paragraphBuf.join(' '))) + '</p>';
      paragraphBuf = [];
    }
  };
  const flushList = () => {
    if (listBuf.length) {
      html += '<ul>' + listBuf.map(li => '<li>' + inlineFormat(escapeHtml(li)) + '</li>').join('') + '</ul>';
      listBuf = [];
    }
  };

  while (i < lines.length) {
    const line = lines[i];
    const trimmed = line.trim();

    if (trimmed.startsWith('|')) {
      flushParagraph(); flushList();
      const tableLines = [];
      while (i < lines.length && lines[i].trim().startsWith('|')) {
        tableLines.push(lines[i]);
        i++;
      }
      html += renderTable(tableLines);
      continue;
    }

    if (/^(---|\*\*\*|___)$/.test(trimmed)) {
      flushParagraph(); flushList();
      html += '<hr class="bubble-hr">';
      i++;
      continue;
    }

    const headingMatch = trimmed.match(/^#{2,4}\s+(.*)$/);
    if (headingMatch) {
      flushParagraph(); flushList();
      html += '<h4>' + inlineFormat(escapeHtml(headingMatch[1])) + '</h4>';
      i++;
      continue;
    }

    const listMatch = trimmed.match(/^[-*]\s+(.*)$/);
    if (listMatch) {
      flushParagraph();
      listBuf.push(listMatch[1]);
      i++;
      continue;
    }
    flushList();

    if (trimmed === '') {
      flushParagraph();
      i++;
      continue;
    }

    paragraphBuf.push(trimmed);
    i++;
  }
  flushParagraph();
  flushList();
  return html || '<p></p>';
}

/* ---------- Conversation rendering ---------- */

function ensureDayDivider() {
  if (dayDividerShown) return;
  const div = document.createElement('div');
  div.className = 'day-divider';
  div.textContent = "Aujourd'hui";
  threadEl.appendChild(div);
  dayDividerShown = true;
}

function avatarSvg(kind) {
  if (kind === 'assistant') {
    return '<img src="Douanes_Marocaines_ADII_Charaf_2015.jpg" alt="ADII" />';
  }
  return '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><circle cx="12" cy="8" r="3.2"/><path d="M5 20c0-3.6 3-6 7-6s7 2.4 7 6" stroke-linecap="round"/></svg>';
}

function sourceIconSvg() {
  return '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><ellipse cx="12" cy="6" rx="7" ry="2.6"/><path d="M5 6v6c0 1.4 3.1 2.6 7 2.6s7-1.2 7-2.6V6"/><path d="M5 12v6c0 1.4 3.1 2.6 7 2.6s7-1.2 7-2.6v-6"/></svg>';
}

function addMessage(role, content, toolCalls) {
  ensureDayDivider();

  const row = document.createElement('div');
  row.className = 'msg-row ' + (role === 'user' ? 'user' : 'assistant');

  const avatar = document.createElement('div');
  avatar.className = 'msg-avatar';
  avatar.innerHTML = avatarSvg(role);

  const col = document.createElement('div');
  col.className = 'msg-col';

  const bubble = document.createElement('div');
  bubble.className = 'bubble';
  if (role === 'user') {
    bubble.textContent = content;
  } else {
    bubble.innerHTML = renderMarkdown(content);
  }
  col.appendChild(bubble);

  if (role === 'assistant' && toolCalls && toolCalls.length) {
    const sources = [...new Set(toolCalls.map(tc => tc.source).filter(Boolean))];
    const meta = document.createElement('div');
    meta.className = 'msg-meta';
    sources.forEach(src => {
      const chip = document.createElement('span');
      chip.className = 'source-chip';
      chip.innerHTML = sourceIconSvg() + '<span>Source : ' + src + '</span>';
      meta.appendChild(chip);
    });
    col.appendChild(meta);

    const det = document.createElement('details');
    det.className = 'tool-details';
    const sum = document.createElement('summary');
    sum.textContent = 'Details techniques (' + toolCalls.length + ' outil(s) appele(s))';
    det.appendChild(sum);
    toolCalls.forEach(tc => {
      const box = document.createElement('div');
      box.className = 'tool-box';
      let html = '<strong>' + tc.tool + '</strong>';
      if (tc.sql) html += '<code>' + tc.sql + '</code>';
      if (tc.error) html += '<div class="tool-error">Erreur : ' + tc.error + '</div>';
      box.innerHTML = html;
      det.appendChild(box);
    });
    col.appendChild(det);
  }

  row.appendChild(avatar);
  row.appendChild(col);
  threadEl.appendChild(row);
  scrollToBottom();
}

function showTyping() {
  ensureDayDivider();
  const row = document.createElement('div');
  row.className = 'msg-row assistant typing-row';
  row.id = 'typingRow';
  row.innerHTML =
    '<div class="msg-avatar">' + avatarSvg('assistant') + '</div>' +
    '<div class="typing-dots"><span></span><span></span><span></span></div>';
  threadEl.appendChild(row);
  scrollToBottom();
}

function hideTyping() {
  const row = document.getElementById('typingRow');
  if (row) row.remove();
}

function scrollToBottom() {
  conversationWrap.scrollTop = conversationWrap.scrollHeight;
}

function showError(message) {
  errorBanner.textContent = message;
  errorBanner.style.display = 'block';
}

function hideError() {
  errorBanner.style.display = 'none';
}

/* ---------- Sending messages ---------- */

async function sendMessage(message) {
  if (!message.trim()) return;
  hideError();
  addMessage('user', message);
  inputEl.value = '';
  autoResizeInput();
  sendBtn.disabled = true;
  showTyping();

  try {
    const res = await fetch('/api/chat', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message: message, history: history }),
    });
    if (!res.ok) {
      const text = await res.text();
      throw new Error('Erreur serveur (' + res.status + ') : ' + text.slice(0, 200));
    }
    const data = await res.json();
    hideTyping();
    addMessage('assistant', data.reply, data.tool_calls);
    history.push({ role: 'user', content: message });
    history.push({ role: 'assistant', content: data.reply });
  } catch (err) {
    hideTyping();
    showError('Erreur : ' + err.message);
  } finally {
    sendBtn.disabled = false;
    inputEl.focus();
  }
}

formEl.addEventListener('submit', (e) => {
  e.preventDefault();
  sendMessage(inputEl.value);
});

inputEl.addEventListener('keydown', (e) => {
  if (e.key === 'Enter' && !e.shiftKey) {
    e.preventDefault();
    sendMessage(inputEl.value);
  }
});

function autoResizeInput() {
  inputEl.style.height = 'auto';
  inputEl.style.height = Math.min(inputEl.scrollHeight, 140) + 'px';
}
inputEl.addEventListener('input', autoResizeInput);

suggestionCards.forEach(card => {
  card.addEventListener('click', () => {
    const question = card.getAttribute('data-question');
    if (question) sendMessage(question);
  });
});

newConvBtn.addEventListener('click', () => {
  history = [];
  dayDividerShown = false;
  threadEl.innerHTML = '';
  hideError();
  inputEl.value = '';
  autoResizeInput();
  inputEl.focus();
});

notifBtn.addEventListener('click', () => {
  notifBtn.classList.toggle('open');
});

helpBtn.addEventListener('click', () => helpBackdrop.classList.add('open'));
helpClose.addEventListener('click', () => helpBackdrop.classList.remove('open'));
helpBackdrop.addEventListener('click', (e) => {
  if (e.target === helpBackdrop) helpBackdrop.classList.remove('open');
});

inputEl.focus();
