(function () {
  const state = {
    users: [],
    peerUserId: Number(localStorage.getItem('mufinances.chat.peerUserId') || '0') || null,
    loading: false,
  };

  const $ = (selector) => document.querySelector(selector);

  function token() {
    return localStorage.getItem('mufinances.token') || '';
  }

  function authHeaders() {
    const currentToken = token();
    return currentToken ? { Authorization: `Bearer ${currentToken}` } : {};
  }

  async function safeJson(response) {
    try {
      return await response.json();
    } catch {
      return {};
    }
  }

  async function apiGet(path) {
    const response = await fetch(path, { headers: authHeaders() });
    if (!response.ok) throw new Error((await safeJson(response)).detail || 'Chat request failed.');
    return response.json();
  }

  async function apiPost(path, payload) {
    const response = await fetch(path, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...authHeaders() },
      body: JSON.stringify(payload || {}),
    });
    if (!response.ok) throw new Error((await safeJson(response)).detail || 'Chat request failed.');
    return response.json();
  }

  function escapeHtml(value) {
    return String(value ?? '').replace(/[&<>"']/g, (char) => ({
      '&': '&amp;',
      '<': '&lt;',
      '>': '&gt;',
      '"': '&quot;',
      "'": '&#39;',
    })[char]);
  }

  function setStatus(message) {
    const status = $('#chatWindowStatus');
    if (status) status.textContent = message || '';
  }

  function renderMessages(messages) {
    const container = $('#chatWindowMessages');
    if (!container) return;
    if (!messages.length) {
      container.innerHTML = '<p class="muted">No messages yet.</p>';
      return;
    }
    container.innerHTML = messages.map((message) => {
      const name = message.direction === 'sent' ? 'You' : (message.sender_name || message.sender_email || 'User');
      return `
        <article class="chat-message ${message.direction}">
          <strong>${escapeHtml(name)}</strong>
          <span>${escapeHtml(message.body)}</span>
          <time datetime="${escapeHtml(message.sent_at)}">${escapeHtml(new Date(message.sent_at).toLocaleString())}</time>
        </article>
      `;
    }).join('');
    container.scrollTop = container.scrollHeight;
  }

  async function loadUsers() {
    const payload = await apiGet('/api/chat/users');
    state.users = payload.users || [];
    const select = $('#chatWindowRecipientSelect');
    if (!select) return;
    const current = state.peerUserId || Number(select.value || '0') || null;
    const preferred = state.users.find((user) => user.unread_count > 0)?.id || current || state.users[0]?.id || null;
    state.peerUserId = preferred;
    if (state.peerUserId) localStorage.setItem('mufinances.chat.peerUserId', String(state.peerUserId));
    select.innerHTML = state.users.length
      ? state.users.map((user) => {
        const unread = user.unread_count ? ` (${user.unread_count} unread)` : '';
        return `<option value="${user.id}">${escapeHtml(user.display_name || user.email)}${unread}</option>`;
      }).join('')
      : '<option value="">No other users available</option>';
    select.disabled = state.users.length === 0;
    if (state.peerUserId) select.value = String(state.peerUserId);
  }

  async function loadMessages() {
    if (!state.peerUserId) {
      renderMessages([]);
      setStatus('Create another active user to start a chat.');
      return;
    }
    const payload = await apiGet(`/api/chat/messages?peer_user_id=${encodeURIComponent(state.peerUserId)}&limit=100`);
    renderMessages(payload.messages || []);
    await apiPost('/api/chat/messages/read', { peer_user_id: state.peerUserId });
  }

  async function refreshChat() {
    if (state.loading) return;
    if (!token()) {
      renderMessages([]);
      setStatus('Sign in to muFinances in the main window first.');
      return;
    }
    state.loading = true;
    try {
      await loadUsers();
      await loadMessages();
      setStatus('Ready');
    } catch (error) {
      setStatus(error.message);
    } finally {
      state.loading = false;
    }
  }

  async function sendMessage() {
    const input = $('#chatWindowMessageInput');
    const body = input?.value.trim() || '';
    if (!body || !state.peerUserId) return;
    await apiPost('/api/chat/messages', { recipient_user_id: state.peerUserId, body });
    input.value = '';
    await refreshChat();
  }

  function wire() {
    $('#chatWindowRefreshButton')?.addEventListener('click', () => refreshChat());
    $('#chatWindowSendButton')?.addEventListener('click', () => sendMessage().catch((error) => setStatus(error.message)));
    $('#chatWindowRecipientSelect')?.addEventListener('change', (event) => {
      state.peerUserId = Number(event.target.value || '0') || null;
      if (state.peerUserId) localStorage.setItem('mufinances.chat.peerUserId', String(state.peerUserId));
      refreshChat();
    });
    $('#chatWindowMessageInput')?.addEventListener('keydown', (event) => {
      if (event.key !== 'Enter' || event.shiftKey) return;
      event.preventDefault();
      sendMessage().catch((error) => setStatus(error.message));
    });
  }

  document.addEventListener('DOMContentLoaded', () => {
    wire();
    refreshChat();
    setInterval(refreshChat, 5000);
  });
})();
