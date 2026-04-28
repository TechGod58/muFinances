(function () {
  const state = {
    users: [],
    peerUserId: null,
    lastNotifiedId: Number(localStorage.getItem('mufinances.chat.lastNotifiedId') || '0'),
    loading: false,
  };

  const $ = (selector) => document.querySelector(selector);

  function commandBar() {
    return $('#heroImportButton')?.closest('.hero-actions')
      || $('#commandDeckToggle')?.parentElement
      || $('.hero-actions');
  }

  function ensureChatButton() {
    const bar = commandBar();
    if (!bar) return null;
    let button = $('#chatButton');
    if (!button) {
      button = document.createElement('button');
      button.id = 'chatButton';
      button.type = 'button';
      button.textContent = 'Chat';
      button.setAttribute('aria-controls', 'chatSatellite');
      button.setAttribute('aria-expanded', 'false');
    }
    if (!button.dataset.chatPopupBound) {
      button.dataset.chatPopupBound = 'true';
      button.addEventListener('click', (event) => {
        event.preventDefault();
        event.stopPropagation();
        if (event.stopImmediatePropagation) event.stopImmediatePropagation();
        openChat().catch((error) => {
          setStatus(error.message);
          toast(error.message);
        });
      }, true);
    }

    const market = $('#marketWatchButton');
    const production = $('#productionReadinessButton');
    const workspace = $('#workspaceMenuButton');
    const openHide = $('#commandDeckToggle');
    const signpost = production || workspace || openHide || null;

    if (market?.parentElement === bar && market.nextElementSibling !== button) {
      bar.insertBefore(button, market.nextElementSibling);
    } else if (button.parentElement !== bar) {
      bar.insertBefore(button, signpost || null);
    } else if (signpost && button.compareDocumentPosition(signpost) & Node.DOCUMENT_POSITION_PRECEDING) {
      bar.insertBefore(button, signpost);
    }

    return button;
  }

  function ensureChatPanel() {
    let panel = $('#chatSatellite');
    if (panel) return panel;
    panel = document.createElement('aside');
    panel.id = 'chatSatellite';
    panel.className = 'chat-satellite hidden';
    panel.setAttribute('aria-labelledby', 'chatSatelliteTitle');
    panel.setAttribute('aria-live', 'polite');
    panel.innerHTML = `
      <div class="chat-satellite-header">
        <div>
          <p class="eyebrow">Direct message</p>
          <h2 id="chatSatelliteTitle">muFinances Chat</h2>
        </div>
        <button id="chatCloseButton" type="button" aria-label="Close chat">Close</button>
      </div>
      <div class="chat-recipient-row">
        <label for="chatRecipientSelect">Send to</label>
        <select id="chatRecipientSelect"></select>
        <button id="chatRefreshButton" type="button">Refresh</button>
      </div>
      <div id="chatMessages" class="chat-messages" role="log" aria-live="polite" aria-label="Chat messages"></div>
      <div class="chat-compose">
        <textarea id="chatMessageInput" rows="3" maxlength="2000" aria-label="Chat message" placeholder="Type a message"></textarea>
        <button id="chatSendButton" class="primary" type="button">Send</button>
      </div>
      <p id="chatStatus" class="muted" role="status" aria-live="polite"></p>
    `;
    document.body.append(panel);
    return panel;
  }

  function ensureChrome() {
    ensureChatButton();
    ensureChatPanel();
  }

  function token() {
    return localStorage.getItem('mufinances.token') || '';
  }

  function authHeaders() {
    const currentToken = token();
    return currentToken ? { Authorization: `Bearer ${currentToken}` } : {};
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

  async function safeJson(response) {
    try {
      return await response.json();
    } catch {
      return {};
    }
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

  function toast(message) {
    const toastEl = $('#toast');
    if (!toastEl) return;
    toastEl.textContent = message;
    toastEl.setAttribute('aria-label', message);
    toastEl.classList.remove('hidden');
    clearTimeout(toastEl._chatTimeout);
    toastEl._chatTimeout = setTimeout(() => toastEl.classList.add('hidden'), 3200);
  }

  function signedIn() {
    const appShell = $('#appShell');
    const authGate = $('#authGate');
    return Boolean(token() && appShell && !appShell.classList.contains('hidden') && (!authGate || authGate.classList.contains('hidden')));
  }

  function setStatus(message) {
    const status = $('#chatStatus');
    if (status) status.textContent = message || '';
  }

  function presenceLabel(user) {
    const presence = user?.presence || {};
    if (presence.status === 'online') return 'online';
    if (presence.last_seen_at) return `last seen ${new Date(presence.last_seen_at).toLocaleString()}`;
    return 'offline';
  }

  function deliveryLabel(message) {
    if (message.direction !== 'sent') return '';
    if (message.delivery_status === 'pending_delivery') return 'Queued until recipient signs in';
    if (message.delivery_status === 'delivered') return 'Delivered';
    return message.delivery_status || '';
  }

  function setChatOpen(open) {
    ensureChrome();
    const panel = $('#chatSatellite');
    const button = $('#chatButton');
    if (!panel || !button) return;
    panel.classList.toggle('hidden', !open);
    button.classList.toggle('primary', open);
    button.setAttribute('aria-expanded', String(open));
    localStorage.setItem('mufinances.chat.open', open ? 'true' : 'false');
  }

  function updateChatButton(unreadCount) {
    ensureChatButton();
    const button = $('#chatButton');
    if (!button) return;
    const popupOpen = Boolean(window.muFinancesChatWindow && !window.muFinancesChatWindow.closed);
    const open = popupOpen || !$('#chatSatellite')?.classList.contains('hidden');
    button.textContent = unreadCount > 0 ? `Chat (${unreadCount})` : 'Chat';
    button.classList.toggle('primary', open || unreadCount > 0);
    button.setAttribute('aria-label', unreadCount > 0 ? `Chat, ${unreadCount} unread messages` : 'Chat');
  }

  async function loadUsers() {
    const payload = await apiGet('/api/chat/users');
    state.users = payload.users || [];
    const select = $('#chatRecipientSelect');
    if (!select) return;
    const current = state.peerUserId || Number(select.value || '0');
    const preferred = state.users.find((user) => user.unread_count > 0)?.id || current || state.users[0]?.id || null;
    state.peerUserId = preferred;
    select.innerHTML = state.users.length
      ? state.users.map((user) => {
        const unread = user.unread_count ? ` (${user.unread_count} unread)` : '';
        return `<option value="${user.id}">${escapeHtml(user.display_name || user.email)} - ${escapeHtml(presenceLabel(user))}${unread}</option>`;
      }).join('')
      : '<option value="">No other users available</option>';
    select.disabled = state.users.length === 0;
    if (state.peerUserId) select.value = String(state.peerUserId);
  }

  async function loadMessages({ markRead = false } = {}) {
    if (!state.peerUserId) {
      $('#chatMessages').innerHTML = '<p class="muted">Create another active user to start a chat.</p>';
      return;
    }
    const payload = await apiGet(`/api/chat/messages?peer_user_id=${encodeURIComponent(state.peerUserId)}&limit=100`);
    renderMessages(payload.messages || []);
    if (markRead) {
      await apiPost('/api/chat/messages/read', { peer_user_id: state.peerUserId });
    }
  }

  function renderMessages(messages) {
    const container = $('#chatMessages');
    if (!container) return;
    if (!messages.length) {
      container.innerHTML = '<p class="muted">No messages yet.</p>';
      return;
    }
    container.innerHTML = messages.map((message) => {
      const name = message.direction === 'sent' ? 'You' : (message.sender_name || message.sender_email || 'User');
      const delivery = deliveryLabel(message);
      return `
        <article class="chat-message ${message.direction}">
          <strong>${escapeHtml(name)}</strong>
          <span>${escapeHtml(message.body)}</span>
          <time datetime="${escapeHtml(message.sent_at)}">${escapeHtml(new Date(message.sent_at).toLocaleString())}</time>
          ${delivery ? `<small class="chat-delivery">${escapeHtml(delivery)}</small>` : ''}
        </article>
      `;
    }).join('');
    container.scrollTop = container.scrollHeight;
  }

  async function refreshChat({ notify = false, markRead = false } = {}) {
    ensureChrome();
    if (!signedIn() || state.loading) return;
    state.loading = true;
    try {
      await loadUsers();
      await loadMessages({ markRead });
      const summary = await apiGet('/api/chat/summary');
      updateChatButton(Number(summary.unread_count || 0));
      if (notify && summary.latest_unread && Number(summary.latest_unread.id) > state.lastNotifiedId) {
        state.lastNotifiedId = Number(summary.latest_unread.id);
        localStorage.setItem('mufinances.chat.lastNotifiedId', String(state.lastNotifiedId));
        toast(`New chat from ${summary.latest_unread.sender_name || summary.latest_unread.sender_email}`);
      }
      setStatus('Ready');
    } catch (error) {
      setStatus(error.message);
    } finally {
      state.loading = false;
    }
  }

  function openChatPopup() {
    const features = [
      'popup=yes',
      'width=620',
      'height=760',
      'left=120',
      'top=80',
      'resizable=yes',
      'scrollbars=yes',
    ].join(',');
    const popup = window.open('/static/chat-window.html?v=2', 'muFinancesChat', features);
    if (!popup) return null;
    window.muFinancesChatWindow = popup;
    try {
      popup.moveTo(120, 80);
      popup.resizeTo(620, 760);
    } catch (_error) {
      // Browser security settings may block scripted sizing; the window is still user-movable.
    }
    popup.focus();
    localStorage.setItem('mufinances.chat.open', 'popup');
    return popup;
  }

  async function openChat() {
    const popup = openChatPopup();
    if (popup) {
      setChatOpen(false);
      await refreshChat({ notify: false, markRead: false });
      return;
    }
    setChatOpen(false);
    toast('Browser blocked the Chat pop-out. Allow pop-ups for localhost:3200, then press Chat again.');
  }

  function closeChat() {
    setChatOpen(false);
  }

  async function sendChatMessage() {
    const input = $('#chatMessageInput');
    const body = input?.value.trim() || '';
    if (!body || !state.peerUserId) return;
    await apiPost('/api/chat/messages', { recipient_user_id: state.peerUserId, body });
    input.value = '';
    await refreshChat({ markRead: true });
    toast('Chat message sent.');
  }

  function wire() {
    document.addEventListener('click', (event) => {
      const target = event.target;
      if (!(target instanceof Element)) return;
      if (target.closest('#chatButton')) {
        event.preventDefault();
        event.stopPropagation();
        return;
      }
      if (target.closest('#chatCloseButton')) {
        event.preventDefault();
        closeChat();
        return;
      }
      if (target.closest('#chatRefreshButton')) {
        event.preventDefault();
        refreshChat({ markRead: true }).catch((error) => setStatus(error.message));
        return;
      }
      if (target.closest('#chatSendButton')) {
        event.preventDefault();
        sendChatMessage().catch((error) => {
          setStatus(error.message);
          toast(error.message);
        });
      }
    });
    document.addEventListener('change', (event) => {
      if (!event.target?.matches?.('#chatRecipientSelect')) return;
      state.peerUserId = Number(event.target.value || '0') || null;
      refreshChat({ markRead: true }).catch((error) => setStatus(error.message));
    });
    document.addEventListener('keydown', (event) => {
      if (!event.target?.matches?.('#chatMessageInput')) return;
      if (event.key !== 'Enter' || event.shiftKey) return;
      event.preventDefault();
      sendChatMessage().catch((error) => {
        setStatus(error.message);
        toast(error.message);
      });
    });
  }

  async function boot() {
    ensureChrome();
    wire();
    setChatOpen(localStorage.getItem('mufinances.chat.open') === 'true' && signedIn());
    await refreshChat({ notify: true, markRead: !$('#chatSatellite')?.classList.contains('hidden') });
    setInterval(() => {
      ensureChrome();
      const popupOpen = Boolean(window.muFinancesChatWindow && !window.muFinancesChatWindow.closed);
      const open = popupOpen || !$('#chatSatellite')?.classList.contains('hidden');
      refreshChat({ notify: true, markRead: open }).catch(() => {});
    }, 5000);
    setInterval(ensureChrome, 1200);
  }

  document.addEventListener('DOMContentLoaded', () => {
    boot().catch((error) => setStatus(error.message));
  });
})();
