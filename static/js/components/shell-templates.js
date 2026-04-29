(function () {
  const namespace = window.muFinancesComponents = window.muFinancesComponents || {};

  function chatSatellite() {
    return `
      <aside id="chatSatellite" class="chat-satellite hidden" aria-labelledby="chatSatelliteTitle" aria-live="polite">
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
      </aside>
    `;
  }

  function ensureChatSatellite() {
    if (document.getElementById('chatSatellite')) return;
    document.body.insertAdjacentHTML('beforeend', chatSatellite());
  }

  namespace.shellTemplates = {
    batch: 'B141',
    chatSatellite,
    ensureChatSatellite,
  };

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', ensureChatSatellite, { once: true });
  } else {
    ensureChatSatellite();
  }
})();
