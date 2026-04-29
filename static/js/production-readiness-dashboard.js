(function () {
  const buttonId = 'productionReadinessButton';
  const panelId = 'productionReadinessPanel';

  function commandBar() {
    return document.querySelector('#heroImportButton')?.closest('.hero-actions')
      || document.querySelector('#commandDeckToggle')?.parentElement
      || document.querySelector('.hero-actions');
  }

  function ensureStyles() {
    if (document.querySelector('#productionReadinessStyles')) return;
    const style = document.createElement('style');
    style.id = 'productionReadinessStyles';
    style.textContent = `
      #${buttonId} {
        border: 1px solid rgba(125, 240, 198, .35);
        border-radius: 8px;
        background: rgba(5, 24, 20, .84);
        color: #f4fffb;
        cursor: pointer;
        font: inherit;
        font-weight: 700;
        min-height: 42px;
        padding: 0 14px;
      }
      #${buttonId}.active {
        background: #7df0c6;
        border-color: #7df0c6;
        color: #04130f;
      }
      #${panelId} {
        background: #06120f;
        border: 1px solid rgba(125, 240, 198, .42);
        border-radius: 10px;
        box-shadow: 0 22px 54px rgba(0, 0, 0, .48);
        box-sizing: border-box;
        color: #f4fffb;
        display: none;
        gap: 12px;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        max-height: calc(100vh - 150px);
        overflow: auto;
        padding: 18px;
        position: fixed;
        right: 22px;
        top: 126px;
        width: min(760px, calc(100vw - 44px));
        z-index: 520;
      }
      #${panelId}.open { display: grid; }
      #${panelId} .readiness-heading { grid-column: 1 / -1; }
      #${panelId} .readiness-card {
        border: 1px solid rgba(125, 240, 198, .28);
        border-radius: 8px;
        padding: 12px;
      }
      #${panelId} .status-ok { color: #7df0c6; }
      #${panelId} .status-warning { color: #ffd166; }
      #${panelId} .status-blocked { color: #ff7a66; }
      @media (max-width: 760px) {
        #${panelId} { grid-template-columns: 1fr; }
      }
    `;
    document.head.append(style);
  }

  function localDashboard() {
    return {
      overall_status: 'warning',
      generated_at: new Date().toISOString(),
      components: [
        { name: 'Database mode', status: 'warning', detail: 'Verify PostgreSQL mode on server' },
        { name: 'Migration status', status: 'unknown', detail: 'API endpoint pending' },
        { name: 'Auth mode', status: 'warning', detail: 'Verify SSO/AD production wiring' },
        { name: 'Worker status', status: 'unknown', detail: 'Worker diagnostics endpoint pending' },
        { name: 'Backup status', status: 'unknown', detail: 'Backup proof endpoint pending' },
        { name: 'Health checks', status: 'unknown', detail: 'Health probes pending' },
        { name: 'Logs', status: 'unknown', detail: 'Structured log endpoint pending' },
        { name: 'Alerts', status: 'unknown', detail: 'Alert feed pending' },
      ],
    };
  }

  async function fetchDashboard() {
    try {
      const token = localStorage.getItem('mufinances.token') || '';
      const headers = { Accept: 'application/json' };
      if (token) headers.Authorization = `Bearer ${token}`;
      const csrfToken = sessionStorage.getItem('mufinances.csrf') || '';
      if (csrfToken) headers['X-CSRF-Token'] = csrfToken;
      const response = await fetch('/api/admin/production-readiness-dashboard', { headers, credentials: 'same-origin' });
      if (response.ok) return await response.json();
    } catch {
      // Fall back to local dashboard.
    }
    return localDashboard();
  }

  function render(panel, dashboard) {
    panel.replaceChildren();
    const heading = document.createElement('div');
    heading.className = 'readiness-heading';
    heading.innerHTML = `<h2>Production readiness</h2><p>Overall: ${dashboard.overall_status || 'unknown'} · ${dashboard.generated_at || ''}</p>`;
    panel.append(heading);

    (dashboard.components || []).forEach((component) => {
      const card = document.createElement('div');
      const status = String(component.status || 'unknown').toLowerCase();
      card.className = 'readiness-card';
      card.innerHTML = `
        <h3>${component.name || 'Component'}</h3>
        <strong class="status-${status}">${status}</strong>
        <p>${component.detail || ''}</p>
      `;
      panel.append(card);
    });
  }

  async function togglePanel() {
    const panel = document.querySelector(`#${panelId}`);
    const button = document.querySelector(`#${buttonId}`);
    if (!panel || !button) return;
    const open = !panel.classList.contains('open');
    panel.classList.toggle('open', open);
    button.classList.toggle('active', open);
    button.setAttribute('aria-expanded', String(open));
    if (open) render(panel, await fetchDashboard());
  }

  function install() {
    ensureStyles();
    const bar = commandBar();
    if (!bar || document.querySelector(`#${buttonId}`)) return;
    const button = document.createElement('button');
    button.id = buttonId;
    button.type = 'button';
    button.textContent = 'Production';
    button.setAttribute('aria-controls', panelId);
    button.setAttribute('aria-expanded', 'false');
    const signpost = document.querySelector('#workspaceMenuButton') || document.querySelector('#commandDeckToggle');
    bar.insertBefore(button, signpost || bar.firstChild);

    const panel = document.createElement('aside');
    panel.id = panelId;
    panel.setAttribute('aria-label', 'Production readiness dashboard');
    document.body.append(panel);
  }

  document.addEventListener('click', (event) => {
    if (event.target instanceof Element && event.target.closest(`#${buttonId}`)) {
      event.preventDefault();
      togglePanel();
    }
  });
  document.addEventListener('DOMContentLoaded', install);
  setTimeout(install, 0);
  setTimeout(install, 800);
  setInterval(install, 2500);
})();
