(function () {
  const buttonId = 'workspaceMenuButton';
  const trayId = 'workspaceToggleTray';
  const emptyStateId = 'workspaceEmptyState';
  const activeClass = 'workspace-toggle-active';
  const openClass = 'workspace-tray-open';
  const storageKey = 'mufinances.workspace.fallback.activeSections.v2';
  const numberStorageKey = 'mufinances.workspace.fallback.activeNumbers.v2';
  const menuOpenKey = 'mufinances.workspace.fallback.menuOpen';
  const changedThisSessionKey = 'mufinances.workspace.fallback.changedThisSession';

  const groups = [
    ['01 Start', [1, 2, 3, 4, 5, 6]],
    ['02 Planning', [7, 8, 9, 10, 11]],
    ['03 Finance Operations', [12, 13, 14, 15]],
    ['04 Data And Automation', [16, 17, 18, 19]],
    ['05 Administration', [20, 21, 22]],
    ['06 Extended Workspaces', [23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34]],
  ];

  const tokens = {
    1: ['what do you want to do first', 'guided data entry'],
    2: ['guidance and finance training'],
    3: ['role workspaces'],
    4: ['ux productivity'],
    5: ['parallel cubed ledger map'],
    6: ['ledger depth and actuals'],
    7: ['operating budget workspace'],
    8: ['enrollment and tuition planning'],
    9: ['workforce, faculty, grants, and capital'],
    10: ['forecast and scenario engine'],
    11: ['model builder and allocations'],
    12: ['reporting and analytics'],
    13: ['close, reconciliation, and consolidation'],
    14: ['budget lines'],
    15: ['planning drivers'],
    16: ['workflow and process orchestration'],
    17: ['campus integrations'],
    18: ['governed automation'],
    19: ['ai explainability'],
    20: ['deployment operations'],
    21: ['compliance and audit hardening'],
    22: ['comments, attachments, and evidence'],
  };

  const titleToNumber = [
    [1, ['what do you want to do first']],
    [2, ['guidance and finance training']],
    [3, ['role workspaces']],
    [4, ['ux productivity']],
    [5, ['parallel cubed ledger map']],
    [6, ['ledger depth and actuals']],
    [7, ['operating budget workspace']],
    [8, ['enrollment and tuition planning']],
    [9, ['workforce, faculty, grants, and capital']],
    [10, ['forecast and scenario engine']],
    [11, ['model builder and allocations']],
    [12, ['reporting and analytics']],
    [13, ['close, reconciliation, and consolidation']],
    [14, ['budget lines']],
    [15, ['planning drivers']],
    [16, ['workflow and process orchestration']],
    [17, ['campus integrations']],
    [18, ['governed automation']],
    [19, ['ai explainability']],
    [20, ['deployment operations']],
    [21, ['compliance and audit hardening']],
    [22, ['comments, attachments, and evidence']],
    [23, ['excel and office interop']],
    [24, ['profitability and allocation management']],
    [25, ['data hub and master data governance']],
    [26, ['connector marketplace depth']],
    [27, ['ai planning agents']],
    [28, ['predictive forecasting studio']],
    [29, ['advanced consolidation and statutory reporting']],
    [30, ['tax classification and compliance watch']],
    [31, ['real chart rendering and export engine']],
    [32, ['market watch and paper trading lab']],
    [33, ['brokerage connection']],
    [34, ['university agent integration']],
  ];

  function normalized(value) {
    return String(value || '').toLowerCase().replace(/\s+/g, ' ').trim();
  }

  function signedIn() {
    const appShell = document.querySelector('#appShell');
    const authGate = document.querySelector('#authGate');
    const authVisible = Boolean(authGate && !authGate.hidden && authGate.offsetParent !== null);
    if (authVisible) return false;
    if (appShell) return !appShell.hidden;
    return Boolean(openHideButton() && commandBar());
  }

  function commandBar() {
    const openHide = openHideButton();
    return document.querySelector('#heroImportButton')?.closest('.hero-actions')
      || document.querySelector('#commandDeckToggle')?.parentElement
      || openHide?.parentElement
      || document.querySelector('.hero-actions');
  }

  function openHideButton() {
    return document.querySelector('#commandDeckToggle')
      || Array.from(document.querySelectorAll('button')).find((button) => /^(open|hide)$/i.test(button.textContent.trim()));
  }

  function panelTitle(panel) {
    return panel.querySelector('h1,h2,h3,.section-title')?.textContent?.trim()
      || panel.getAttribute('aria-label')
      || panel.dataset.workspaceTitle
      || 'Workspace';
  }

  function sectionContainerForHeading(heading) {
    const panel = heading.closest('section.panel[id], section[id]');
    if (panel && !panel.closest(`#${trayId}, #authGate, dialog`)) return panel;
    let node = heading;
    for (let depth = 0; depth < 6 && node; depth += 1) {
      const style = window.getComputedStyle(node);
      const border = `${style.borderTopWidth} ${style.borderRightWidth} ${style.borderBottomWidth} ${style.borderLeftWidth}`;
      const hasBorder = border !== '0px 0px 0px 0px';
      const rect = node.getBoundingClientRect();
      if (
        node !== heading
        && rect.width > 300
        && rect.height > 80
        && (hasBorder || node.matches('section, article, .panel, .card, [class*="section"], [class*="workspace"]'))
      ) {
        return node;
      }
      node = node.parentElement;
    }
    return heading.closest('section, article, .panel, .card, [class*="section"], [class*="workspace"]') || heading.parentElement;
  }

  function setPanelVisible(panel, visible) {
    if (!panel) return;
    if (visible) {
      panel.hidden = false;
      panel.style.display = '';
      panel.classList.remove('workspace-hidden');
      panel.removeAttribute('aria-hidden');
      delete panel.dataset.workspaceHiddenByToggle;
    } else {
      panel.dataset.workspaceHiddenByToggle = 'true';
      panel.hidden = true;
      panel.style.setProperty('display', 'none', 'important');
      panel.classList.add('workspace-hidden');
      panel.setAttribute('aria-hidden', 'true');
    }
  }

  function availableWorkspaceNumbers() {
    return titleToNumber
      .map(([number]) => String(number))
      .filter((number) => Boolean(panelFor(Number(number))));
  }

  function tagPanelsByHeadings() {
    const headings = Array.from(document.querySelectorAll('h1,h2,h3,.section-title,[class*="title"]'))
      .filter((heading) => !heading.closest(`#${trayId}`));
    headings.forEach((heading) => {
      const text = normalized(heading.textContent);
      const match = titleToNumber.find(([, names]) => names.some((name) => text.includes(name)));
      if (!match) return;
      const number = match[0];
      const container = sectionContainerForHeading(heading);
      if (!container || container.closest(`#${trayId}`)) return;
      container.dataset.workspacePanel = String(number);
      if (!container.id) container.id = `workspace-view-${String(number).padStart(2, '0')}`;
    });
  }

  function allMajorContentPanels() {
    tagPanelsByHeadings();
    const candidates = Array.from(document.querySelectorAll(
      '[data-workspace-panel], main section, main > div, #appShell main > div, .workspace-panel, .workspace-section, .flow-section, .panel, .card'
    ));
    return candidates.filter((panel) => {
      if (!panel || panel.closest(`#${trayId}`)) return false;
      if (panel.id === trayId || panel.id === buttonId) return false;
      if (panel.closest('header, nav, .hero-actions')) return false;
      if (panel.querySelector(`#${buttonId}, #${trayId}`)) return false;
      const rect = panel.getBoundingClientRect();
      if (rect.width < 280 || rect.height < 70) return false;
      const text = normalized(panel.textContent);
      if (text.length < 12) return false;
      return Boolean(panel.querySelector('h1,h2,h3,.section-title,[class*="title"],table,button,input,textarea,select'));
    });
  }

  function applyGlobalClosedState(activeNumbers) {
    return false;
  }

  function showAllWorkspacePanels() {
    const candidates = Array.from(document.querySelectorAll(
      '[data-workspace-hidden-by-toggle], [data-workspace-panel], main section, main > div, #appShell main > div, .workspace-panel, .workspace-section, .flow-section, .panel, .card, section.panel'
    ));
    candidates.forEach((panel) => {
      if (!panel || panel.closest(`#${trayId}, #authGate, header, nav, .hero-actions, dialog`)) return;
      if (panel.id === trayId || panel.id === buttonId) return;
      if (panel.querySelector(`#${buttonId}, #${trayId}`)) return;
      panel.hidden = false;
      panel.style.display = '';
      panel.classList.remove('workspace-hidden');
      panel.removeAttribute('aria-hidden');
      delete panel.dataset.workspaceHiddenByToggle;
    });
    return true;
  }

  function restoreHiddenWorkspacePanels() {
    const candidates = Array.from(document.querySelectorAll(
      '[data-workspace-hidden-by-toggle], [data-workspace-panel], main section, main > div, #appShell main > div, .workspace-panel, .workspace-section, .flow-section, .panel, .card'
    ));
    candidates.forEach((panel) => {
      if (!panel || panel.closest(`#${trayId}, #authGate, header, nav, .hero-actions`)) return;
      if (panel.id === trayId || panel.id === buttonId) return;
      if (panel.querySelector(`#${buttonId}, #${trayId}`)) return;
      panel.hidden = false;
      panel.style.display = '';
      delete panel.dataset.workspaceHiddenByToggle;
    });
  }

  function panels() {
    tagPanelsByHeadings();
    const candidates = Array.from(document.querySelectorAll(
      '[data-workspace-panel], main section, main > div, #appShell main > div, .workspace-panel, .workspace-section, .flow-section'
    ));
    return candidates
      .filter((panel) => !panel.closest(`#${trayId}`))
      .filter((panel) => {
        if (panel.matches('script, style, template')) return false;
        if (panel.id === trayId || panel.id === buttonId) return false;
        if (panel.querySelector(`#${buttonId}, #${trayId}`)) return false;
        const text = normalized(panel.textContent);
        return text.length > 12 && Boolean(panel.querySelector('h1,h2,h3,.section-title,[class*="title"]'));
      });
  }

  function panelFor(number) {
    const wanted = tokens[number] || [];
    const allPanels = panels();
    const tagged = allPanels.find((panel) => panel.dataset.workspacePanel === String(number));
    if (tagged) {
      if (!tagged.id) tagged.id = `workspace-view-${String(number).padStart(2, '0')}`;
      return tagged;
    }
    const matched = allPanels.find((panel) => {
      const haystack = normalized(`${panelTitle(panel)} ${panel.id || ''}`);
      return wanted.some((token) => haystack.includes(token));
    });
    const panel = matched || allPanels[number - 1] || null;
    if (panel && !panel.id) {
      panel.id = `workspace-view-${String(number).padStart(2, '0')}`;
    }
    if (panel) {
      panel.dataset.workspacePanel = String(number);
    }
    return panel;
  }

  function readActive() {
    try {
      const raw = localStorage.getItem(storageKey);
      if (raw) return new Set(JSON.parse(raw));
    } catch {
      // Fall through to the default-open behavior.
    }
    return new Set(
      Object.keys(tokens)
        .map((number) => panelFor(Number(number))?.id)
        .filter(Boolean)
    );
  }

  function writeActive(active) {
    localStorage.setItem(storageKey, JSON.stringify(Array.from(active)));
  }

  function readActiveNumbers() {
    try {
      const raw = localStorage.getItem(numberStorageKey);
      if (raw) {
        const parsed = new Set(JSON.parse(raw).map(String));
        if (parsed.size > 0 || sessionStorage.getItem(changedThisSessionKey) === 'true') {
          return parsed;
        }
      }
    } catch {
      // Fall through to default-open behavior.
    }
    return new Set(Object.keys(tokens));
  }

  function writeActiveNumbers(activeNumbers) {
    sessionStorage.setItem(changedThisSessionKey, 'true');
    localStorage.setItem(numberStorageKey, JSON.stringify(Array.from(activeNumbers).map(String)));
  }

  function ensureStyles() {
    if (document.querySelector('#workspaceFallbackStyles')) return;
    const style = document.createElement('style');
    style.id = 'workspaceFallbackStyles';
    style.textContent = `
      #${buttonId} {
        align-items: center !important;
        border: 1px solid rgba(125, 240, 198, .35) !important;
        border-radius: 8px !important;
        background: rgba(5, 24, 20, .84) !important;
        color: #f4fffb !important;
        cursor: pointer !important;
        display: inline-flex !important;
        font: inherit !important;
        font-weight: 700 !important;
        min-height: 42px !important;
        padding: 0 14px !important;
        white-space: nowrap !important;
      }
      .hero-actions {
        gap: 8px !important;
      }
      .command-deck,
      .flow-command-deck,
      .command-panel,
      [data-command-deck] {
        padding-bottom: 12px !important;
      }
      .command-deck-status,
      .flow-status,
      [data-command-status] {
        min-height: 0 !important;
        padding: 6px 22px !important;
      }
      #${buttonId}.${activeClass},
      #${trayId} button.${activeClass},
      #${trayId} .workspace-section-toggle.${activeClass},
      #${trayId} .workspace-section-toggle[aria-pressed="true"] {
        background: #7df0c6 !important;
        border-color: #7df0c6 !important;
        color: #04130f !important;
        font-weight: 800 !important;
        box-shadow: 0 0 0 1px rgba(125, 240, 198, .45), 0 0 18px rgba(125, 240, 198, .18) !important;
      }
      #${trayId} {
        background: #06120f !important;
        border: 1px solid rgba(125, 240, 198, .42) !important;
        border-radius: 10px !important;
        box-shadow: 0 22px 54px rgba(0, 0, 0, .48) !important;
        box-sizing: border-box !important;
        gap: 16px !important;
        grid-template-columns: 1fr !important;
        left: auto !important;
        max-height: calc(100vh - 150px) !important;
        overflow: auto !important;
        padding: 18px !important;
        position: fixed !important;
        right: 22px !important;
        top: 126px !important;
        width: min(620px, calc(100vw - 44px)) !important;
        z-index: 500 !important;
      }
      #${trayId}:not(.${openClass}) {
        display: none !important;
        visibility: hidden !important;
        pointer-events: none !important;
      }
      #${trayId}.${openClass} {
        display: grid !important;
        visibility: visible !important;
        pointer-events: auto !important;
      }
      #${trayId} .workspace-toggle-category { display: grid !important; gap: 8px !important; }
      #${trayId} .workspace-toggle-label {
        color: #7df0c6 !important;
        font-size: 12px !important;
        font-weight: 800 !important;
        text-transform: uppercase !important;
      }
      #${trayId} .workspace-toggle-buttons { display: flex !important; flex-wrap: wrap !important; gap: 8px !important; }
      #${trayId} .workspace-section-toggle {
        border: 1px solid rgba(125, 240, 198, .32) !important;
        border-radius: 8px !important;
        background: rgba(5, 24, 20, .72) !important;
        color: #f4fffb !important;
        cursor: pointer !important;
        font: inherit !important;
        min-height: 36px !important;
        padding: 0 11px !important;
      }
      #${trayId} .workspace-section-toggle[aria-pressed="false"] {
        background: rgba(5, 24, 20, .72) !important;
        color: #f4fffb !important;
      }
      .workspace-hidden,
      [data-workspace-hidden-by-toggle="true"] {
        display: none !important;
      }
      #${emptyStateId} {
        border: 1px solid rgba(125, 240, 198, .34) !important;
        border-left: 4px solid #7df0c6 !important;
        border-radius: 8px !important;
        background: rgba(10, 26, 22, .88) !important;
        display: grid !important;
        gap: 12px !important;
        margin: 0 !important;
        padding: 18px 20px !important;
      }
      #${emptyStateId}[hidden] {
        display: none !important;
      }
      #${emptyStateId} .workspace-empty-actions {
        display: flex !important;
        flex-wrap: wrap !important;
        gap: 10px !important;
      }
    `;
    document.head.append(style);
  }

  function ensureEmptyState() {
    let empty = document.querySelector(`#${emptyStateId}`);
    if (empty) return empty;
    const main = document.querySelector('#mainContent');
    if (!main) return null;
    empty = document.createElement('div');
    empty.id = emptyStateId;
    empty.setAttribute('role', 'region');
    empty.setAttribute('aria-label', 'No open workspaces');
    empty.innerHTML = `
      <div>
        <p class="eyebrow">Workspace visibility</p>
        <h2>No workspaces are open</h2>
        <p class="muted">Use the Workspaces menu to turn sections back on, or restore the full workspace set.</p>
      </div>
      <div class="workspace-empty-actions">
        <button id="workspaceEmptyOpenMenu" type="button" class="primary">Open Workspaces</button>
        <button id="workspaceEmptyRestoreAll" type="button">Show all workspaces</button>
      </div>
    `;
    const status = document.querySelector('#appStatus');
    if (status?.parentElement === main) status.after(empty);
    else main.prepend(empty);
    return empty;
  }

  function ensureTray() {
    let tray = document.querySelector(`#${trayId}`);
    if (!tray) {
      tray = document.createElement('aside');
      tray.id = trayId;
      tray.setAttribute('aria-label', 'Workspace menu');
      document.body.append(tray);
    }

    if (tray.dataset.fallbackBuilt === 'true') return tray;
    tray.replaceChildren();
    groups.forEach(([label, numbers]) => {
      const group = document.createElement('div');
      group.className = 'workspace-toggle-category';

      const heading = document.createElement('div');
      heading.className = 'workspace-toggle-label';
      heading.textContent = label;
      group.append(heading);

      const row = document.createElement('div');
      row.className = 'workspace-toggle-buttons';
      numbers.forEach((number) => {
        const panel = panelFor(number);
        if (!panel) return;
        const toggle = document.createElement('button');
        toggle.type = 'button';
        toggle.className = 'workspace-section-toggle';
        toggle.dataset.workspaceNumber = String(number);
        toggle.textContent = `${String(number).padStart(2, '0')} ${panel ? panelTitle(panel) : 'Workspace'}`;
        row.append(toggle);
      });
      group.append(row);
      tray.append(group);
    });
    tray.dataset.fallbackBuilt = 'true';
    return tray;
  }

  function setTrayOpen(open) {
    const tray = document.querySelector(`#${trayId}`);
    const button = document.querySelector(`#${buttonId}`);
    if (!tray || !button) return;
    tray.classList.toggle(openClass, open);
    tray.hidden = !open;
    button.classList.toggle(activeClass, open);
    button.setAttribute('aria-expanded', String(open));
    localStorage.setItem(menuOpenKey, open ? 'true' : 'false');
  }

  function sync() {
    if (!signedIn()) {
      document.querySelector(`#${buttonId}`)?.remove();
      document.querySelector(`#${trayId}`)?.remove();
      return;
    }

    ensureStyles();
    const bar = commandBar();
    const openHide = openHideButton();
    if (!bar || !openHide) return;

    let button = document.querySelector(`#${buttonId}`);
    if (!button) {
      button = document.createElement('button');
      button.id = buttonId;
      button.type = 'button';
      button.textContent = 'Workspaces';
      button.setAttribute('aria-controls', trayId);
      button.setAttribute('aria-expanded', 'false');
      bar.insertBefore(button, openHide);
    } else if (button.parentElement !== bar) {
      bar.insertBefore(button, openHide);
    } else if (button.nextElementSibling !== openHide) {
      bar.insertBefore(button, openHide);
    }

    const tray = ensureTray();
    const active = readActive();
    const activeNumbers = readActiveNumbers();
    const emptyState = ensureEmptyState();
    const activePanels = new Set();
    titleToNumber.forEach(([number]) => {
      const panel = panelFor(Number(number));
      if (panel?.id) {
        const isActive = activeNumbers.has(String(number));
        if (isActive) activePanels.add(panel);
      }
    });
    Array.from(document.querySelectorAll('#mainContent > section, main > section')).forEach((panel) => {
      if (panel.closest(`#${trayId}, #authGate, header, nav, .hero-actions, dialog`)) return;
      if (panel.id === trayId || panel.id === buttonId) return;
      if (panel.querySelector(`#${buttonId}, #${trayId}`)) return;
      setPanelVisible(panel, activePanels.has(panel));
    });
    if (emptyState) {
      emptyState.hidden = activeNumbers.size > 0;
      emptyState.setAttribute('aria-hidden', activeNumbers.size > 0 ? 'true' : 'false');
    }

    tray.querySelectorAll('[data-workspace-number]').forEach((toggle) => {
      const number = String(toggle.dataset.workspaceNumber);
      const isActive = activeNumbers.has(number);
      toggle.classList.toggle(activeClass, isActive);
      toggle.setAttribute('aria-pressed', String(isActive));
      toggle.style.setProperty('background', isActive ? '#7df0c6' : 'rgba(5, 24, 20, .72)', 'important');
      toggle.style.setProperty('border-color', isActive ? '#7df0c6' : 'rgba(125, 240, 198, .32)', 'important');
      toggle.style.setProperty('color', isActive ? '#04130f' : '#f4fffb', 'important');
    });

    button.textContent = `Workspaces (${activeNumbers.size})`;
    setTrayOpen(localStorage.getItem(menuOpenKey) === 'true');
  }

  document.addEventListener('click', (event) => {
    const target = event.target;
    if (!(target instanceof Element)) return;

    const menuButton = target.closest(`#${buttonId}`);
    if (menuButton) {
      event.preventDefault();
      const tray = document.querySelector(`#${trayId}`);
      setTrayOpen(!tray?.classList.contains(openClass));
      return;
    }

    if (target.closest(`#workspaceEmptyOpenMenu`)) {
      event.preventDefault();
      setTrayOpen(true);
      document.querySelector(`#${buttonId}`)?.focus();
      return;
    }

    if (target.closest(`#workspaceEmptyRestoreAll`)) {
      event.preventDefault();
      const activeNumbers = new Set(availableWorkspaceNumbers());
      writeActiveNumbers(activeNumbers);
      writeActive(new Set(
        Array.from(activeNumbers)
          .map((activeNumber) => panelFor(Number(activeNumber))?.id)
          .filter(Boolean)
      ));
      sync();
      return;
    }

    const toggle = target.closest(`#${trayId} [data-workspace-number]`);
    if (toggle) {
      event.preventDefault();
      const number = String(toggle.dataset.workspaceNumber);
      const panel = panelFor(Number(number));
      const activeNumbers = readActiveNumbers();
      if (activeNumbers.has(number)) activeNumbers.delete(number);
      else activeNumbers.add(number);
      writeActiveNumbers(activeNumbers);
      const active = new Set(
        Array.from(activeNumbers)
          .map((activeNumber) => panelFor(Number(activeNumber))?.id)
          .filter(Boolean)
      );
      writeActive(active);
      sync();
      if (panel?.id && activeNumbers.has(number)) {
        panel.scrollIntoView({ behavior: 'smooth', block: 'start' });
      }
    }
  }, true);

  document.addEventListener('DOMContentLoaded', sync);
  setTimeout(sync, 0);
  setTimeout(sync, 500);
  setTimeout(sync, 1500);
  setInterval(sync, 1000);
})();
