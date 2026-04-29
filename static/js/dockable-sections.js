(function () {
  const layerId = 'dockableSectionLayer';
  const controlClass = 'dock-toggle-button';
  const floatingClass = 'section-undocked';
  const placeholderClass = 'dock-placeholder';
  const storageKey = 'mufinances.dockableSections.undocked';
  const popouts = new Map();

  const dockableTitles = [
    'excel and office interop',
    'guidance and finance training',
    'role workspaces',
    'ux productivity',
    'ledger depth and actuals',
    'department position',
    'account position',
    'operating budget workspace',
    'enrollment and tuition planning',
    'positions',
    'budget lines',
    'planning drivers',
    'forecast and scenario engine',
    'model builder and allocations',
    'profitability and allocation management',
    'reporting and analytics',
    'close, reconciliation, and consolidation',
    'workflow and process orchestration',
    'campus integrations',
    'data hub and master data governance',
    'governed automation',
    'ai explainability',
    'deployment operations',
    'performance, scale, and reliability',
    'deployment governance and release controls',
    'compliance and audit hardening',
    'enterprise security administration',
    'comments, attachments, and evidence',
  ];

  function normalize(value) {
    return String(value || '').toLowerCase().replace(/\s+/g, ' ').trim();
  }

  function isSignedIn() {
    const authGate = document.querySelector('#authGate');
    const appShell = document.querySelector('#appShell');
    const authVisible = Boolean(authGate && !authGate.hidden && authGate.getBoundingClientRect().height > 0);
    if (authVisible) return false;
    if (appShell) return !appShell.hidden && appShell.getBoundingClientRect().height > 0;
    return Boolean(document.querySelector('#commandDeckToggle, #heroImportButton'));
  }

  function isVisibleElement(element) {
    if (!element || element.hidden) return false;
    const rect = element.getBoundingClientRect();
    const style = window.getComputedStyle(element);
    return rect.width > 0 && rect.height > 0 && style.display !== 'none' && style.visibility !== 'hidden';
  }

  function slug(value) {
    return normalize(value).replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');
  }

  function readUndocked() {
    try {
      return new Set(JSON.parse(localStorage.getItem(storageKey) || '[]'));
    } catch {
      return new Set();
    }
  }

  function writeUndocked(set) {
    localStorage.setItem(storageKey, JSON.stringify(Array.from(set)));
  }

  function ensureLayer() {
    let layer = document.querySelector(`#${layerId}`);
    if (!layer) {
      layer = document.createElement('div');
      layer.id = layerId;
      layer.setAttribute('aria-label', 'Undocked workspace sections');
      document.body.append(layer);
    }
    return layer;
  }

  function ensureStyles() {
    if (document.querySelector('#dockableSectionStyles')) return;
    const style = document.createElement('style');
    style.id = 'dockableSectionStyles';
    style.textContent = `
      #${layerId} {
        display: none;
      }
      .${placeholderClass} {
        display: none !important;
        height: 0 !important;
        min-height: 0 !important;
        margin: 0 !important;
        padding: 0 !important;
        overflow: hidden !important;
      }
      .${controlClass} {
        align-items: center;
        border: 1px solid rgba(125, 240, 198, .35);
        border-radius: 7px;
        background: rgba(5, 24, 20, .82);
        color: #f4fffb;
        cursor: pointer;
        display: inline-flex;
        font: inherit;
        font-weight: 800;
        height: 34px;
        justify-content: center;
        line-height: 1;
        margin-left: 2px;
        min-width: 34px;
        padding: 0 9px;
      }
      .${controlClass}:hover,
      .${controlClass}:focus-visible {
        background: #7df0c6;
        border-color: #7df0c6;
        color: #04130f;
      }
      [data-dockable-section="true"] {
        position: relative;
        padding-right: 64px !important;
      }
      [data-dockable-section="true"] > .dock-control-row {
        align-items: center;
        display: flex;
        gap: 8px;
        justify-content: flex-end;
        position: absolute;
        right: 14px;
        top: 12px;
        z-index: 2;
      }
      [data-dockable-section="true"] > .dock-control-row ~ .dock-control-row {
        display: none !important;
      }
      #${layerId} [data-dockable-section="true"] > .dock-control-row {
        position: sticky;
        top: 0;
      }
    `;
    document.head.append(style);
  }

  function titleFor(section) {
    return section.querySelector('h1,h2,h3,.section-title,[class*="title"]')?.textContent?.trim()
      || section.getAttribute('aria-label')
      || section.dataset.dockTitle
      || 'Section';
  }

  function sectionContainerForHeading(heading) {
    const panel = heading.closest('section.panel[id], section[id]');
    if (panel && !panel.closest(`#${layerId}, #workspaceToggleTray, #productionReadinessPanel, #authGate`)) {
      return panel;
    }
    let node = heading.parentElement;
    for (let depth = 0; depth < 7 && node; depth += 1) {
      if (node.closest(`#${layerId}`)) return node;
      const rect = node.getBoundingClientRect();
      const style = window.getComputedStyle(node);
      const hasBorder = ['Top', 'Right', 'Bottom', 'Left'].some((side) => style[`border${side}Width`] !== '0px');
      if (
        rect.width > 280
        && rect.height > 60
        && (hasBorder || node.matches('section, article, .panel, .card, [class*="section"], [class*="workspace"]'))
      ) {
        return node;
      }
      node = node.parentElement;
    }
    return heading.closest('section, article, .panel, .card, [class*="section"], [class*="workspace"]') || heading.parentElement;
  }

  function findDockableSections() {
    const headings = Array.from(document.querySelectorAll('h1,h2,h3,.section-title,[class*="title"]'))
      .filter((heading) => isVisibleElement(heading))
      .filter((heading) => !heading.closest(`#${layerId}, #workspaceToggleTray, #productionReadinessPanel, #authGate`));
    const sections = [];
    headings.forEach((heading) => {
      const headingText = normalize(heading.textContent);
      const match = dockableTitles.find((title) => headingText.includes(title));
      if (!match) return;
      const section = sectionContainerForHeading(heading);
      if (!section || section.closest(`#workspaceToggleTray, #productionReadinessPanel`)) return;
      if (!isVisibleElement(section)) return;
      if (sections.includes(section)) return;
      section.dataset.dockableSection = 'true';
      section.dataset.dockTitle = titleFor(section);
      section.dataset.dockKey = section.dataset.dockKey || slug(match);
      sections.push(section);
    });
    return sections;
  }

  function ensureControl(section) {
    section.querySelectorAll(':scope > .dock-control-row').forEach((row, index) => {
      if (index > 0) row.remove();
    });

    let row = section.querySelector(':scope > .dock-control-row');
    if (!row) {
      row = document.createElement('div');
      row.className = 'dock-control-row';
      section.prepend(row);
    }

    row.querySelectorAll(`.${controlClass}`).forEach((button, index) => {
      if (index > 0) button.remove();
    });

    let button = row.querySelector(`.${controlClass}`);
    if (!button) {
      button = document.createElement('button');
      button.type = 'button';
      button.className = controlClass;
      row.append(button);
    }

    const undocked = section.classList.contains(floatingClass);
    const title = titleFor(section);
    button.textContent = undocked ? '↙' : '↗';
    button.title = undocked ? `Dock ${title}` : `Undock ${title}`;
    button.setAttribute('aria-label', button.title);
    button.dataset.dockKey = section.dataset.dockKey;
    button.onclick = (event) => {
      event.preventDefault();
      event.stopPropagation();
      if (section.classList.contains(floatingClass)) dock(section);
      else undock(section, true, true);
    };
    return button;
  }

  function placeholderFor(section) {
    const key = section.dataset.dockKey;
    let placeholder = document.querySelector(`.${placeholderClass}[data-dock-placeholder="${key}"]`);
    if (!placeholder) {
      placeholder = document.createElement('div');
      placeholder.className = placeholderClass;
      placeholder.dataset.dockPlaceholder = key;
      placeholder.setAttribute('aria-hidden', 'true');
    }
    return placeholder;
  }

  function cloneStylesInto(targetDocument) {
    Array.from(document.querySelectorAll('link[rel="stylesheet"], style')).forEach((node) => {
      targetDocument.head.append(node.cloneNode(true));
    });
    const style = targetDocument.createElement('style');
    style.textContent = `
      body {
        background: #03110d;
        color: #f4fffb;
        font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        margin: 0;
        min-height: 100vh;
        padding: 18px;
      }
      .popout-shell {
        display: grid;
        gap: 14px;
      }
      .popout-titlebar {
        align-items: center;
        border-bottom: 1px solid rgba(125, 240, 198, .28);
        display: flex;
        gap: 12px;
        justify-content: space-between;
        padding-bottom: 12px;
      }
      .popout-titlebar strong {
        color: #7df0c6;
      }
      [data-dockable-section="true"] {
        margin: 0 !important;
        max-width: none !important;
        width: 100% !important;
      }
      [data-dockable-section="true"] > .dock-control-row {
        position: sticky !important;
        top: 0 !important;
      }
    `;
    targetDocument.head.append(style);
  }

  function openPopout(section) {
    const key = section.dataset.dockKey;
    const title = titleFor(section);
    const features = 'popup=yes,width=1120,height=760,left=120,top=80,resizable=yes,scrollbars=yes';
    const popup = window.open('', `muFinances_${key}`, features);
    if (!popup) return null;

    popup.document.open();
    popup.document.write('<!doctype html><html><head><title></title></head><body><div class="popout-shell"><div class="popout-titlebar"><strong></strong><span>Drag this window to another screen if needed.</span></div><main id="popoutContent"></main></div></body></html>');
    popup.document.close();
    popup.document.title = `${title} - muFinances`;
    popup.document.querySelector('.popout-titlebar strong').textContent = title;
    cloneStylesInto(popup.document);

    let returned = false;
    const returnSection = () => {
      if (returned) return;
      const liveSection = popup.document.querySelector('[data-dockable-section="true"]');
      if (liveSection) {
        returned = true;
        dock(liveSection, true);
      }
    };
    popup.addEventListener('beforeunload', returnSection);
    popup.addEventListener('pagehide', returnSection);

    popouts.set(key, popup);
    return popup;
  }

  function undock(section, persist = true, userInitiated = false) {
    if (!section || section.classList.contains(floatingClass)) return;
    if (!userInitiated) return;
    const placeholder = placeholderFor(section);
    section.after(placeholder);
    section.dataset.originalDisplay = section.style.display || '';
    section.dataset.originalParentUndocked = 'true';
    section.classList.add(floatingClass);
    const popup = openPopout(section);
    if (!popup) {
      section.classList.remove(floatingClass);
      placeholder.replaceWith(section);
      const button = section.querySelector(`.${controlClass}`);
      if (button) {
        button.title = 'Pop-out blocked. Allow pop-ups for localhost, then try again.';
        button.setAttribute('aria-label', button.title);
      }
      return;
    }
    popup.document.querySelector('#popoutContent').append(section);
    ensureControl(section);
    placeholder.setAttribute('aria-hidden', 'true');
    const closeWatcher = window.setInterval(() => {
      if (!popup.closed) return;
      window.clearInterval(closeWatcher);
      if (section.classList.contains(floatingClass)) {
        dock(section, true);
      }
    }, 500);

    if (persist) {
      const undocked = readUndocked();
      undocked.add(section.dataset.dockKey);
      writeUndocked(undocked);
    }
  }

  function dock(section, persist = true) {
    if (!section || !section.classList.contains(floatingClass)) return;
    const key = section.dataset.dockKey;
    const placeholder = placeholderFor(section);
    section.classList.remove(floatingClass);
    delete section.dataset.originalParentUndocked;
    section.style.display = section.dataset.originalDisplay || '';
    placeholder.replaceWith(section);
    ensureControl(section);
    const popup = popouts.get(key);
    if (popup && !popup.closed) {
      popouts.delete(key);
      try {
        popup.close();
      } catch {
        // Browser may block scripted close for a user-moved window.
      }
    }

    if (persist) {
      const undocked = readUndocked();
      undocked.delete(section.dataset.dockKey);
      writeUndocked(undocked);
    }
  }

  function restorePersistedState() {
    const sections = findDockableSections();
    const currentSections = new Set(sections);
    document.querySelectorAll('[data-dockable-section="true"]').forEach((section) => {
      if (currentSections.has(section) || section.closest(`#${layerId}`)) return;
      section.querySelectorAll(':scope > .dock-control-row').forEach((row) => row.remove());
      delete section.dataset.dockableSection;
      delete section.dataset.dockTitle;
      delete section.dataset.dockKey;
      section.classList.remove(floatingClass);
    });
    sections.forEach((section) => {
      cleanupLegacyDockControls(section);
      ensureControl(section);
    });
  }

  function cleanupLegacyDockControls(section) {
    section.querySelectorAll('button').forEach((button) => {
      if (button.classList.contains(controlClass)) return;
      const label = `${button.textContent || ''} ${button.title || ''} ${button.getAttribute('aria-label') || ''}`;
      const compact = label.replace(/\s+/g, '').trim();
      if (compact === '↗' || compact === '↙' || /dock|undock|pop.?out/i.test(label)) {
        const parentRow = button.closest('.dock-control-row');
        button.remove();
        if (parentRow && !parentRow.querySelector('button')) parentRow.remove();
      }
    });
  }

  function install() {
    ensureStyles();
    ensureLayer();
    if (!isSignedIn()) {
      cleanupAllDockControls();
      return;
    }
    restorePersistedState();
  }

  function cleanupAllDockControls() {
    document.querySelectorAll(`.${controlClass}, .dock-control-row, .dock-arrow-cluster, .${placeholderClass}`).forEach((node) => node.remove());
    document.querySelectorAll('[data-dockable-section="true"]').forEach((section) => {
      delete section.dataset.dockableSection;
      delete section.dataset.dockTitle;
      delete section.dataset.dockKey;
      section.classList.remove(floatingClass);
      section.style.removeProperty('padding-right');
    });
    const layer = document.querySelector(`#${layerId}`);
    if (layer) layer.replaceChildren();
  }

  document.addEventListener('DOMContentLoaded', install);
  setTimeout(install, 0);
  setTimeout(install, 800);
  setTimeout(install, 2000);
  setInterval(install, 3000);
})();
