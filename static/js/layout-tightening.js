(function () {
  function signedIn() {
    const authGate = document.querySelector('#authGate');
    const appShell = document.querySelector('#appShell');
    const authVisible = Boolean(authGate && !authGate.hidden && authGate.offsetParent !== null);
    return Boolean(appShell && !appShell.hidden && !authVisible);
  }

  function tightenReadyStatus() {
    const status = document.querySelector('#appStatus');
    if (!status) return;

    status.dataset.tightenedReadyStatus = 'true';
    status.setAttribute('aria-hidden', 'true');
    status.style.setProperty('display', 'none', 'important');
    status.style.setProperty('min-height', '0', 'important');
    status.style.setProperty('height', '0', 'important');
    status.style.setProperty('margin', '0', 'important');
    status.style.setProperty('padding', '0', 'important');
    status.style.setProperty('line-height', '0', 'important');
  }

  function tightenMainContentGap() {
    const main = document.querySelector('#mainContent');
    const deck = document.querySelector('.command-deck');
    const shell = document.querySelector('#appShell');
    if (!main || !deck || !shell || shell.classList.contains('hidden')) return;

    const targetGap = 16;
    const deckBottom = Math.ceil(deck.getBoundingClientRect().bottom);
    main.style.removeProperty('height');
    main.style.removeProperty('min-height');
    main.style.removeProperty('display');
    main.style.removeProperty('align-items');
    main.style.removeProperty('justify-content');
    main.style.setProperty('padding-top', `${deckBottom + targetGap}px`, 'important');
    main.style.setProperty('gap', `${targetGap}px`, 'important');
    main.style.setProperty('padding-bottom', '36px', 'important');
  }

  function restoreAuthGateSpacing() {
    const main = document.querySelector('#mainContent');
    const shell = document.querySelector('#appShell');
    if (!main || (shell && !shell.classList.contains('hidden'))) return;
    main.style.removeProperty('padding-top');
  }

  function cleanupOrphanDockClusters() {
    document.querySelectorAll('.dock-arrow-cluster').forEach((cluster) => {
      const parent = cluster.parentElement;
      cluster.querySelectorAll('.dock-toggle-button').forEach((button) => {
        const section = button.closest('[data-dockable-section="true"]');
        if (section) {
          let row = section.querySelector(':scope > .dock-control-row');
          if (!row) {
            row = document.createElement('div');
            row.className = 'dock-control-row';
            section.prepend(row);
          }
          row.append(button);
        } else {
          button.remove();
        }
      });
      cluster.remove();
      if (parent && !parent.children.length && parent.classList.contains('dock-control-row')) parent.remove();
    });

    if (!signedIn()) {
      document.querySelectorAll('.dock-toggle-button, .dock-control-row').forEach((node) => node.remove());
    }
  }

  function ensureStyles() {
    if (document.querySelector('#layoutTighteningStyles')) return;
    const style = document.createElement('style');
    style.id = 'layoutTighteningStyles';
    style.textContent = `
      #appStatus[data-tightened-ready-status="true"] + * {
        margin-top: 0 !important;
      }
      #appStatus[data-tightened-ready-status="true"] {
        display: none !important;
      }
      [data-dockable-section="true"] > .dock-control-row {
        gap: 4px !important;
      }
      [data-dockable-section="true"] > .dock-control-row:empty {
        display: none !important;
      }
      .dock-toggle-button {
        margin-left: 0 !important;
      }
    `;
    document.head.append(style);
  }

  function run() {
    ensureStyles();
    cleanupOrphanDockClusters();
    tightenReadyStatus();
    tightenMainContentGap();
    restoreAuthGateSpacing();
  }

  document.addEventListener('DOMContentLoaded', run);
  document.addEventListener('click', (event) => {
    if (!event.target?.closest?.('#commandDeckToggle')) return;
    requestAnimationFrame(run);
    setTimeout(run, 180);
  });
  const observer = new MutationObserver(run);
  document.addEventListener('DOMContentLoaded', () => {
    const shell = document.querySelector('#appShell');
    if (shell) observer.observe(shell, { attributes: true, attributeFilter: ['class'] });
  });
  setTimeout(run, 0);
  setTimeout(run, 500);
  setTimeout(run, 1500);
  setInterval(run, 2500);
})();
