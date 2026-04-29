(function () {
  const namespace = window.muFinancesControllers = window.muFinancesControllers || {};

  function button(id) {
    return document.getElementById(id);
  }

  function status() {
    return {
      batch: 'B140',
      buttons: {
        import: Boolean(button('heroImportButton')),
        export: Boolean(button('heroExportButton')),
        marketWatch: Boolean(button('marketWatchButton')),
        chat: Boolean(button('chatButton')),
        workspaces: Boolean(button('workspaceMenuButton')),
        openHide: Boolean(button('commandDeckToggle')),
      },
      shell: Boolean(document.getElementById('appShell')),
    };
  }

  function setBusy(message) {
    const target = document.getElementById('appStatus');
    if (!target) return;
    target.textContent = message || 'Ready';
    target.dataset.state = message ? 'loading' : 'ready';
  }

  namespace.commandBar = {
    status,
    setBusy,
  };
})();
