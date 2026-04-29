(function () {
  const namespace = window.muFinancesControllers = window.muFinancesControllers || {};

  function showDialog(id) {
    const dialog = document.getElementById(id);
    if (dialog?.showModal) dialog.showModal();
  }

  function openImport() {
    showDialog('importDialog');
  }

  function openGuidedImport() {
    showDialog('guidedImportDialog');
  }

  function openExport() {
    showDialog('powerBiExportDialog');
  }

  function openGuidedExport() {
    showDialog('guidedExportDialog');
  }

  namespace.importExport = {
    batch: 'B140',
    openImport,
    openGuidedImport,
    openExport,
    openGuidedExport,
  };
})();
