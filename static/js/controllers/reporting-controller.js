(function () {
  const namespace = window.muFinancesControllers = window.muFinancesControllers || {};

  function tableIds() {
    return [
      'reportDefinitionTable',
      'dashboardWidgetTable',
      'financialStatementTable',
      'varianceReportTable',
      'reportBookTable',
      'reportFootnoteTable',
      'chartRenderTable',
      'productionPdfArtifactTable',
      'boardPackageTable',
    ].filter((id) => document.getElementById(id));
  }

  function status() {
    return {
      batch: 'B140',
      table_ids: tableIds(),
      reporting_workspace_present: Boolean(document.getElementById('reporting')),
    };
  }

  namespace.reporting = {
    status,
  };
})();
