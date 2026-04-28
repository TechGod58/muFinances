window.muFinancesModules = window.muFinancesModules || {};
window.muFinancesModules.architecture = {
  batch: 'B52',
  boundaries: ['auth-router', 'health-router', 'managed-schema-files', 'static-feature-modules'],
};

window.muFinancesModules.frontendReliability = {
  batch: 'B58',
  boundaries: ['api-request-helper', 'loading-status', 'safe-rendering-helpers', 'hash-routing', 'playwright-workflow-smoke'],
};
