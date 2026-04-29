window.muFinancesModules = window.muFinancesModules || {};
window.muFinancesModules.architecture = {
  batch: 'B52',
  boundaries: ['auth-router', 'health-router', 'managed-schema-files', 'static-feature-modules'],
};

window.muFinancesModules.frontendReliability = {
  batch: 'B58',
  boundaries: ['api-request-helper', 'loading-status', 'safe-rendering-helpers', 'hash-routing', 'playwright-workflow-smoke'],
};

window.muFinancesModules.frontendMonolithSplit = {
  batch: 'B140',
  controllers: [
    'core/api',
    'core/ui-state',
    'core/loading',
    'controllers/command-bar-controller',
    'controllers/workspace-controller',
    'controllers/import-export-controller',
    'controllers/reporting-controller',
    'dockable-sections',
    'chat-satellite',
  ],
  obsoleteControllersRemovedFromShell: ['workspace-button-fallback'],
};

window.muFinancesModules.htmlShellCleanup = {
  batch: 'B141',
  components: ['components/shell-templates'],
  extractedTemplates: ['chatSatellite'],
};
