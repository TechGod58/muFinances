# Frontend Modularization Notes

B71 starts moving muFinances away from a single large `static/app.js` file and toward feature modules.

## Module Boundaries

- `static/js/core/api.js`: typed-ish API wrapper, JSON handling, consistent error messages.
- `static/js/core/ui-state.js`: local storage-backed UI state such as open workspaces.
- `static/js/core/loading.js`: shared loading, busy, and error state helpers.
- `static/js/features/workspace-toggles.js`: command-deck workspace menu and section visibility behavior.
- `static/js/bootstrap.js`: compatibility bootstrap that exposes modules under `window.muFinances`.

## Migration Rule

New UI work should not add another controller to `static/app.js`. Put shared behavior in `static/js/core`, and put feature behavior in `static/js/features`.

## Compatibility Rule

The existing app remains the compatibility shell until the runtime can run tests and Playwright again. The module layer is loaded separately so features can be migrated one at a time without a high-risk rewrite.

## B71 Completion Criteria

B71 is considered complete when:

- Shared API calls use the central API helper.
- Loading and error states are controlled through shared helpers.
- Workspace visibility behavior is owned by one module.
- Old duplicate workspace controllers are removed from `static/app.js`.
- Playwright covers login, workspace menu open/close, section toggle, and data persistence through hide/show.

