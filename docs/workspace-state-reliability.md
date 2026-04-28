# Workspace State Reliability

B72 makes workspace visibility a UI concern only. Opening or hiding a workspace changes presentation state; it must not mutate, save, clear, or reload finance data.

## Behavior

- The `Workspaces` button opens a right-side pop-out menu.
- Workspace buttons stay highlighted while their section is visible.
- Pressing the same workspace button again hides that section and removes the highlight.
- Active workspaces are persisted per signed-in user.
- The menu open/closed state is also persisted per signed-in user.
- The menu is not created on the sign-in screen.
- Keyboard users can activate the menu and section toggles with `Enter` or `Space`.
- Clicking outside the menu closes the pop-out without changing open workspace sections.

## Implementation

The workspace behavior is owned by:

```text
static/js/features/workspace-toggles.js
```

Shared persistence is owned by:

```text
static/js/core/ui-state.js
```

The legacy workspace controller in `static/app.js` is disabled so there is one owner for workspace visibility.

## Verification

Once the local runtime can launch Node/Playwright again, run:

```powershell
npx playwright test tests/playwright/workspace_state.spec.js
```

