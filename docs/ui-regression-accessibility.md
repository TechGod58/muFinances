# UI Regression And Accessibility Pass

B85 adds accessibility helpers and Playwright coverage.

## Covered Areas

- Keyboard navigation.
- Button accessible names.
- Input accessible names.
- Table labels and column scopes.
- Skip-to-main link.
- Focus-visible styling.
- Mobile command-bar layout.
- High-contrast table markers.
- Desktop visual smoke screenshot.

## Files

- `static/js/accessibility-pass.js`
- `tests/playwright/ui_regression_accessibility.spec.js`

## Verification

After the local Node/Playwright runtime is repaired:

```powershell
npx playwright test tests/playwright/ui_regression_accessibility.spec.js
```

