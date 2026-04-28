# Static Feature Modules

The legacy `static/app.js` file still owns bootstrap and rendering while the UI
is being split safely. New browser-side feature code should be placed in this
folder and exposed through `window.muFinancesModules` until the shell can move
to native ES modules.
