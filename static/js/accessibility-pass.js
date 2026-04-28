(function () {
  function textOf(element) {
    return String(element.textContent || '').replace(/\s+/g, ' ').trim();
  }

  function ensureButtonLabels() {
    document.querySelectorAll('button, [role="button"]').forEach((button) => {
      if (button.getAttribute('aria-label') || button.getAttribute('aria-labelledby')) return;
      const text = textOf(button);
      if (text) button.setAttribute('aria-label', text);
    });
  }

  function ensureTableLabels() {
    document.querySelectorAll('table').forEach((table, index) => {
      if (!table.getAttribute('aria-label') && !table.getAttribute('aria-labelledby')) {
        const heading = table.closest('section, article, div')?.querySelector('h1,h2,h3,.section-title');
        table.setAttribute('aria-label', heading ? textOf(heading) : `Data table ${index + 1}`);
      }
      table.querySelectorAll('th').forEach((header) => {
        if (!header.getAttribute('scope')) header.setAttribute('scope', 'col');
      });
    });
  }

  function ensureInputsHaveLabels() {
    document.querySelectorAll('input, select, textarea').forEach((field) => {
      if (field.getAttribute('aria-label') || field.getAttribute('aria-labelledby') || field.id && document.querySelector(`label[for="${field.id}"]`)) {
        return;
      }
      const placeholder = field.getAttribute('placeholder');
      const name = field.getAttribute('name');
      field.setAttribute('aria-label', placeholder || name || 'Input field');
    });
  }

  function addSkipLink() {
    if (document.querySelector('#skipToMainContent')) return;
    const link = document.createElement('a');
    link.id = 'skipToMainContent';
    link.href = '#mainContent';
    link.textContent = 'Skip to main content';
    link.className = 'skip-link';
    document.body.prepend(link);
    const main = document.querySelector('main') || document.querySelector('#appShell');
    if (main && !main.id) main.id = 'mainContent';
  }

  function ensureFocusStyles() {
    if (document.querySelector('#accessibilityPassStyles')) return;
    const style = document.createElement('style');
    style.id = 'accessibilityPassStyles';
    style.textContent = `
      .skip-link {
        background: #7df0c6;
        color: #04130f;
        font-weight: 800;
        left: 12px;
        padding: 10px 12px;
        position: fixed;
        top: -60px;
        z-index: 1000;
      }
      .skip-link:focus { top: 12px; }
      button:focus-visible,
      a:focus-visible,
      input:focus-visible,
      select:focus-visible,
      textarea:focus-visible,
      [tabindex]:focus-visible {
        outline: 3px solid #7df0c6 !important;
        outline-offset: 3px !important;
      }
      table, th, td {
        border-color: rgba(125, 240, 198, .38) !important;
      }
      @media (max-width: 900px) {
        .hero-actions {
          align-items: stretch !important;
          display: grid !important;
          grid-template-columns: repeat(2, minmax(0, 1fr)) !important;
        }
        .hero-actions > button {
          justify-content: center !important;
          min-width: 0 !important;
          white-space: normal !important;
        }
      }
    `;
    document.head.append(style);
  }

  function markTablesForHighContrastReview() {
    document.querySelectorAll('table').forEach((table) => {
      table.dataset.highContrastChecked = 'true';
    });
  }

  function runAccessibilityPass() {
    addSkipLink();
    ensureFocusStyles();
    ensureButtonLabels();
    ensureInputsHaveLabels();
    ensureTableLabels();
    markTablesForHighContrastReview();
  }

  document.addEventListener('DOMContentLoaded', runAccessibilityPass);
  setTimeout(runAccessibilityPass, 0);
  setTimeout(runAccessibilityPass, 800);
  setInterval(runAccessibilityPass, 2500);
})();

