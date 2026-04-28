from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_mufinances_layout_has_distinct_flow_console_signature() -> None:
    index = (PROJECT_ROOT / 'static' / 'index.html').read_text(encoding='utf-8')
    styles = (PROJECT_ROOT / 'static' / 'styles.css').read_text(encoding='utf-8')

    assert 'data-layout-signature="mu-ledger-flow-console"' in index
    assert '<h1>muFinances</h1>' in index
    assert 'Parallel Cubed ledger map' in index
    assert 'Campus FPM Base' not in index
    assert 'grid-template-columns: 1fr;' in styles
    assert 'counter-reset: flow-nav;' in styles
    assert 'counter(flow-nav, decimal-leading-zero)' in styles
    assert '--accent: #7df0c6;' in styles
    assert '--accent-2: #f2c14e;' in styles


def test_command_deck_has_taskbar_popout_controls() -> None:
    index = (PROJECT_ROOT / 'static' / 'index.html').read_text(encoding='utf-8')
    styles = (PROJECT_ROOT / 'static' / 'styles.css').read_text(encoding='utf-8')
    app_js = (PROJECT_ROOT / 'static' / 'app.js').read_text(encoding='utf-8')

    assert 'id="commandDeckToggle"' in index
    assert 'aria-controls="commandDeckBody"' in index
    assert '.deck-collapsed .command-deck' in styles
    assert 'id="commandDeckPin"' not in index
    assert 'Pin open' not in index
    assert 'class="deck-titlebar"' in index
    assert '.deck-collapsed .command-deck-body' in styles
    assert 'display: none;' in styles
    assert '.deck-pinned .content' not in styles
    assert 'class="deck-signout"' in index
    assert 'mufinances.commandDeckCollapsed' in app_js
    assert 'mufinances.commandDeckPinned' not in app_js
    assert 'function toggleCommandDeck()' in app_js
    assert 'function toggleCommandDeckPin()' not in app_js
