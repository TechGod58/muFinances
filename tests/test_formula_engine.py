from __future__ import annotations

import pytest

from app.services.formula_engine import evaluate_formula, expression_names, lint_formula


def test_formula_engine_evaluates_math_functions_and_trace() -> None:
    result = evaluate_formula('revenue * 1.05 + max(cost, 100)', {'revenue': 1000, 'cost': 50}, rounding=2)

    assert result['value'] == 1150
    assert result['names'] == ['cost', 'revenue']
    assert {'max'} == set(result['functions'])
    assert any(step['type'] == 'function' and step['name'] == 'max' for step in result['trace'])
    assert any(step['type'] == 'binary' for step in result['trace'])


def test_formula_engine_blocks_python_escape_paths() -> None:
    for expression in [
        '__import__("os").system("dir")',
        '(1).__class__',
        '[value for value in values]',
        'open("file.txt")',
    ]:
        lint = lint_formula(expression)
        assert lint['ok'] is False
        with pytest.raises(ValueError):
            evaluate_formula(expression, {'values': 1})


def test_formula_engine_enforces_sandbox_limits() -> None:
    assert lint_formula('1 + ' * 90 + '1')['ok'] is False
    with pytest.raises(ValueError, match='Exponent exceeds'):
        evaluate_formula('2 ** 99', {})


def test_expression_names_ignores_allowed_function_names() -> None:
    assert expression_names('round(headcount * rate, 2) + ACCOUNT_TUITION') == {'headcount', 'rate', 'ACCOUNT_TUITION'}

