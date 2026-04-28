from __future__ import annotations

import ast
import math
from dataclasses import dataclass
from typing import Any, Callable

MAX_EXPRESSION_LENGTH = 500
MAX_NODE_COUNT = 80
MAX_AST_DEPTH = 12
MAX_ABSOLUTE_RESULT = 1_000_000_000_000.0
MAX_ABSOLUTE_EXPONENT = 8.0

FormulaFunction = Callable[..., float]


def _formula_round(value: float, digits: float = 0.0) -> float:
    return float(round(value, int(digits)))


DEFAULT_FUNCTIONS: dict[str, FormulaFunction] = {
    'abs': abs,
    'max': max,
    'min': min,
    'round': _formula_round,
}

_BINARY_OPERATORS: dict[type[ast.operator], tuple[str, Callable[[float, float], float]]] = {
    ast.Add: ('+', lambda left, right: left + right),
    ast.Sub: ('-', lambda left, right: left - right),
    ast.Mult: ('*', lambda left, right: left * right),
    ast.Div: ('/', lambda left, right: left / right),
    ast.Mod: ('%', lambda left, right: left % right),
    ast.Pow: ('**', lambda left, right: left**right),
}

_UNARY_OPERATORS: dict[type[ast.unaryop], tuple[str, Callable[[float], float]]] = {
    ast.UAdd: ('+', lambda value: value),
    ast.USub: ('-', lambda value: -value),
}

_ALLOWED_NODES = (
    ast.Expression,
    ast.BinOp,
    ast.UnaryOp,
    ast.Constant,
    ast.Name,
    ast.Load,
    ast.Call,
    ast.Add,
    ast.Sub,
    ast.Mult,
    ast.Div,
    ast.Mod,
    ast.Pow,
    ast.UAdd,
    ast.USub,
)


@dataclass(slots=True)
class FormulaCheck:
    expression: str
    errors: list[str]
    names: set[str]
    functions: set[str]
    node_count: int
    max_depth: int

    @property
    def ok(self) -> bool:
        return not self.errors

    def to_dict(self) -> dict[str, Any]:
        return {
            'ok': self.ok,
            'errors': self.errors,
            'names': sorted(self.names),
            'functions': sorted(self.functions),
            'node_count': self.node_count,
            'max_depth': self.max_depth,
            'limits': {
                'max_expression_length': MAX_EXPRESSION_LENGTH,
                'max_node_count': MAX_NODE_COUNT,
                'max_ast_depth': MAX_AST_DEPTH,
                'max_absolute_result': MAX_ABSOLUTE_RESULT,
                'max_absolute_exponent': MAX_ABSOLUTE_EXPONENT,
            },
        }


def lint_formula(expression: str, allowed_functions: dict[str, FormulaFunction] | None = None) -> dict[str, Any]:
    return _check_formula(expression, allowed_functions).to_dict()


def expression_names(expression: str, allowed_functions: dict[str, FormulaFunction] | None = None) -> set[str]:
    return _check_formula(expression, allowed_functions).names


def evaluate_formula(
    expression: str,
    context: dict[str, float],
    allowed_functions: dict[str, FormulaFunction] | None = None,
    *,
    default_missing_names_to_zero: bool = False,
    rounding: int | None = None,
) -> dict[str, Any]:
    functions = allowed_functions or DEFAULT_FUNCTIONS
    check = _check_formula(expression, functions)
    if not check.ok:
        raise ValueError('; '.join(check.errors))
    tree = ast.parse(expression, mode='eval')
    trace: list[dict[str, Any]] = []

    def numeric(value: Any, label: str) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
            raise ValueError(f'{label} must resolve to a finite number.')
        result = float(value)
        if abs(result) > MAX_ABSOLUTE_RESULT:
            raise ValueError(f'{label} exceeds the sandbox result limit.')
        return result

    def evaluate(node: ast.AST) -> float:
        if isinstance(node, ast.Expression):
            return evaluate(node.body)
        if isinstance(node, ast.Constant):
            value = numeric(node.value, 'Formula constant')
            trace.append({'type': 'constant', 'value': value})
            return value
        if isinstance(node, ast.Name):
            if node.id not in context:
                if default_missing_names_to_zero:
                    value = 0.0
                else:
                    raise NameError(node.id)
            else:
                value = numeric(context[node.id], f'Variable {node.id}')
            trace.append({'type': 'variable', 'name': node.id, 'value': value})
            return value
        if isinstance(node, ast.UnaryOp) and type(node.op) in _UNARY_OPERATORS:
            symbol, operation = _UNARY_OPERATORS[type(node.op)]
            operand = evaluate(node.operand)
            value = numeric(operation(operand), 'Unary formula result')
            trace.append({'type': 'unary', 'operator': symbol, 'operand': operand, 'value': value})
            return value
        if isinstance(node, ast.BinOp) and type(node.op) in _BINARY_OPERATORS:
            symbol, operation = _BINARY_OPERATORS[type(node.op)]
            left = evaluate(node.left)
            right = evaluate(node.right)
            if isinstance(node.op, ast.Div) and right == 0:
                value = 0.0
            elif isinstance(node.op, ast.Mod) and right == 0:
                value = 0.0
            elif isinstance(node.op, ast.Pow) and abs(right) > MAX_ABSOLUTE_EXPONENT:
                raise ValueError('Exponent exceeds the sandbox exponent limit.')
            else:
                value = numeric(operation(left, right), 'Binary formula result')
            trace.append({'type': 'binary', 'operator': symbol, 'left': left, 'right': right, 'value': value})
            return value
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            args = [evaluate(arg) for arg in node.args]
            try:
                function_result = functions[node.func.id](*args)
            except TypeError as exc:
                raise ValueError(f'Function {node.func.id} received invalid arguments.') from exc
            value = numeric(function_result, f'Function {node.func.id}')
            trace.append({'type': 'function', 'name': node.func.id, 'args': args, 'value': value})
            return value
        raise ValueError('Formula expression uses an unsupported operation.')

    value = evaluate(tree)
    if rounding is not None:
        value = round(value, rounding)
    return {
        'value': value,
        'trace': trace,
        'names': sorted(check.names),
        'functions': sorted(check.functions),
        'node_count': check.node_count,
        'max_depth': check.max_depth,
    }


def _check_formula(expression: str, allowed_functions: dict[str, FormulaFunction] | None = None) -> FormulaCheck:
    functions = allowed_functions or DEFAULT_FUNCTIONS
    errors: list[str] = []
    names: set[str] = set()
    function_names: set[str] = set()
    node_count = 0
    max_depth = 0
    if not expression or not expression.strip():
        errors.append('Formula expression is required.')
        return FormulaCheck(expression, errors, names, function_names, node_count, max_depth)
    if len(expression) > MAX_EXPRESSION_LENGTH:
        errors.append(f'Formula expression must be {MAX_EXPRESSION_LENGTH} characters or less.')
    try:
        tree = ast.parse(expression, mode='eval')
    except SyntaxError:
        errors.append('Formula expression is not valid.')
        return FormulaCheck(expression, errors, names, function_names, node_count, max_depth)

    def visit(node: ast.AST, depth: int) -> None:
        nonlocal node_count, max_depth
        node_count += 1
        max_depth = max(max_depth, depth)
        if node_count > MAX_NODE_COUNT:
            errors.append(f'Formula expression exceeds the {MAX_NODE_COUNT} node sandbox limit.')
            return
        if depth > MAX_AST_DEPTH:
            errors.append(f'Formula expression exceeds the {MAX_AST_DEPTH} level sandbox depth limit.')
            return
        if not isinstance(node, _ALLOWED_NODES):
            errors.append(f'Unsupported formula operation: {type(node).__name__}.')
            return
        if isinstance(node, ast.Constant) and (isinstance(node.value, bool) or not isinstance(node.value, (int, float))):
            errors.append('Formula constants must be numeric.')
            return
        if isinstance(node, ast.Name):
            names.add(node.id)
        if isinstance(node, ast.Call):
            if not isinstance(node.func, ast.Name) or node.func.id not in functions:
                errors.append('Only approved formula functions are allowed.')
                return
            if node.keywords:
                errors.append('Formula functions do not accept keyword arguments.')
                return
            function_names.add(node.func.id)
        for child in ast.iter_child_nodes(node):
            visit(child, depth + 1)

    visit(tree, 1)
    return FormulaCheck(expression, sorted(set(errors)), names - function_names, function_names, node_count, max_depth)
