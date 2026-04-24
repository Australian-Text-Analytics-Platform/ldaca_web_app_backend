"""AST-based validation for user-supplied Polars expression code strings.

Before ``exec()``-ing untrusted code in ``_exec_polars_expr``, we parse the
code with :mod:`ast` and walk the tree to ensure it only contains constructs
that legitimate Polars expressions need.  This blocks all known ``exec()``
sandbox-escape vectors (dunder attribute traversal, ``import``, arbitrary name
access, assignments, class/function definitions, etc.) while still allowing
the full polars method-chaining API (``pl.col("x").str.starts_with("y")``).

Raises :class:`PolarsExprValidationError` with a descriptive message on any
disallowed construct so the API can return a clear 400 response.
"""

from __future__ import annotations

import ast

__all__ = [
    "validate_polars_expr_code",
    "PolarsExprValidationError",
]


class PolarsExprValidationError(ValueError):
    """Raised when expression code contains disallowed constructs."""


_ALLOWED_NAMES = frozenset({"pl", "True", "False", "None"})

_ALLOWED_NODE_TYPES = frozenset({
    # Wrapper
    ast.Expression,
    # Values
    ast.Constant,
    ast.Name,
    ast.Attribute,
    ast.Call,
    ast.List,
    ast.Tuple,
    ast.Dict,
    # Operators
    ast.BinOp,
    ast.UnaryOp,
    ast.Compare,
    ast.BoolOp,
    ast.Add,
    ast.Sub,
    ast.Mult,
    ast.Div,
    ast.Mod,
    ast.Pow,
    ast.FloorDiv,
    ast.USub,
    ast.UAdd,
    ast.Not,
    ast.Invert,
    ast.BitAnd,
    ast.BitOr,
    ast.BitXor,
    ast.LShift,
    ast.RShift,
    ast.Eq,
    ast.NotEq,
    ast.Lt,
    ast.LtE,
    ast.Gt,
    ast.GtE,
    ast.In,
    ast.NotIn,
    ast.Is,
    ast.IsNot,
    ast.And,
    ast.Or,
    # Subscript (e.g. pl.col("a")[0])
    ast.Subscript,
    ast.Slice,
    # Call helpers
    ast.Starred,
    ast.keyword,
    # Lambda (for pl.Expr.map_elements)
    ast.Lambda,
    ast.arguments,
    ast.arg,
    # Comprehensions (list comprehension in expression lists)
    ast.ListComp,
    ast.comprehension,
    # Ternary
    ast.IfExp,
    # Context nodes (always present on Name, Attribute, Subscript)
    ast.Load,
    ast.Store,  # needed for comprehension targets
    ast.Del,
    # f-strings
    ast.JoinedStr,
    ast.FormattedValue,
})


def validate_polars_expr_code(code: str) -> None:
    """Validate that *code* is a safe Polars expression.

    Only permits the subset of Python AST nodes needed for polars
    method-chaining expressions (``pl.col(...).method(...)``).
    Blocks imports, dunder access, assignments, function/class definitions,
    and references to any name other than ``pl``, ``True``, ``False``,
    ``None``.

    Raises:
        PolarsExprValidationError: if the code contains disallowed constructs.
    """
    code = code.strip()
    if not code:
        raise PolarsExprValidationError("Expression code cannot be empty")

    try:
        tree = ast.parse(code, mode="eval")
    except SyntaxError as exc:
        raise PolarsExprValidationError(
            f"Invalid Python syntax: {exc.msg}"
        ) from exc

    # Collect lambda parameter names so they are allowed in the body
    _lambda_arg_names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Lambda):
            for a in node.args.args:
                _lambda_arg_names.add(a.arg)

    allowed_names = _ALLOWED_NAMES | _lambda_arg_names

    for node in ast.walk(tree):
        node_type = type(node)

        if node_type not in _ALLOWED_NODE_TYPES:
            raise PolarsExprValidationError(
                f"Disallowed construct: {node_type.__name__}. "
                "Only Polars expressions (pl.col(...).method(...)) are permitted."
            )

        if node_type is ast.Name:
            name_node = node  # type: ignore[assignment]
            if name_node.id not in allowed_names:  # type: ignore[attr-defined]
                raise PolarsExprValidationError(
                    f"Disallowed name: {name_node.id!r}. "  # type: ignore[attr-defined]
                    "Only 'pl' is available as a top-level name."
                )

        if node_type is ast.Attribute:
            attr_node = node  # type: ignore[assignment]
            if attr_node.attr.startswith("_"):  # type: ignore[attr-defined]
                raise PolarsExprValidationError(
                    f"Access to private attribute '.{attr_node.attr}' is not permitted."  # type: ignore[attr-defined]
                )
