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
from dataclasses import dataclass

__all__ = [
    "validate_polars_expr_code",
    "PolarsExprValidationError",
    "ValidationResult",
]


@dataclass(frozen=True, slots=True)
class ValidationResult:
    """Result of validating a polars expression code string."""

    mode: str  # "eval" or "assign"
    alias: str | None = None  # target name when mode == "assign"


class PolarsExprValidationError(ValueError):
    """Raised when expression code contains disallowed constructs."""


_ALLOWED_NAMES = frozenset({"pl", "True", "False", "None"})

_ALLOWED_NODE_TYPES = frozenset(
    {
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
    }
)


def validate_polars_expr_code(code: str) -> ValidationResult:
    """Validate that *code* is a safe Polars expression.

    Supports two forms:
    * **Expression**: ``pl.col("x").method(...)`` — parsed with ``mode="eval"``
    * **Assignment**: ``name = pl.col("x").method(...)`` — a single simple
      assignment whose RHS is a valid expression.  The target name is returned
      in ``ValidationResult.alias`` so the caller can append ``.alias(name)``.

    Only permits the subset of Python AST nodes needed for polars
    method-chaining expressions.  Blocks imports, dunder access, function/class
    definitions, and references to any name other than ``pl``, ``True``,
    ``False``, ``None``.

    Returns:
        A :class:`ValidationResult` indicating the parse mode and, for
        assignment syntax, the target alias name.

    Raises:
        PolarsExprValidationError: if the code contains disallowed constructs.
    """
    code = code.strip()
    if not code:
        raise PolarsExprValidationError("Expression code cannot be empty")

    # Try eval mode first (plain expression)
    alias: str | None = None
    expr_code = code
    try:
        tree = ast.parse(code, mode="eval")
        mode = "eval"
    except SyntaxError:
        # Fallback: check for single assignment  ``name = expr``
        try:
            tree_exec = ast.parse(code, mode="exec")
        except SyntaxError as exc:
            raise PolarsExprValidationError(
                f"Invalid Python syntax: {exc.msg}"
            ) from exc
        body = tree_exec.body
        if (
            len(body) != 1
            or not isinstance(body[0], ast.Assign)
            or len(body[0].targets) != 1
            or not isinstance(body[0].targets[0], ast.Name)
        ):
            raise PolarsExprValidationError(
                "Only single assignments of the form `name = expression` "
                "are allowed.  Multi-statement code is not permitted."
            )
        alias = body[0].targets[0].id
        if alias.startswith("_"):
            raise PolarsExprValidationError(
                f"Assignment target {alias!r} must not start with an underscore."
            )
        # Validate the RHS as an expression
        expr_code = (
            ast.get_source_segment(code, body[0].value) or code.split("=", 1)[1].strip()
        )
        try:
            tree = ast.parse(expr_code, mode="eval")
        except SyntaxError as exc:
            raise PolarsExprValidationError(
                f"Invalid Python syntax in RHS: {exc.msg}"
            ) from exc
        mode = "assign"

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

    return ValidationResult(mode=mode, alias=alias)
