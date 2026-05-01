"""Tests for the Polars expression AST validator."""

from __future__ import annotations

import pytest
from ldaca_web_app.core.polars_expr_validator import (
    PolarsExprValidationError,
    ValidationResult,
    validate_polars_expr_code,
)


# ── Valid expressions ────────────────────────────────────────────────
class TestValidExpressions:
    """Expressions that should pass validation."""

    @pytest.mark.parametrize(
        "code",
        [
            'pl.col("text")',
            'pl.col("age") > 18',
            'pl.col("price").mul(0.9).alias("discounted")',
            'pl.col("a") + pl.col("b")',
            'pl.col("x").str.starts_with("hello")',
            'pl.col("x").cast(pl.Int64)',
            'pl.col("date").dt.year()',
            'pl.col("a").is_in(["x", "y", "z"])',
            'pl.col("a").sort(descending=True)',
            'pl.when(pl.col("a") > 0).then(pl.col("a")).otherwise(0)',
            "pl.lit(42)",
            "pl.lit(None)",
            'pl.col("a").fill_null(pl.col("b"))',
            'pl.col("a").over("group")',
            'pl.col("val").sum()',
            'pl.col("val").mean().round(2)',
            'pl.col("a") & pl.col("b")',
            'pl.col("a") | ~pl.col("b")',
            'pl.concat_str([pl.col("a"), pl.col("b")], separator="-")',
            'pl.col("x").map_elements(lambda x: x + 1)',
        ],
    )
    def test_valid_expression_passes(self, code: str) -> None:
        result = validate_polars_expr_code(code)
        assert result.mode == "eval"
        assert result.alias is None


# ── Blocked expressions ─────────────────────────────────────────────
class TestBlockedExpressions:
    """Expressions that MUST be rejected."""

    def test_blocks_import(self) -> None:
        with pytest.raises(PolarsExprValidationError, match="Disallowed"):
            # import via __import__ is a Name node for __import__,
            # but `import os` is an Import statement — caught by node type check
            validate_polars_expr_code("__import__('os').system('id')")

    def test_blocks_dunder_class(self) -> None:
        with pytest.raises(PolarsExprValidationError, match="private attribute"):
            validate_polars_expr_code("pl.__class__.__bases__[0].__subclasses__()")

    def test_blocks_dunder_globals(self) -> None:
        with pytest.raises(PolarsExprValidationError, match="private attribute"):
            validate_polars_expr_code("pl.__spec__.__init__.__globals__")

    def test_blocks_dunder_builtins(self) -> None:
        with pytest.raises(PolarsExprValidationError, match="private attribute"):
            validate_polars_expr_code("().__class__.__bases__[0].__subclasses__()")

    def test_blocks_arbitrary_names(self) -> None:
        with pytest.raises(PolarsExprValidationError, match="Disallowed name"):
            validate_polars_expr_code("os.system('id')")

    def test_blocks_open(self) -> None:
        with pytest.raises(PolarsExprValidationError, match="Disallowed name"):
            validate_polars_expr_code("open('/etc/passwd').read()")

    def test_blocks_eval(self) -> None:
        with pytest.raises(PolarsExprValidationError, match="Disallowed name"):
            validate_polars_expr_code("eval('1+1')")

    def test_blocks_exec(self) -> None:
        with pytest.raises(PolarsExprValidationError, match="Disallowed name"):
            validate_polars_expr_code("exec('import os')")

    def test_blocks_empty_code(self) -> None:
        with pytest.raises(PolarsExprValidationError, match="cannot be empty"):
            validate_polars_expr_code("")

    def test_blocks_whitespace_only(self) -> None:
        with pytest.raises(PolarsExprValidationError, match="cannot be empty"):
            validate_polars_expr_code("   ")

    def test_blocks_syntax_error(self) -> None:
        with pytest.raises(PolarsExprValidationError, match="Invalid Python syntax"):
            validate_polars_expr_code("pl.col(")

    def test_blocks_mro_traversal(self) -> None:
        with pytest.raises(PolarsExprValidationError, match="private attribute"):
            validate_polars_expr_code("''.__class__.__mro__[1].__subclasses__()")

    def test_allows_single_assignment(self) -> None:
        result = validate_polars_expr_code("x = pl.col('a')")
        assert result.mode == "assign"
        assert result.alias == "x"

    def test_blocks_generator_expression(self) -> None:
        with pytest.raises(PolarsExprValidationError, match="Disallowed"):
            validate_polars_expr_code("next(x for x in range(10))")

    def test_blocks_walrus_operator(self) -> None:
        with pytest.raises(
            PolarsExprValidationError,
            match="Disallowed construct|Invalid Python syntax",
        ):
            validate_polars_expr_code("(x := pl.col('a'))")


# ── Assignment syntax ────────────────────────────────────────────────
class TestAssignmentSyntax:
    """Tests for ``name = expr`` assignment form."""

    def test_simple_assignment(self) -> None:
        result = validate_polars_expr_code('discounted = pl.col("price").mul(0.9)')
        assert result == ValidationResult(mode="assign", alias="discounted")

    def test_assignment_with_method_chain(self) -> None:
        result = validate_polars_expr_code('year = pl.col("date").dt.year()')
        assert result == ValidationResult(mode="assign", alias="year")

    def test_blocks_dunder_assignment_target(self) -> None:
        with pytest.raises(PolarsExprValidationError, match="underscore"):
            validate_polars_expr_code('__x = pl.col("a")')

    def test_blocks_multi_statement(self) -> None:
        with pytest.raises(
            PolarsExprValidationError, match="single assignment|Invalid Python syntax"
        ):
            validate_polars_expr_code('x = pl.col("a")\ny = pl.col("b")')

    def test_blocks_multi_target(self) -> None:
        with pytest.raises(
            PolarsExprValidationError, match="single assignment|Invalid Python syntax"
        ):
            validate_polars_expr_code('x = y = pl.col("a")')

    def test_blocks_assignment_with_dangerous_rhs(self) -> None:
        with pytest.raises(PolarsExprValidationError, match="Disallowed name"):
            validate_polars_expr_code("x = os.system('id')")
