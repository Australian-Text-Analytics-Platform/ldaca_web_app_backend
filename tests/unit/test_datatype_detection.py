import polars as pl
import pytest
from ldaca_wordflow.core.docworkspace_data_types import DocWorkspaceDataTypeUtils


class TestDocWorkspaceTypeMapping:
    """Tests for API schema type conversion helpers"""

    @pytest.mark.parametrize(
        ("polars_dtype", "ldaca_dtype"),
        [
            (pl.Categorical(), "categorical"),
            (pl.List(pl.String), "list_string"),
            (pl.List(pl.Int64), "unknown"),
            (pl.Array(pl.Int64, 2), "unknown"),
        ],
    )
    def test_polars_dtype_to_ldaca_dtype(self, polars_dtype, ldaca_dtype):
        assert (
            DocWorkspaceDataTypeUtils.polars_dtype_to_ldaca_dtype(polars_dtype)
            == ldaca_dtype
        )
