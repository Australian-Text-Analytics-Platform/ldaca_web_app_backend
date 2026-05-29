"""
Tests for core utilities
"""

import uuid
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from ldaca_wordflow.core.utils import (
    detect_file_type,
    get_user_data_folder,
    get_user_workspace_folder,
    load_data_file,
    normalize_dtypes,
    read_zip_file,
    setup_user_folders,
)


class TestUserFolders:
    """Test user folder management functions"""

    @patch("ldaca_wordflow.core.user_folders.settings")
    def test_get_user_data_folder(self, mock_settings, temp_dir):
        """Test getting user data folder"""
        mock_settings.get_data_root.return_value = temp_dir
        mock_settings.user_data_folder = "users"

        user_id = "test_user_123"
        folder = get_user_data_folder(user_id)

        expected_path = temp_dir / "users" / f"user_{user_id}" / "user_data"
        assert folder == expected_path
        assert folder.exists()

    @patch("ldaca_wordflow.core.user_folders.settings")
    def test_get_user_workspace_folder(self, mock_settings, temp_dir):
        """Test getting user workspace folder"""
        mock_settings.get_data_root.return_value = temp_dir
        mock_settings.user_data_folder = "users"

        user_id = "test_user_123"
        folder = get_user_workspace_folder(user_id)

        expected_path = temp_dir / "users" / f"user_{user_id}" / "user_workspaces"
        assert folder == expected_path
        assert folder.exists()

    @patch("ldaca_wordflow.core.user_folders.settings")
    def test_setup_user_folders(self, mock_settings, temp_dir):
        """Test setting up complete user folder structure (no auto sample copy)"""
        mock_settings.get_data_root.return_value = temp_dir
        mock_settings.user_data_folder = "users"
        sample_data_dir = temp_dir / "sample_data"
        sample_data_dir.mkdir()
        (sample_data_dir / "test_file.txt").write_text("test content")
        mock_settings.get_sample_data_folder.return_value = sample_data_dir

        user_id = "test_user_123"
        folders = setup_user_folders(user_id)

        assert "user_folder" in folders
        assert "user_data" in folders
        assert "user_workspaces" in folders

        user_folder = temp_dir / "users" / f"user_{user_id}"
        user_data = user_folder / "user_data"
        user_workspaces = user_folder / "user_workspaces"
        sample_data_copy = user_data / "sample_data"

        assert user_folder.exists()
        assert user_data.exists()
        assert user_workspaces.exists()
        assert not sample_data_copy.exists()


class TestFileOperations:
    """Test file operation utilities"""

    def test_detect_file_type(self):
        """Test file type detection"""
        test_cases = [
            ("document.json", "json"),
            ("logs.jsonl", "jsonl"),
            ("table.parquet", "parquet"),
            ("spreadsheet.xlsx", "excel"),
            ("legacy.xls", "excel"),
            ("macro.xlsm", "excel"),
            ("binary.xlsb", "excel"),
            ("calc.ods", "excel"),
            ("notes.txt", "text"),
            ("readme.md", "text"),
            ("doc.rst", "text"),
            ("server.log", "text"),
            ("snippet.text", "text"),
            ("data.tsv", "tsv"),
            ("archive.zip", "zip"),
            ("unknown.xyz", "unknown"),
            ("file_without_extension", "unknown"),
        ]

        for filename, expected_type in test_cases:
            assert detect_file_type(filename) == expected_type

    def test_load_data_file_csv(self, sample_csv_file):
        """Test loading CSV file"""
        df = load_data_file(sample_csv_file)

        if isinstance(df, pl.LazyFrame):
            actual_df = cast(pl.DataFrame, df.collect())
            columns = list(df.collect_schema().names())
        else:
            assert isinstance(df, pl.DataFrame)
            actual_df = df
            columns = list(df.columns)

        assert actual_df.shape[0] == 3
        assert actual_df.shape[1] == 3
        assert "name" in columns
        assert "age" in columns
        assert "city" in columns

    def test_read_zip_file_returns_legacy_text_schema(self, temp_dir):
        """ZIP text ingestion should expose legacy file metadata columns."""
        from zipfile import ZipFile

        target = temp_dir / "legacy.zip"
        with ZipFile(target, "w") as zf:
            zf.writestr("nested/alpha.txt", "hello alpha")
            zf.writestr("beta.md", "hello beta")

        df = read_zip_file(target)

        assert df.columns == ["file_path", "base_name", "extension", "document"]
        assert df.to_dicts() == [
            {
                "file_path": "beta.md",
                "base_name": "beta",
                "extension": ".md",
                "document": "hello beta",
            },
            {
                "file_path": "nested/alpha.txt",
                "base_name": "alpha",
                "extension": ".txt",
                "document": "hello alpha",
            },
        ]

    def test_load_json_file(self, sample_json_file):
        """Test loading a JSON file"""
        df = load_data_file(sample_json_file)

        if isinstance(df, pl.LazyFrame):
            actual_df = cast(pl.DataFrame, df.collect())
            columns = list(df.collect_schema().names())
        else:
            assert isinstance(df, pl.DataFrame)
            actual_df = df
            columns = list(df.columns)

        assert actual_df.shape[0] == 3
        assert "name" in columns
        assert "age" in columns
        assert "city" in columns

    @patch("ldaca_wordflow.core.data_loading.pl.read_excel")
    def test_load_excel_file_selects_requested_sheet(self, mock_read_excel, temp_dir):
        """load_data_file should request and return the selected Excel sheet."""

        target = temp_dir / "workbook.xlsx"
        target.write_bytes(b"placeholder")

        expected = pl.DataFrame({"text": ["a", "b"]})
        mock_read_excel.return_value = expected

        result = load_data_file(target, sheet_name="Sheet2")

        mock_read_excel.assert_called_once_with(target, sheet_name="Sheet2")
        assert isinstance(result, pl.DataFrame)
        assert result.to_dicts() == expected.to_dicts()

    @patch("ldaca_wordflow.core.data_loading.pl.read_excel")
    def test_load_excel_file_dict_result_uses_selected_sheet(
        self, mock_read_excel, temp_dir
    ):
        """load_data_file should coerce workbook dict responses to the requested sheet DataFrame."""

        target = temp_dir / "workbook.xlsx"
        target.write_bytes(b"placeholder")

        sheet1 = pl.DataFrame({"value": [1]})
        sheet2 = pl.DataFrame({"value": [2]})
        mock_read_excel.return_value = {"Sheet1": sheet1, "Sheet2": sheet2}

        result = load_data_file(target, sheet_name="Sheet2")

        assert isinstance(result, pl.DataFrame)
        assert result.to_dicts() == [{"value": 2}]


class TestNormalizeDtypes:
    """Tests for the canonical dtype profile applied on data load."""

    def test_no_changes_for_already_canonical_frame(self):
        df = pl.DataFrame(
            {
                "text": ["a", "b"],
                "count": pl.Series([1, 2], dtype=pl.Int64),
                "score": pl.Series([1.0, 2.0], dtype=pl.Float64),
            }
        )
        normalized, changes = normalize_dtypes(df)
        assert changes == []
        assert normalized.schema == df.schema

    def test_naive_datetime_ns_assumed_utc_and_collapsed_to_us(self):
        df = pl.DataFrame(
            {"ts": pl.Series([datetime(2026, 5, 17, 10, 0, 0)], dtype=pl.Datetime("ns"))}
        )
        normalized, changes = normalize_dtypes(df)
        assert normalized.schema["ts"] == pl.Datetime("us", "UTC")
        assert len(changes) == 1
        change = changes[0]
        assert change["column"] == "ts"
        assert change["to_dtype"] == str(pl.Datetime("us", "UTC"))
        assert "naive" in change["reason"]
        assert "ns" in change["reason"]
        assert normalized["ts"][0] == datetime(2026, 5, 17, 10, 0, 0, tzinfo=timezone.utc)

    def test_aware_datetime_converted_to_utc(self):
        sydney_ts = datetime(2026, 5, 17, 20, 0, 0, tzinfo=ZoneInfo("Australia/Sydney"))
        df = pl.DataFrame(
            {"ts": pl.Series([sydney_ts], dtype=pl.Datetime("us", "Australia/Sydney"))}
        )
        normalized, changes = normalize_dtypes(df)
        assert normalized.schema["ts"] == pl.Datetime("us", "UTC")
        assert len(changes) == 1
        assert "Australia/Sydney" in changes[0]["reason"]
        assert normalized["ts"][0] == sydney_ts.astimezone(timezone.utc)

    def test_unsigned_and_narrow_signed_integers_promoted_to_int64(self):
        df = pl.DataFrame(
            {
                "u32": pl.Series([1, 2], dtype=pl.UInt32),
                "i16": pl.Series([-1, 2], dtype=pl.Int16),
                "already_i64": pl.Series([1, 2], dtype=pl.Int64),
            }
        )
        normalized, changes = normalize_dtypes(df)
        assert normalized.schema["u32"] == pl.Int64
        assert normalized.schema["i16"] == pl.Int64
        assert normalized.schema["already_i64"] == pl.Int64
        changed_cols = {c["column"] for c in changes}
        assert changed_cols == {"u32", "i16"}
        reasons = {c["column"]: c["reason"] for c in changes}
        assert "unsigned" in reasons["u32"]
        assert "narrower signed" in reasons["i16"]

    def test_float32_widened_to_float64(self):
        df = pl.DataFrame({"x": pl.Series([1.5, 2.5], dtype=pl.Float32)})
        normalized, changes = normalize_dtypes(df)
        assert normalized.schema["x"] == pl.Float64
        assert len(changes) == 1
        assert changes[0]["to_dtype"] == "Float64"

    def test_leaves_date_string_bool_categorical_unchanged(self):
        df = pl.DataFrame(
            {
                "d": pl.Series(["2026-05-17"]).str.to_date(),
                "s": pl.Series(["x"], dtype=pl.Utf8),
                "b": pl.Series([True], dtype=pl.Boolean),
                "c": pl.Series(["x"], dtype=pl.Categorical),
            }
        )
        normalized, changes = normalize_dtypes(df)
        assert changes == []
        assert normalized.schema == df.schema

    def test_empty_frame_returns_empty_changes(self):
        df = pl.DataFrame()
        normalized, changes = normalize_dtypes(df)
        assert changes == []
        assert normalized.width == 0
