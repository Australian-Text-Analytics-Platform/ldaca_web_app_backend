import sqlite3
from pathlib import Path
from types import SimpleNamespace

import polars as pl
from ldaca_wordflow.core import worker_tasks_import


def test_select_text_documents_prefers_plain_text_derivatives() -> None:
    metadata = {
        "@graph": [
            {
                "@id": "arcp://name,hdl10.26180~example/work/1",
                "@type": "CreativeWork",
                "name": "Document 1",
                "dateCreated": "1788",
            },
            {
                "@id": "https://data.ldaca.edu.au/api/stream?id=arcp%3A%2F%2Fname%2Chdl10.26180~example&path=data%2F1-001.txt",
                "@type": ["File"],
                "name": "Document 1 with codes",
                "encodingFormat": ["text/plain"],
                "contentSize": "20",
                "ldac:annotationOf": {"@id": "arcp://name,hdl10.26180~example/work/1"},
            },
            {
                "@id": "https://data.ldaca.edu.au/api/stream?id=arcp%3A%2F%2Fname%2Chdl10.26180~example&path=data%2F1-001-plain.txt",
                "@type": ["File"],
                "name": "Document 1 plain",
                "encodingFormat": ["text/plain"],
                "contentSize": "18",
                "ldac:annotationOf": {"@id": "arcp://name,hdl10.26180~example/work/1"},
            },
        ]
    }

    documents = worker_tasks_import._select_text_documents(metadata)

    assert documents == [
        {
            "file_id": "https://data.ldaca.edu.au/api/stream?id=arcp%3A%2F%2Fname%2Chdl10.26180~example&path=data%2F1-001-plain.txt",
            "path": "data/1-001-plain.txt",
            "name": "Document 1 plain",
            "encoding_format": "text/plain",
            "content_size": 18,
            "annotation_of": "arcp://name,hdl10.26180~example/work/1",
            "work_name": "Document 1",
            "date_created": "1788",
        }
    ]


def test_run_ldaca_import_task_uses_oni_metadata_and_writes_parquet(
    tmp_path,
    monkeypatch,
):
    user_data_dir = tmp_path / "users"
    cache_dir = tmp_path / "ldaca_cache"
    original_cwd = Path.cwd()
    observed_identifier: list[str] = []
    observed_downloads: list[list[str]] = []
    observed_tables: list[str] = []

    class FakeOniClient:
        async def get_metadata(self, identifier: str) -> dict:
            observed_identifier.append(identifier)
            return {
                "@context": "https://w3id.org/ro/crate/1.1/context",
                "@graph": [
                    {"@id": "ro-crate-metadata.json", "@type": "CreativeWork"},
                    {"@id": "./", "@type": "Dataset", "name": "Corpus Name"},
                    {
                        "@id": "arcp://name,hdl10.26180~example/work/1",
                        "@type": "CreativeWork",
                        "name": "Document 1",
                    },
                    {
                        "@id": "https://data.ldaca.edu.au/api/stream?id=arcp%3A%2F%2Fname%2Chdl10.26180~example&path=data%2F1-001-plain.txt",
                        "@type": ["File"],
                        "name": "Document 1 plain",
                        "encodingFormat": ["text/plain"],
                        "contentSize": "18",
                        "ldac:annotationOf": {
                            "@id": "arcp://name,hdl10.26180~example/work/1"
                        },
                    },
                ],
            }

        async def download_object_texts(
            self,
            identifier: str,
            paths: list[str],
            *,
            concurrency: int = 8,
        ) -> dict[str, str]:
            assert identifier == "arcp://name,hdl10.26180~example"
            assert concurrency == 8
            observed_downloads.append(paths)
            return {"data/1-001-plain.txt": "Downloaded document text"}

    class FakeTabulator:
        def load_config(self, config_file: str) -> None:
            assert Path(config_file).is_file()

        def crate_to_db(self, crate_uri: Path, db_file: Path, rebuild: bool = True):
            assert (crate_uri / "ro-crate-metadata.json").is_file()
            with sqlite3.connect(db_file) as connection:
                connection.execute(
                    "CREATE TABLE RepositoryObject (entity_id TEXT, name TEXT)"
                )
                connection.execute(
                    "INSERT INTO RepositoryObject VALUES (?, ?)",
                    ("./", "Corpus Name"),
                )

        def entity_table(self, table_name: str) -> list[str]:
            observed_tables.append(table_name)
            return []

        def close(self) -> None:
            pass

    observed_token = []

    def fake_from_settings(_settings, *, token=None):
        observed_token.append(token)
        return FakeOniClient()

    monkeypatch.setattr(
        worker_tasks_import.OniClient, "from_settings", fake_from_settings
    )
    monkeypatch.setattr(
        worker_tasks_import,
        "_load_rocrate_tabulator_class",
        lambda: FakeTabulator,
        raising=False,
    )
    monkeypatch.setattr(
        worker_tasks_import,
        "settings",
        SimpleNamespace(get_data_root=lambda: tmp_path),
        raising=False,
    )
    monkeypatch.setattr(
        "ldaca_wordflow.core.utils.get_user_data_folder",
        lambda _user_id: user_data_dir,
    )

    result = worker_tasks_import.run_ldaca_import_task(
        configure_worker_environment=lambda: None,
        user_id="test-user",
        workspace_id="test-workspace",
        url="https://data.ldaca.edu.au/collection?id=arcp%3A%2F%2Fname%2Chdl10.26180~example",
        api_token="portal-token",
    )

    assert observed_token == ["portal-token"]
    assert observed_identifier == ["arcp://name,hdl10.26180~example"]
    assert observed_downloads == [["data/1-001-plain.txt"]]
    assert "RepositoryObject" in observed_tables
    assert cache_dir.is_dir()
    assert Path.cwd() == original_cwd
    assert result["message"] == "Successfully imported Corpus Name"
    parquet_path = user_data_dir / "LDaCA" / "Corpus_Name" / "Corpus_Name.parquet"
    assert parquet_path.exists()
    df = pl.read_parquet(parquet_path)
    assert df.select("text").to_series().to_list() == ["Downloaded document text"]
    assert df.select("path").to_series().to_list() == ["data/1-001-plain.txt"]
