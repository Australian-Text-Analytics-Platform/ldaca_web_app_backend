"""Topic-modeling analysis request schema module.

Used by:
- topic-modeling routes and worker task request validation

Why:
- Keeps topic-modeling specific input contract centralized.
"""

from typing import Literal

from pydantic import Field

from ..models import BaseAnalysisRequest


class TopicModelingRequest(BaseAnalysisRequest):
    """
    Request model for topic-modeling analysis.

    Used by:
    - topic-modeling run/update endpoints

    Why:
    - Validates node selection and clustering configuration inputs.

    """

    node_ids: list[str] = Field(..., description="List of node IDs to analyze")
    node_columns: dict[str, str] | None = Field(
        None, description="Map of node_id to column name"
    )
    min_topic_size: int = Field(
        5, description="DBSCAN min_points (minimum cluster size)"
    )
    random_seed: int = Field(
        42, description="Random seed used for reproducible topic-modeling runs"
    )
    representative_words_count: int = Field(
        5, description="Number of representative words to keep per topic"
    )
    force_mode: Literal["auto", "classic", "online"] | None = Field(
        None,
        description=(
            "Override pipeline selection: 'classic' forces UMAP+HDBSCAN, "
            "'online' forces IncrementalPCA+MiniBatchKMeans, "
            "'auto' uses the corpus-size threshold (default)."
        ),
    )
    n_clusters: int | None = Field(
        None,
        description=(
            "Number of clusters for the online pipeline. "
            "Auto-selected (sqrt heuristic, clamped 10–200) when not set."
        ),
    )
