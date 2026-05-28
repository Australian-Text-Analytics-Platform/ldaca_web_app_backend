"""Topic-modeling analysis request schema module.

Used by:
- topic-modeling routes and worker task request validation because they need a backend
  boundary that validates inputs before delegating to workspace or worker state.
Why:
- Keeps topic-modeling specific input contract centralized.

Flow: normalize inputs, delegate to the owning backend state or service boundary, and
    return serialized values or existing domain errors to callers.
"""

from typing import Literal

from pydantic import Field

from ..models import BaseAnalysisRequest


class TopicModelingRequest(BaseAnalysisRequest):
    """Request model for topic-modeling analysis.

    Used by:
    - topic-modeling run/update endpoints because they need a backend boundary that
      validates inputs before delegating to workspace or worker state.
    Why:
    - Validates node selection and clustering configuration inputs.

    Flow: normalize inputs, delegate to the owning backend state or service boundary, and
        return serialized values or existing domain errors to callers.
    """

    node_ids: list[str] = Field(..., description="List of node IDs to analyze")
    node_columns: dict[str, str] | None = Field(
        None, description="Map of node_id to column name"
    )
    min_topic_size: int = Field(
        5,
        description=(
            "Kept for backwards compatibility. Ignored when topic_size_mode is "
            "'target' or 'exact' — computed from topic_size_value in those modes."
        ),
    )
    random_seed: int = Field(
        42, description="Random seed used for reproducible topic-modeling runs"
    )
    representative_words_count: int = Field(
        5, description="Number of representative words to keep per topic"
    )
    sample_fractions: list[float | None] | None = Field(
        None,
        description=(
            "One sampling fraction (0 < f ≤ 1) per corpus in node_ids order. "
            "None for a corpus means no sampling. Sampling uses random_seed."
        ),
    )
    topic_size_mode: Literal["target", "min", "exact"] | None = Field(
        "target",
        description=(
            "'target': min_topic_size = max(2, n_eff // (topic_size_value * 10)). "
            "'min': topic_size_value used directly as min_topic_size. "
            "'exact': min_topic_size = max(5, int(target_min_topic_size * 0.75)) "
            "where target_min_topic_size = max(2, n_eff // (topic_size_value * 10)), "
            "then reduce_topics(nr_topics=topic_size_value) post-fit."
        ),
    )
    topic_size_value: int | None = Field(
        25,
        description="Numeric parameter interpreted according to topic_size_mode.",
    )
