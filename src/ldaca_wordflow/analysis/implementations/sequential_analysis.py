"""Sequential analysis request schema module.

Used by:
- sequential analysis routes and task persistence models because they need a backend
  boundary that validates inputs before delegating to workspace or worker state.
Why:
- Keeps sequential-analysis specific request fields versioned in one place.

Flow: normalize inputs, delegate to the owning backend state or service boundary, and
    return serialized values or existing domain errors to callers.
"""

from pydantic import Field

from ..models import BaseAnalysisRequest


class SequentialAnalysisRequest(BaseAnalysisRequest):
    """Request model for sequential analysis.

    Used by:
    - sequential analysis run/update endpoints because they need a backend boundary that
      validates inputs before delegating to workspace or worker state.
    Why:
    - Validates temporal grouping and binning parameters.

    Refactor note:
    - `column_type`, `numeric_origin`, and `numeric_interval` are declared twice;
        remove duplicated declarations to reduce schema ambiguity.

    Flow: normalize inputs, delegate to the owning backend state or service boundary, and
        return serialized values or existing domain errors to callers.
    """

    node_id: str | None = Field(None, description="Node ID to analyze")
    time_column: str = Field(..., description="Column containing time/numeric data")
    group_by_columns: list[str] | None = Field(None, description="Columns to group by")
    frequency: str = Field(
        "monthly", description="Frequency (daily, weekly, monthly, yearly)"
    )
    sort_by_time: bool = Field(True, description="Whether to sort by time")
    column_type: str = Field(
        "datetime", description="Column type (datetime or numeric)"
    )
    numeric_origin: float | None = Field(None, description="Origin for numeric binning")
    numeric_interval: float | None = Field(
        None, description="Interval for numeric binning"
    )
    custom_interval_value: int | None = Field(
        None,
        description="Custom datetime interval count (used when frequency='custom')",
    )
    custom_interval_unit: str | None = Field(
        None,
        description="Custom datetime interval unit (seconds|minutes|hours|days|weeks)",
    )
    case_sensitive: bool = Field(
        True, description="Whether group-by values are compared case-sensitively"
    )
