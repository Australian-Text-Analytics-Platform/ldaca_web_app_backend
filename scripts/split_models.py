#!/usr/bin/env python3
"""Split monolithic models/__init__.py into domain files.

Usage (from backend/)::

    uv run python scripts/split_models.py
"""

from __future__ import annotations

import re
from collections import OrderedDict
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

MODELS_DIR = Path(__file__).resolve().parent.parent / "src" / "ldaca_wordflow" / "models"
INIT_PATH = MODELS_DIR / "__init__.py"

# ── Which classes go to which domain ───────────────────────────────────
# Every class name that appears in the monolithic file must be mapped here.
# Classes within a section are handled by SECTION_DOMAIN_MAP; overrides
# handle scattered/special placement.

CLASS_TO_DOMAIN: Dict[str, str] = {
    # === AUTHENTICATION MODELS ===
    "User": "auth",
    "AuthMethod": "auth",
    "AuthInfoResponse": "auth",
    "GoogleIn": "auth",
    "GoogleOut": "auth",
    "UserResponse": "auth",

    # === FILE MANAGEMENT MODELS ===
    "FileUploadResponse": "files",
    "ImportSampleDataResponse": "files",
    "ImportSampleDataRequest": "files",
    "SampleDataFileEntry": "files",
    "SampleDataCollection": "files",
    "SampleDataCatalogueResponse": "files",
    "DemoSnapshotEntry": "files",
    "DemoSnapshotsCatalogueResponse": "files",
    "ImportDemoSnapshotsRequest": "files",
    "DemoSnapshotImportResult": "files",
    "ImportDemoSnapshotsResponse": "files",
    "DataFileInfo": "files",
    "LDaCAImportRequest": "files",
    "OniSearchRequest": "files",
    "OniSearchResult": "files",
    "OniSearchResponse": "files",
    "FileTreeNodeResponse": "files",
    "CreateFolderRequest": "files",
    "CreateFolderResponse": "files",
    "MoveFileRequest": "files",
    "MessageResponse": "files",
    "RenameColumnRequest": "files",
    "FilesTaskMetadataResponse": "files",
    "FilesImportTaskStartResponse": "files",
    "TaskListResponse": "files",
    "TaskClearActionDataResponse": "files",
    "TaskClearActionResponse": "files",
    "TaskCancelActionDataResponse": "files",
    "TaskCancelActionResponse": "files",
    "FilesTasksListResponse": "files",
    "FilesTaskActionDataResponse": "files",
    "FilesTaskActionResponse": "files",
    "FileInfoResponse": "files",
    "SnapshotCapabilities": "files",
    "SnapshotPayloadEntryResult": "files",
    "SnapshotPayloadEntryDispersionBins": "files",
    "SnapshotPayloadEntrySourceProjection": "files",
    "SnapshotPayloadEntrySettings": "files",
    "ConcordanceSnapshotPreview": "files",
    "QuotationSnapshotPreview": "files",
    "TokenFrequenciesSnapshotPreview": "files",
    "SequentialAnalysisSnapshotPreview": "files",
    "TopicModelingSnapshotPreview": "files",
    "SnapshotSource": "files",
    "SnapshotManifest": "files",
    "SnapshotListItem": "files",
    "SnapshotListResponse": "files",
    "SnapshotUploadResponse": "files",
    "SnapshotDeleteResponse": "files",

    # === WORKSPACE MODELS ===
    "WorkspaceInfo": "workspace",
    "WorkspaceSummary": "workspace",
    "CurrentWorkspaceResponse": "workspace",
    "SetCurrentWorkspaceResponse": "workspace",
    "DtypeNormalizationChange": "workspace",
    "WorkspaceNodeInfo": "workspace",
    "NodeDocumentColumnUpdateRequest": "workspace",
    "NodeTokenizationPreferenceRequest": "workspace",
    "TokenizerModelInfo": "workspace",
    "TokenizerModelsResponse": "workspace",
    "WorkspaceGraphEdge": "workspace",
    "WorkspaceGraphResponse": "workspace",
    "WorkspaceNodesResponse": "workspace",
    "WorkspaceCreateRequest": "workspace",
    "WorkspaceSaveRequest": "workspace",
    "WorkspaceActionResponse": "workspace",
    "WorkspaceTaskStartResponse": "workspace",
    "WorkspaceUploadResponse": "workspace",

    # === DATAFRAME MODELS ===
    "DataFrameNode": "dataframe",
    "NodeLineage": "dataframe",
    "DataFrameInfo": "dataframe",

    # === DATA OPERATION MODELS ===
    "DataOperation": "data_operations",
    "FilterOperation": "data_operations",
    "SliceOperation": "data_operations",
    "TransformOperation": "data_operations",
    "AggregateOperation": "data_operations",
    "ReplaceRequest": "data_operations",
    "ReplacePreviewResponse": "data_operations",
    "ReplaceApplyResponse": "data_operations",
    "JoinRequest": "data_operations",
    "ConcatPreviewRequest": "data_operations",
    "ConcatRequest": "data_operations",
    "NodeOperationResponse": "data_operations",
    "NodeActionResponse": "data_operations",
    "CastNodeRequest": "data_operations",
    "CastNodeInfo": "data_operations",
    "CastNodeResponse": "data_operations",
    "DataFrameOperationRequest": "data_operations",

    # === TEXT ANALYSIS MODELS → analysis_common ===
    "TextSetupRequest": "analysis_common",
    "DTMRequest": "analysis_common",
    "KeywordExtractionRequest": "analysis_common",
    "TextAnalysisInfo": "analysis_common",

    # === TEXT ANALYSIS → concordance ===
    "ConcordanceAnalysisRequest": "concordance",
    "ConcordanceDetachRequest": "concordance",
    "ConcordanceDispersionDetachRequest": "concordance",
    "ConcordanceMaterializeRequest": "concordance",
    "ConcordanceDetachNodeOption": "concordance",
    "ConcordanceDetachOptionsResponse": "concordance",

    # === TEXT ANALYSIS → quotation ===
    "QuotationEngineType": "quotation",
    "QuotationEngineConfig": "quotation",
    "QuotationRequest": "quotation",
    "QuotationDetachRequest": "quotation",
    "QuotationMaterializeRequest": "quotation",
    "QuotationDetachNodeOption": "quotation",
    "QuotationDetachOptionsResponse": "quotation",
    "QuotationMetadata": "quotation",
    "QuotationAnalysisResponse": "quotation",
    "QuotationPreferenceUpdateData": "quotation",
    "QuotationPreferenceUpdateResponse": "quotation",
    "QuotationResultQuery": "quotation",

    # === TEXT ANALYSIS → sequential ===
    "SequentialAnalysisRequest": "sequential_analysis",
    "SequentialAnalysisResponse": "sequential_analysis",
    "SequentialAnalysisPreviewResponse": "sequential_analysis",
    "SequentialAnalysisPreferenceUpdateData": "sequential_analysis",
    "SequentialAnalysisPreferenceUpdateRequest": "sequential_analysis",
    "SequentialAnalysisPreferenceUpdateResponse": "sequential_analysis",
    "SequentialAnalysisDetachResponse": "sequential_analysis",

    # === Scattered between TEXT ANALYSIS and RESPONSE MODELS ===
    "AnalysisTaskActionResponse": "analysis_common",
    "AnalysisClearResponse": "analysis_common",
    "CurrentAnalysisTasksResponse": "analysis_common",
    "AiAnnotationDetachData": "ai_annotation",
    "AiAnnotationDetachResponse": "ai_annotation",
    "AiAnnotationSaveData": "ai_annotation",
    "AiAnnotationSaveResponse": "ai_annotation",
    "TopicModelingEmbeddingCacheMeasurement": "topic_modeling",
    "TopicModelingEmbeddingCacheSizeResponse": "topic_modeling",
    "TopicModelingEmbeddingCacheClearData": "topic_modeling",
    "TopicModelingEmbeddingCacheClearResponse": "topic_modeling",

    # === RESPONSE MODELS ===
    "APIResponse": "shared",
    "PaginatedResponse": "shared",
    "ErrorResponse": "shared",

    # === FILE PREVIEW MODELS ===
    "FilePreviewRequest": "shared",
    "FilePreviewResponse": "shared",

    # === FILTER AND SLICE + NODE MODELS → nodes.py ===
    "FilterCondition": "nodes",
    "FilterRequest": "nodes",
    "SliceRequest": "nodes",
    "FilterPreviewResponse": "nodes",
    "NodeDataResponse": "nodes",
    "NodeQueryPlanResponse": "nodes",
    "NodeShapeResponse": "nodes",
    "ColumnUniqueValuesResponse": "nodes",
    "ColumnOperationInfo": "nodes",
    "ColumnOperationsResponse": "nodes",

    # === Shared analysis types → analysis_common ===
    "PaginationInfo": "analysis_common",
    "AnalysisTaskMetadata": "analysis_common",
    "SourceRowPagination": "analysis_common",
    "AnalysisSorting": "analysis_common",
    "NodeDataSorting": "analysis_common",
    "NodeDataFiltering": "analysis_common",

    # === POLARS EXPRESSION MODELS ===
    "PolarsExpressionContext": "polars_expression",
    "PolarsExpressionItem": "polars_expression",
    "PolarsExpressionRequest": "polars_expression",
    "PolarsExpressionApplyResponse": "polars_expression",

    # === TOKEN FREQUENCY MODELS ===
    "StopWordsPayload": "token_frequencies",
    "TokenFrequencyRequest": "token_frequencies",
    "TokenFrequencyPreferenceUpdateRequest": "token_frequencies",
    "TokenFrequencyData": "token_frequencies",
    "TokenStatisticsData": "token_frequencies",
    "TokenFrequencyNodeResult": "token_frequencies",
    "TokenFrequencyResponse": "token_frequencies",

    # === AI ANNOTATION MODELS ===
    "AiAnnotationClassDef": "ai_annotation",
    "AiAnnotationExample": "ai_annotation",
    "AiAnnotationModelsRequest": "ai_annotation",
    "AiAnnotationRequest": "ai_annotation",
    "AiAnnotationDetachRequest": "ai_annotation",
    "AiAnnotationEdit": "ai_annotation",
    "AiAnnotationSaveRequest": "ai_annotation",
    "AiAnnotationNodeResult": "ai_annotation",
    "AiAnnotationResultQuery": "ai_annotation",
    "AiAnnotationResponse": "ai_annotation",
    "AiAnnotationModelInfo": "ai_annotation",
    "AiAnnotationModelsData": "ai_annotation",
    "AiAnnotationModelsResponse": "ai_annotation",
    "AiAnnotationProvidersData": "ai_annotation",
    "AiAnnotationProvidersResponse": "ai_annotation",
    "AiAnnotationCategoriesData": "ai_annotation",
    "AiAnnotationCategoriesResponse": "ai_annotation",

    # === TOPIC MODELING MODELS ===
    "TopicModelingRequest": "topic_modeling",
    "TopicModelingTopic": "topic_modeling",
    "TopicModelingData": "topic_modeling",
    "TopicModelingResponse": "topic_modeling",
    "TopicModelingResultUpdateRequest": "topic_modeling",
    "TopicMeaningOverrideItem": "topic_modeling",
    "TopicModelingDetachRequest": "topic_modeling",
    "TopicModelingDetachNodeOption": "topic_modeling",
    "TopicModelingDetachOptionsResponse": "topic_modeling",
    "TopicModelingDetachedNode": "topic_modeling",
    "TopicModelingDetachData": "topic_modeling",
    "TopicModelingDetachResponse": "topic_modeling",

    # === Concordance response models (after TOPIC MODELING section) ===
    "ConcordanceMetadata": "concordance",
    "ConcordanceNodeResult": "concordance",
    "ConcordanceAnalysisResponse": "concordance",
    "ConcordanceDispersionBinRow": "concordance",
    "ConcordanceDispersionBinsResponse": "concordance",

    # === COLUMN DESCRIBE MODELS ===
    "ColumnDescribeResponse": "nodes",
}

# Type aliases → domain (these are `X = Union/Literal[...]` lines)
TYPE_ALIASES: List[Tuple[str, str]] = [
    ("SnapshotPayloadEntry", "files"),
    ("SnapshotPreview", "files"),
    ("SnapshotToolKey", "files"),
    ("ColumnScalarValue", "analysis_common"),
    ("AnalysisTaskState", "analysis_common"),
]

# ── Domain descriptions for docstring headers ───────────────────────────
DOMAIN_DESCRIPTIONS: Dict[str, str] = {
    "auth": "Authentication and user models.",
    "files": "File management, snapshot, import, and data catalogue models.",
    "workspace": "Workspace, node, and tokenizer models.",
    "dataframe": "DataFrame and node-lineage models.",
    "data_operations": "Dataframe operation models (filter, join, replace, cast, etc.).",
    "analysis_common": "Shared analysis models (metadata, pagination, sorting, state).",
    "concordance": "Concordance analysis request and response models.",
    "quotation": "Quotation analysis request and response models.",
    "sequential_analysis": "Sequential/chart analysis request and response models.",
    "ai_annotation": "AI annotation and LLM-prompting models.",
    "topic_modeling": "Topic modeling (BERTopic) request and response models.",
    "polars_expression": "Polars expression evaluation models.",
    "token_frequencies": "Token frequency and statistics models.",
    "shared": "Shared API response wrappers and file-preview models.",
    "nodes": "Node data, filter, slice, and column-describe models.",
}

# ── Domain-specific cross-imports needed ───────────────────────────────
# {domain: [(module, import_str), ...]}
DOMAIN_CROSS_IMPORTS: Dict[str, List[str]] = {
    "concordance": ["from .analysis_common import AnalysisSorting, AnalysisTaskMetadata, AnalysisTaskState, PaginationInfo, SourceRowPagination"],
    "quotation": ["from .analysis_common import AnalysisSorting, AnalysisTaskMetadata, AnalysisTaskState, PaginationInfo, SourceRowPagination"],
    "sequential_analysis": ["from .analysis_common import AnalysisSorting, AnalysisTaskMetadata, AnalysisTaskState"],
    "ai_annotation": ["from .analysis_common import AnalysisSorting, AnalysisTaskMetadata, AnalysisTaskState, SourceRowPagination"],
    "topic_modeling": ["from .analysis_common import AnalysisSorting, AnalysisTaskMetadata, AnalysisTaskState"],
    "nodes": ["from .analysis_common import AnalysisTaskState, NodeDataFiltering, NodeDataSorting, PaginationInfo"],
    "files": [],
    "token_frequencies": ["from .analysis_common import AnalysisSorting, AnalysisTaskMetadata, AnalysisTaskState"],
    "workspace": ["from .files import FilesTaskMetadataResponse"],
}


def find_classes(lines: List[str]) -> List[Tuple[str, int, int]]:
    """Find top-level class definitions: [(name, start_0, end_0_exclusive), ...].

    start_0 is the first line of decorators+class.
    end_0_exclusive is the line after the class body.
    """
    # First, find all class header lines (^class Name...)
    class_starts: List[int] = []
    for i, line in enumerate(lines):
        if re.match(r"^class \w+", line):
            class_starts.append(i)

    classes: List[Tuple[str, int, int]] = []
    for idx, start in enumerate(class_starts):
        # Include any decorator lines immediately above
        eff_start = start
        while eff_start > 0 and lines[eff_start - 1].strip().startswith("@"):
            eff_start -= 1

        # Class name
        m = re.match(r"^class (\w+)", lines[start])
        name = m.group(1)

        # End is either the next class (prev decorator) or EOF
        if idx + 1 < len(class_starts):
            next_start = class_starts[idx + 1]
            # Move next_start back past its decorators
            while next_start > 0 and lines[next_start - 1].strip().startswith("@"):
                next_start -= 1
            end = next_start
        else:
            end = len(lines)

        classes.append((name, eff_start, end))

    return classes


def find_type_alias_lines(lines: List[str], alias_name: str) -> Optional[Tuple[int, int]]:
    """Find a top-level type alias definition and return (start, end_exclusive)."""
    for i, line in enumerate(lines):
        m = re.match(rf"^{alias_name}\s*=", line)
        if m:
            start = i
            j = i
            depth = 0
            saw_open = False
            while j < len(lines):
                stripped = lines[j].rstrip("\n")
                depth += stripped.count("[") - stripped.count("]")
                depth += stripped.count("(") - stripped.count(")")
                if "[" in stripped or "(" in stripped:
                    saw_open = True
                if saw_open and depth == 0 and (stripped.endswith("]") or stripped.endswith(")") or "]" in stripped):
                    return (start, j + 1)
                if saw_open and depth == 0:
                    # Single-line alias
                    return (start, j + 1)
                j += 1
            return (start, len(lines))
    return None


def collect_imports(class_sources: List[str], class_names: List[str]) -> List[str]:
    """Determine which imports are needed for a set of classes."""
    combined = "\n".join(class_sources)

    needs = set()
    if re.search(r"\bList\[", combined):
        needs.add("List")
    if re.search(r"\bDict\[", combined):
        needs.add("Dict")
    if re.search(r"\bUnion\[", combined):
        needs.add("Union")
    if re.search(r"\bLiteral\[", combined):
        needs.add("Literal")
    if re.search(r"\bOptional\[", combined):
        needs.add("Optional")
    if re.search(r"\bAny\b", combined):
        needs.add("Any")
    if re.search(r"\bTuple\[", combined):
        needs.add("Tuple")

    pydantic = set()
    if re.search(r"\bBaseModel\b", combined):
        pydantic.add("BaseModel")
    if re.search(r"\bField\(", combined):
        pydantic.add("Field")
    if re.search(r"\bConfigDict\b", combined):
        pydantic.add("ConfigDict")
    if re.search(r"\bmodel_validator\b", combined):
        pydantic.add("model_validator")
    if re.search(r"\bAnyHttpUrl\b", combined):
        pydantic.add("AnyHttpUrl")

    has_enum = "Enum)" in combined or "(str, Enum)" in combined
    has_base_analysis = "BaseAnalysisRequest" in combined

    lines_out = ["from __future__ import annotations\n"]
    if needs:
        lines_out.append(f"from typing import {', '.join(sorted(needs))}\n")
    if has_enum:
        lines_out.append("from enum import Enum\n")
    if pydantic:
        lines_out.append(f"from pydantic import {', '.join(sorted(pydantic))}\n")
    if has_base_analysis:
        lines_out.append("from ..analysis.models import BaseAnalysisRequest\n")
    return lines_out


def main():
    lines = INIT_PATH.read_text().splitlines(keepends=True)

    classes = find_classes(lines)
    print(f"Found {len(classes)} class definitions")

    # Verify all classes are mapped
    for name, _, _ in classes:
        if name not in CLASS_TO_DOMAIN:
            print(f"  WARNING: {name} not in CLASS_TO_DOMAIN")

    # ── Collect classes by domain ─────────────────────────────────────
    domain_classes: Dict[str, List[Tuple[str, int, int]]] = OrderedDict()
    for name, start, end in classes:
        domain = CLASS_TO_DOMAIN.get(name)
        if domain is None:
            print(f"  ERROR: {name} has no domain mapping")
            continue
        domain_classes.setdefault(domain, []).append((name, start, end))

    # ── Collect type aliases per domain ────────────────────────────────
    domain_aliases: Dict[str, List[str]] = {}
    for alias_name, domain in TYPE_ALIASES:
        alias_range = find_type_alias_lines(lines, alias_name)
        if alias_range:
            alias_src = "".join(lines[alias_range[0]:alias_range[1]])
            domain_aliases.setdefault(domain, []).append(alias_src)
        else:
            print(f"  WARNING: type alias '{alias_name}' not found")

    # ── Write domain files ────────────────────────────────────────────
    written_domains: Dict[str, List[str]] = {}
    for domain in sorted(domain_classes.keys()):
        cls_triples = domain_classes[domain]
        if not cls_triples:
            continue

        class_names = sorted({name for name, _, _ in cls_triples})

        # Collect class sources
        cls_sources = []
        for name, start, end in cls_triples:
            cls_sources.append("".join(lines[start:end]))

        import_lines = collect_imports(cls_sources, class_names)

        # Cross-imports
        cross = DOMAIN_CROSS_IMPORTS.get(domain, [])
        for ci in cross:
            import_lines.append(ci + "\n")

        desc = DOMAIN_DESCRIPTIONS.get(domain, f"Models for {domain}.")
        output = [f'"""{desc}\n\nSplit from models/__init__.py.\n"""\n\n']
        output.extend(import_lines)
        output.append("\n")

        # Classes in file order (placed before type aliases so forward refs resolve)
        cls_triples_sorted = sorted(cls_triples, key=lambda x: x[1])
        for i, (name, start, end) in enumerate(cls_triples_sorted):
            src = "".join(lines[start:end])
            output.append(src)
            output.append("\n")

        # Type aliases at the end, after all class definitions
        if domain_aliases.get(domain):
            output.append("\n")
            for alias_src in domain_aliases[domain]:
                output.append(alias_src)

        filepath = MODELS_DIR / f"{domain}.py"
        filepath.write_text("".join(output))
        written_domains[domain] = class_names
        print(f"  {domain}: {len(cls_triples)} classes -> {filepath}")

    # ── Write new __init__.py ─────────────────────────────────────────
    # Order imports to match the domain order
    domain_order = [
        "auth", "files", "workspace", "dataframe", "data_operations",
        "analysis_common", "concordance", "quotation", "sequential_analysis",
        "token_frequencies", "ai_annotation", "topic_modeling",
        "polars_expression", "shared", "nodes",
    ]

    init = [
        '"""Pydantic request/response models for backend API contracts.\n',
        "\n",
        "Re-exports from domain files under ``models/``.\n",
        "All existing imports remain valid after the split.\n",
        '"""\n',
        "\n",
    ]

    for domain in domain_order:
        if domain not in written_domains:
            continue
        names = sorted(written_domains[domain])
        if not names:
            continue
        init.append(f"from .{domain} import {', '.join(names)}\n")

    # Re-export type aliases
    init.append("\n")
    init.append("# Type aliases\n")
    alias_re_exports = set()
    for alias_name, domain in TYPE_ALIASES:
        if domain in written_domains:
            alias_re_exports.add((domain, alias_name))
    # Group by domain
    from collections import defaultdict
    aliases_by_domain = defaultdict(list)
    for dom, aname in alias_re_exports:
        aliases_by_domain[dom].append(aname)
    for dom, anames in sorted(aliases_by_domain.items()):
        init.append(f"from .{dom} import {', '.join(sorted(anames))}\n")

    init.append("\n")
    INIT_PATH.write_text("".join(init))
    print(f"\n  Rewrote __init__.py with re-exports from {len(written_domains)} domain files.")


if __name__ == "__main__":
    print(f"Splitting {INIT_PATH} into domain files...\n")
    main()
    print("\nDone.")
