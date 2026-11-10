"""Validity must exclude ineligible hits before a bounded text recall."""
from datetime import UTC, datetime, timedelta

import pytest

from jiuwen_memory.common.tokenizer.tokenizer_impl.whitespace_tokenizer import WhitespaceTokenizer
from jiuwen_memory.common.type_def import (
    FilterClause,
    FilterGroup,
    FilterLogic,
    FilterOp,
    MemoryUnit,
    Scope,
    Segment,
    Temporal,
)
from jiuwen_memory.retrieval.retriever_impl.predicate_builder import build_system_filters
from jiuwen_memory.storage.shadow_impl.sqlite_shadow_index import (
    SqliteDocumentShadowIndex,
    _compile_system_filters,
)
from jiuwen_memory.storage.types import TextQuery

pytestmark = pytest.mark.unit
NOW = datetime(2026, 11, 3, 12, tzinfo=UTC)


@pytest.mark.parametrize("historical", [False, True])
def test_invalid_versions_do_not_consume_the_only_recall_slot(tmp_path, historical):
    scope = Scope(org="acme", user="alice")
    store = SqliteDocumentShadowIndex(
        db_path=str(tmp_path / "validity.db"), tokenizer=WhitespaceTokenizer(),
    )
    units = [MemoryUnit(
        id=f"future-{i}", scope=scope, segments=[Segment(content="espresso espresso")],
        temporal=Temporal(t_valid=NOW + timedelta(days=1)),
    ) for i in range(60)]
    units += [MemoryUnit(
        id="expired", scope=scope, segments=[Segment(content="espresso espresso")],
        temporal=Temporal(t_invalid=NOW),
    ), MemoryUnit(
        id="valid", scope=scope, segments=[Segment(content="espresso with other context words")],
        temporal=Temporal(t_valid=NOW),
    )]
    store.insert_units(scope, units)
    filters = FilterGroup(FilterLogic.AND, build_system_filters(
        NOW if historical else None, None, None, current_time=NOW,
    ))
    hits = store.search_fulltext(scope, TextQuery(text="espresso", top_k=1, filters=filters))
    assert [hit.id for hit in hits] == ["valid"]


def test_open_validity_start_survives_null_aware_not(tmp_path):
    scope = Scope(org="acme")
    store = SqliteDocumentShadowIndex(
        db_path=str(tmp_path / "open.db"), tokenizer=WhitespaceTokenizer(),
    )
    store.insert_units(scope, [MemoryUnit(
        id="open", scope=scope, segments=[Segment(content="espresso")],
    )])
    filters = FilterGroup(FilterLogic.AND, build_system_filters(NOW, None, None))
    assert [hit.id for hit in store.search_fulltext(
        scope, TextQuery(text="espresso", top_k=1, filters=filters),
    )] == ["open"]


def test_partial_negation_is_not_pushed_down():
    expression = FilterGroup(FilterLogic.NOT, [FilterGroup(FilterLogic.AND, [
        FilterClause("t_valid", FilterOp.GT, 123),
        FilterClause("user_metadata.unknown", FilterOp.EQ, "value"),
    ])])
    assert _compile_system_filters(expression) == (None, [])
