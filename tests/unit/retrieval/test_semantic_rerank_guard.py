"""A lexical scorer must not silently erase a useful semantic candidate."""
from __future__ import annotations

import pytest

from jiuwen_memory.common.reranker.reranker_impl.overlap_reranker import OverlapReranker
from jiuwen_memory.common.tokenizer.tokenizer_impl.whitespace_tokenizer import WhitespaceTokenizer
from jiuwen_memory.common.type_def import MemoryUnit, RetrievalPipeline, ScoredUnit, Segment
from jiuwen_memory.retrieval.types import RecallChannel, RetrievalQuery
from tests.unit.retrieval.test_storage_pipelines import IdRecaller, _build_retriever

pytestmark = pytest.mark.unit


class SemanticCandidates(IdRecaller):
    def recall(self, scope, query, top_k):
        return [ScoredUnit("u1", 0.95, RecallChannel.VECTOR),
                ScoredUnit("u2", 0.1, RecallChannel.VECTOR)][:top_k]


def _fixture(pipeline, reranker=None):
    tokenizer = WhitespaceTokenizer()
    retriever, domain, _, scope = _build_retriever(
        pipeline, [SemanticCandidates(RecallChannel.VECTOR, "u1")],
        reranker=reranker or OverlapReranker(tokenizer),
    )
    domain.update(scope, [MemoryUnit(
        id="u1", scope=scope, segments=[Segment(content="Espresso is my usual order.")],
    )])
    domain.add(scope, [MemoryUnit(
        id="u2", scope=scope, segments=[Segment(content="Which coffee do I prefer? Unknown.")],
    )])
    return retriever, scope


@pytest.mark.parametrize("pipeline", list(RetrievalPipeline))
def test_auto_rerank_preserves_semantic_paraphrase(pipeline):
    retriever, scope = _fixture(pipeline)
    result = retriever.retrieve(scope, RetrievalQuery(
        text="Which coffee do I prefer?", top_k=2, with_trajectory=True,
    ))
    assert [item.unit_id for item in result.items] == ["u1", "u2"]
    assert result.items[0].score > result.items[1].score > 0
    assert any(step.detail.get("skipped") == "lexical_reranker_requires_explicit_opt_in"
               for step in result.trajectory)


def test_explicit_lexical_rerank_retains_opt_in_behavior():
    retriever, scope = _fixture(RetrievalPipeline.RECALL_GET_RANK)
    result = retriever.retrieve(scope, RetrievalQuery(
        text="Which coffee do I prefer?", top_k=2, rerank=True,
    ))
    assert [item.unit_id for item in result.items] == ["u2"]


def test_semantic_reranker_still_runs_automatically():
    class ModelScorer(OverlapReranker):
        def supports_semantic_ranking(self):
            return True

        def rerank(self, query, texts):
            return [0.9, 0.05]

    retriever, scope = _fixture(
        RetrievalPipeline.RECALL_GET_RANK, ModelScorer(WhitespaceTokenizer()),
    )
    result = retriever.retrieve(scope, RetrievalQuery(text="Which coffee do I prefer?"))
    assert [item.score for item in result.items] == [0.9, 0.05]


@pytest.mark.parametrize("scores", [None, [0.9], [float("nan"), 0.1], [True, 0.2]])
def test_bad_model_output_preserves_fused_results(scores):
    class BadModel(OverlapReranker):
        def supports_semantic_ranking(self):
            return True

        def rerank(self, query, texts):
            return scores

    retriever, scope = _fixture(
        RetrievalPipeline.RECALL_GET_RANK, BadModel(WhitespaceTokenizer()),
    )
    result = retriever.retrieve(scope, RetrievalQuery(
        text="Which coffee do I prefer?", with_trajectory=True,
    ))
    assert [item.unit_id for item in result.items] == ["u1", "u2"]
    assert any(step.detail.get("skipped") == "reranker_unavailable"
               for step in result.trajectory)


def test_routing_reranker_exposes_active_scorer_capability():
    from jiuwen_memory.common.reranker.reranker_impl.bge_reranker import BGEReranker
    from jiuwen_memory.config.config_source_impl.dict_config_source import DictConfigSource
    from jiuwen_memory.config.routing import ActiveRouter, RoutingReranker

    source = DictConfigSource({"reranker.active": "lexical"})
    router = ActiveRouter(
        namespace="reranker", config_source=source, default_name="lexical",
        instances={"lexical": OverlapReranker(WhitespaceTokenizer()), "model": BGEReranker()},
    )
    reranker = RoutingReranker(router)
    assert reranker.supports_semantic_ranking() is False
    source.put("reranker.active", "model")
    assert reranker.supports_semantic_ranking() is True


def test_embedder_failure_still_returns_keyword_evidence(
    world, scope, unit_factory, index_unit_fn, monkeypatch,
):
    from jiuwen_memory.common.errors import BackendError

    index_unit_fn(world, unit_factory("saved", "Espresso preference", scope=scope))

    def failed_embed_query(text):
        raise BackendError("embedding backend unavailable")

    monkeypatch.setattr(world.embedder, "embed_query", failed_embed_query)
    result = world.retriever.retrieve(scope, RetrievalQuery(text="espresso"))
    assert [item.unit_id for item in result.items] == ["saved"]
    assert len(result.errors) == 1
    assert result.errors[0].channel is RecallChannel.VECTOR
    assert result.errors[0].source == "query_embedder"
