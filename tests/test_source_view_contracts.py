from __future__ import annotations

from freqinout.core.source_view_contracts import (
    VIEW_ATTENTION,
    VIEW_COMPOSE,
    VIEW_MAP_CONTEXT,
    VIEW_TRAFFIC_INBOX,
    all_source_contracts,
    contracts_for_view,
    normalize_source_family,
    source_contract_for,
    source_contracts_missing_gates,
)


def test_future_source_priority_contracts_are_complete_and_ordered() -> None:
    contracts = all_source_contracts()

    assert [contract.family for contract in contracts[:4]] == ["meshcore", "mqtt", "aprs", "reticulum"]
    assert source_contracts_missing_gates(contracts[:4]) == {}
    assert all(contract.complete for contract in contracts[:4])


def test_source_contract_aliases_match_product_language() -> None:
    assert normalize_source_family("Mesh MQTT") == "mqtt"
    assert normalize_source_family("Reticulum/LXMF") == "reticulum"
    assert normalize_source_family("JS8Spotter") == "fiospotter"
    assert normalize_source_family("CommStat RF") == "commstat"
    assert normalize_source_family("BBS") == "varac"


def test_aprs_contract_limits_volume_and_map_actions() -> None:
    contract = source_contract_for("aprs")

    assert contract.default_view == VIEW_MAP_CONTEXT
    assert contract.retention.rollup_required is True
    assert contract.map_scaling.clustering_required is True
    assert contract.map_scaling.default_marker_limit == 250
    assert contract.actions.map is True
    assert contract.actions.reply is False
    assert contract.actions.compose is False


def test_meshcore_and_lxmf_can_feed_attention_inbox_map_and_compose() -> None:
    for family in ("meshcore", "reticulum"):
        contract = source_contract_for(family)
        assert contract.supports_view(VIEW_ATTENTION)
        assert contract.supports_view(VIEW_TRAFFIC_INBOX)
        assert contract.supports_view(VIEW_MAP_CONTEXT)
        assert contract.supports_view(VIEW_COMPOSE)


def test_commstat_contract_matches_fio_compose_workflow() -> None:
    contract = source_contract_for("commstat_rf")

    assert contract.family == "commstat"
    assert contract.actions.read is True
    assert contract.actions.reply is True
    assert contract.actions.compose is True
    assert contract.actions.map is True


def test_view_lookup_returns_only_sources_allowed_for_template() -> None:
    compose_sources = {contract.family for contract in contracts_for_view(VIEW_COMPOSE)}

    assert "meshcore" in compose_sources
    assert "reticulum" in compose_sources
    assert "aprs" not in compose_sources
