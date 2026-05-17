import pandas as pd

from gb_awards.ranking import compute_awards


def test_highest_weighted_score_gets_rank_one():
    weights = {
        "sell_in_growth": 0.3,
        "sell_out_growth": 0.3,
        "distance": 0.2,
        "cash_reconciliation": 0.2,
    }
    sell_out = pd.DataFrame(
        [
            {"year": 2024, "month": 1, "target_category": "Tomato", "field_rep_username": "a", "area": "Area A", "region": "North", "sell_out_value": 100, "distance_score": 0.9, "cash_recon_score": 0.9},
            {"year": 2025, "month": 1, "target_category": "Tomato", "field_rep_username": "a", "area": "Area A", "region": "North", "sell_out_value": 150, "distance_score": 0.9, "cash_recon_score": 0.9},
            {"year": 2024, "month": 1, "target_category": "Tomato", "field_rep_username": "b", "area": "Area B", "region": "South", "sell_out_value": 100, "distance_score": 0.7, "cash_recon_score": 0.7},
            {"year": 2025, "month": 1, "target_category": "Tomato", "field_rep_username": "b", "area": "Area B", "region": "South", "sell_out_value": 120, "distance_score": 0.7, "cash_recon_score": 0.7},
        ]
    )
    sell_in = pd.DataFrame(
        [
            {"year": 2024, "month": 1, "target_category": "Tomato", "KAM": "k1", "area": "Area A", "region": "North", "sell_in_value": 100},
            {"year": 2025, "month": 1, "target_category": "Tomato", "KAM": "k1", "area": "Area A", "region": "North", "sell_in_value": 160},
            {"year": 2024, "month": 1, "target_category": "Tomato", "KAM": "k2", "area": "Area B", "region": "South", "sell_in_value": 100},
            {"year": 2025, "month": 1, "target_category": "Tomato", "KAM": "k2", "area": "Area B", "region": "South", "sell_in_value": 110},
        ]
    )
    awards = compute_awards(sell_out, sell_in, weights, "area")
    top = awards.sort_values("Rank").iloc[0]
    assert top["Area"] == "Area A"
    assert top["Rank"] == 1


def test_zero_baseline_growth_stays_unavailable():
    weights = {
        "sell_in_growth": 0.3,
        "sell_out_growth": 0.3,
        "distance": 0.2,
        "cash_reconciliation": 0.2,
    }
    sell_out = pd.DataFrame(
        [
            {"year": 2025, "month": 1, "target_category": "Tomato", "field_rep_username": "a", "area": "Area A", "region": "North", "sell_out_value": 100, "distance_score": 1.0, "cash_recon_score": 1.0},
        ]
    )
    sell_in = pd.DataFrame(
        [
            {"year": 2025, "month": 1, "target_category": "Tomato", "KAM": "k1", "area": "Area A", "region": "North", "sell_in_value": 100},
        ]
    )
    awards = compute_awards(sell_out, sell_in, weights, "area")
    assert pd.isna(awards.loc[0, "Sell In Growth vs YA"])
    assert pd.isna(awards.loc[0, "Sell Out Growth vs YA"])
