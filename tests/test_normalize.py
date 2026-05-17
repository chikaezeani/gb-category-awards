from pathlib import Path

import pandas as pd

from gb_awards.normalize import resolve_category


def test_mayo_sachet_below_20ml():
    mapping = pd.DataFrame(
        [
            {"mapping_key": "category::mayo", "target_category": "Mayo Dynamic"},
        ]
    )
    assert resolve_category("Mayo", "", "Bama Mayo 10ml", "", mapping) == "Mayo Sachet"


def test_mayo_jar_at_or_above_20ml():
    mapping = pd.DataFrame(
        [
            {"mapping_key": "category::mayo", "target_category": "Mayo Dynamic"},
        ]
    )
    assert resolve_category("Mayo", "", "Bama Mayo 20ml", "", mapping) == "Mayo Jar"


def test_spices_fallback_mapping():
    mapping = pd.DataFrame(columns=["mapping_key", "target_category"])
    assert resolve_category("Spices", "", "Gino Curry 3.5g", "", mapping) == "Spices"
