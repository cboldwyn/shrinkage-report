"""
Regression test for v2.6.9.

A week's Inventory Reconciliation export can arrive with an entirely-empty
Reason column (the "reason none" data-pipeline drop). pandas types that column
as float64, and the "No Reason" filter in app.py used to call .str.strip() on
it, which raises AttributeError and crashes the whole dashboard.

The fix coerces Reason / Reason Note to string at the all_recon load layer.
This test reproduces the failure mode and proves the coercion makes the filter
safe. Run: python tests/test_blank_reason_column.py
"""
import io
import pandas as pd


def test_blank_reason_column_does_not_crash_filter():
    # Simulate a fresh weekly upload where every Reason cell is blank.
    csv = (
        "Date,Shop,Reason,Reason Note,COGS\n"
        "2026-05-25,DTLB,,,1.50\n"
        "2026-05-25,DTLB,,,2.00\n"
        "2026-05-26,OC,,,3.00\n"
    )
    df = pd.read_csv(io.StringIO(csv))

    # Without coercion the column is floating and .str access raises.
    assert df["Reason"].dtype == "float64"
    raised = False
    try:
        _ = df["Reason"].isna() | (df["Reason"].str.strip() == "")
    except AttributeError:
        raised = True
    assert raised, "expected the unfixed path to raise AttributeError"

    # Apply the v2.6.9 fix (mirrors the all_recon coercion in app.py).
    for col in ("Reason", "Reason Note"):
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)

    # Now the No Reason filter works and flags all blank rows.
    mask = df["Reason"].isna() | (df["Reason"].str.strip() == "")
    assert int(mask.sum()) == 3


def test_mixed_reason_column_still_classifies():
    # A normal week with some real reasons and some blanks must be unaffected.
    csv = (
        "Date,Shop,Reason,Reason Note,COGS\n"
        "2026-05-25,DTLB,OVERSOLD,,1.50\n"
        "2026-05-25,DTLB,,,2.00\n"
        "2026-05-26,OC,DAMAGED,broken,3.00\n"
    )
    df = pd.read_csv(io.StringIO(csv))
    for col in ("Reason", "Reason Note"):
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)
    mask = df["Reason"].isna() | (df["Reason"].str.strip() == "")
    assert int(mask.sum()) == 1
    assert set(df["Reason"]) == {"OVERSOLD", "", "DAMAGED"}


if __name__ == "__main__":
    test_blank_reason_column_does_not_crash_filter()
    test_mixed_reason_column_still_classifies()
    print("PASS: blank-reason regression tests")
