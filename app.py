"""
Shrinkage Report v1.0.0
Automated weekly/monthly shrinkage reporting for Haven retail locations.

Compares inventory adjustment costs (from Blaze Inventory Reconciliation History)
against sales COGS (from Blaze Total Sales Detail) to calculate shrinkage
percentages by location, category, and employee.

CHANGELOG:
v1.0.0 (2026-03-31)
- Initial release
- Four-tab layout: Location Summary, Category Detail, Employee Breakdown, Raw Data
- Two CSV inputs: Inventory Reconciliation + Total Sales Detail
- Reason filter to exclude expected adjustment types
- Excel and CSV downloads per tab
"""

import streamlit as st
import pandas as pd
import io

# ============================================================================
# CONFIGURATION
# ============================================================================

VERSION = "1.0.0"

st.set_page_config(
    page_title=f"Shrinkage Report v{VERSION}",
    page_icon="📉",
    layout="wide"
)

STORE_NAME_MAP = {
    "HAVEN - Corona": "CORONA",
    "HAVEN - Hawthorne South Bay": "HAWTHORNE",
    "HAVEN - Fresno": "FRESNO",
    "HAVEN - Maywood": "MAYWOOD",
    "HAVEN - Lakewood": "LAKEWOOD",
    "HAVEN - LB#1 - Los Alamitos": "LOS ALAMITOS",
    "HAVEN - LB#2 - Paramount": "PARAMOUNT",
    "HAVEN - LB#3 - Downtown LB": "DTLB",
    "HAVEN - LB#4 - Belmont": "BELMONT",
    "HAVEN - Orange County": "ORANGE COUNTY",
    "HAVEN - Porterville": "PORTERVILLE",
    "HAVEN - San Bernardino": "SAN BERNARDINO",
}

STORE_ORDER = list(STORE_NAME_MAP.values())

RECON_REQUIRED_COLS = [
    "Date", "Shop", "Employee Name", "Category Name",
    "Difference", "Cost per Unit", "COGS", "Reason"
]

SALES_REQUIRED_COLS = ["Shop", "Product Category", "COGS"]

EXCLUDABLE_REASONS = [
    "WASTE_DISPLAY", "SAMPLES", "WASTE_EXPIRED", "WASTE_DISPOSAL", "WASTE_RETURN"
]

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================


def validate_columns(df, required, label):
    """Check that required columns exist. Return (ok, missing_list)."""
    missing = [c for c in required if c not in df.columns]
    if missing:
        return False, missing
    return True, []


def short_store_name(full_name):
    """Map Blaze shop name to short name, fallback to original."""
    return STORE_NAME_MAP.get(full_name, full_name)


def store_sort_key(name):
    """Sort key to order stores consistently."""
    try:
        return STORE_ORDER.index(name)
    except ValueError:
        return len(STORE_ORDER)


def format_currency(val):
    """Format number as currency string."""
    if pd.isna(val):
        return "N/A"
    return f"${val:,.2f}"


def format_pct(val):
    """Format number as percentage string."""
    if pd.isna(val):
        return "N/A"
    return f"{val:.2%}"


def make_excel_download(dataframes_dict):
    """Create an Excel file with multiple sheets from a dict of {name: df}."""
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        for sheet_name, df in dataframes_dict.items():
            clean_name = sheet_name[:31]  # Excel sheet name limit
            df.to_excel(writer, index=False, sheet_name=clean_name)
    buf.seek(0)
    return buf


# ============================================================================
# DATA LOADING
# ============================================================================


def load_recon_csv(uploaded_file):
    """Load Inventory Reconciliation History CSV."""
    df = pd.read_csv(uploaded_file, low_memory=False)
    ok, missing = validate_columns(df, RECON_REQUIRED_COLS, "Inventory Reconciliation")
    if not ok:
        st.error(f"Missing columns in Inventory Reconciliation: {', '.join(missing)}")
        return None
    df["COGS"] = pd.to_numeric(df["COGS"], errors="coerce").fillna(0)
    df["Cost per Unit"] = pd.to_numeric(df["Cost per Unit"], errors="coerce").fillna(0)
    df["Difference"] = pd.to_numeric(df["Difference"], errors="coerce").fillna(0)
    df["Store"] = df["Shop"].map(short_store_name)
    return df


def load_sales_csv(uploaded_file):
    """Load Total Sales Detail CSV, keeping only needed columns."""
    try:
        df = pd.read_csv(
            uploaded_file,
            usecols=["Shop", "Product Category", "COGS"],
            low_memory=False
        )
    except ValueError:
        st.error(
            "Could not find required columns (Shop, Product Category, COGS) "
            "in the Total Sales Detail CSV."
        )
        return None
    df["COGS"] = pd.to_numeric(df["COGS"], errors="coerce").fillna(0)
    df["Store"] = df["Shop"].map(short_store_name)
    return df


# ============================================================================
# DATA PROCESSING
# ============================================================================


def aggregate_sales_cogs(sales_df):
    """Aggregate Total Sales Detail to COGS by Store + Category."""
    agg = (
        sales_df
        .groupby(["Store", "Product Category"], as_index=False)["COGS"]
        .sum()
        .rename(columns={"COGS": "Sales COGS", "Product Category": "Category"})
    )
    # Also compute store-level totals
    store_totals = (
        sales_df
        .groupby("Store", as_index=False)["COGS"]
        .sum()
        .rename(columns={"COGS": "Store Sales COGS"})
    )
    return agg, store_totals


def aggregate_adjustments(recon_df, exclude_reasons=None):
    """Aggregate inventory adjustments by Store + Category, split by reason type."""
    df = recon_df.copy()
    if exclude_reasons:
        df = df[~df["Reason"].isin(exclude_reasons)]

    # Store + Category level
    cat_detail = (
        df.groupby(["Store", "Category Name"])
        .agg(
            Adjustments=("COGS", "count"),
            OVERSOLD=("COGS", lambda x: x[x > 0].sum()),
            UNDERSOLD=("COGS", lambda x: x[x < 0].sum()),
            TRUE_AUDIT_COST=("COGS", "sum"),
        )
        .reset_index()
        .rename(columns={"Category Name": "Category"})
    )

    # Store level
    store_summary = (
        df.groupby("Store")
        .agg(
            Adjustments=("COGS", "count"),
            OVERSOLD=("COGS", lambda x: x[x > 0].sum()),
            UNDERSOLD=("COGS", lambda x: x[x < 0].sum()),
            TRUE_AUDIT_COST=("COGS", "sum"),
        )
        .reset_index()
    )

    # Employee level
    emp_detail = (
        df.groupby(["Store", "Employee Name"])
        .agg(
            Adjustments=("COGS", "count"),
            OVERSOLD=("COGS", lambda x: x[x > 0].sum()),
            UNDERSOLD=("COGS", lambda x: x[x < 0].sum()),
            TRUE_AUDIT_COST=("COGS", "sum"),
            UNDERSOLD_count=("Reason", lambda x: (x == "UNDERSOLD").sum()),
            OVERSOLD_count=("Reason", lambda x: (x == "OVERSOLD").sum()),
        )
        .reset_index()
    )

    return cat_detail, store_summary, emp_detail


def merge_with_sales(adj_df, sales_cogs_df, on_cols):
    """Merge adjustment aggregations with sales COGS and calculate shrinkage %."""
    merged = adj_df.merge(sales_cogs_df, on=on_cols, how="left")
    cogs_col = "Sales COGS" if "Sales COGS" in merged.columns else "Store Sales COGS"
    merged["Shrinkage %"] = merged.apply(
        lambda r: r["TRUE_AUDIT_COST"] / r[cogs_col]
        if pd.notna(r.get(cogs_col)) and r.get(cogs_col, 0) != 0
        else None,
        axis=1
    )
    return merged


# ============================================================================
# DISPLAY HELPERS
# ============================================================================


def style_shrinkage_table(df, pct_col="Shrinkage %"):
    """Apply conditional formatting to shrinkage percentage column."""
    def color_pct(val):
        if pd.isna(val):
            return ""
        if abs(val) > 0.05:
            return "background-color: #ffcccc"
        if abs(val) > 0.02:
            return "background-color: #fff3cd"
        return ""

    styled = df.style.map(color_pct, subset=[pct_col])
    styled = styled.format(
        {
            "OVERSOLD": "${:,.2f}",
            "UNDERSOLD": "${:,.2f}",
            "TRUE_AUDIT_COST": "${:,.2f}",
            "Sales COGS": "${:,.2f}",
            "Store Sales COGS": "${:,.2f}",
            pct_col: "{:.2%}",
        },
        na_rep="N/A"
    )
    return styled


def download_buttons(df, label, key_prefix):
    """Render CSV and Excel download buttons for a dataframe."""
    col1, col2 = st.columns(2)
    with col1:
        csv_buf = io.StringIO()
        df.to_csv(csv_buf, index=False)
        st.download_button(
            f"📥 Download CSV",
            csv_buf.getvalue(),
            file_name=f"{label}.csv",
            mime="text/csv",
            key=f"{key_prefix}_csv"
        )
    with col2:
        excel_buf = make_excel_download({label: df})
        st.download_button(
            f"📥 Download Excel",
            excel_buf,
            file_name=f"{label}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"{key_prefix}_xlsx"
        )


# ============================================================================
# MAIN APPLICATION
# ============================================================================


def main():
    st.title(f"📉 Shrinkage Report v{VERSION}")
    st.markdown("Compare inventory adjustment costs against sales COGS by location, category, and employee.")

    # -- Sidebar --
    st.sidebar.header("📂 Upload Files")
    file_recon = st.sidebar.file_uploader(
        "Inventory Reconciliation History",
        type=["csv"],
        key="recon",
        help="Blaze Inventory Reconciliation History export (CSV)."
    )
    file_sales = st.sidebar.file_uploader(
        "Total Sales Detail",
        type=["csv"],
        key="sales",
        help="Blaze Total Sales Detail export (CSV). All stores in one file."
    )

    # Status indicators
    if file_recon:
        st.sidebar.success("Inventory Reconciliation loaded")
    if file_sales:
        st.sidebar.success("Total Sales Detail loaded")

    st.sidebar.markdown("---")

    # Reason filter
    st.sidebar.subheader("Filters")
    exclude_reasons = st.sidebar.multiselect(
        "Exclude adjustment reasons:",
        options=EXCLUDABLE_REASONS,
        default=[],
        help="Exclude selected reasons from shrinkage calculations. "
             "Useful for filtering out expected adjustments like display waste or samples."
    )

    # Version
    with st.sidebar.expander("Version History"):
        st.markdown("""
        **v1.0.0** (2026-03-31)
        - Initial release
        - Location, category, employee, and raw data views
        - Reason filtering
        - Excel and CSV downloads
        """)
    st.sidebar.markdown("---")
    st.sidebar.markdown(f"**Version {VERSION}**")

    # -- Check uploads --
    if not file_recon or not file_sales:
        st.info(
            "Upload both CSV files in the sidebar to generate the shrinkage report.\n\n"
            "**Inventory Reconciliation History:** Blaze > Data Export > Inventory Reconciliation History\n\n"
            "**Total Sales Detail:** Blaze > Data Export > Total Sales Detail"
        )
        return

    # -- Load data --
    recon_df = load_recon_csv(file_recon)
    if recon_df is None:
        return

    sales_df = load_sales_csv(file_sales)
    if sales_df is None:
        return

    # -- Date range --
    try:
        recon_df["_parsed_date"] = pd.to_datetime(recon_df["Date"], format="mixed")
        date_min = recon_df["_parsed_date"].min().strftime("%m/%d/%Y")
        date_max = recon_df["_parsed_date"].max().strftime("%m/%d/%Y")
        date_range_str = f"{date_min} - {date_max}"
    except Exception:
        date_range_str = "Date range not detected"

    st.markdown(f"**Report Period:** {date_range_str}")

    # -- Process --
    sales_by_cat, sales_by_store = aggregate_sales_cogs(sales_df)
    cat_detail, store_summary, emp_detail = aggregate_adjustments(recon_df, exclude_reasons or None)

    # Merge
    store_merged = merge_with_sales(store_summary, sales_by_store, on_cols=["Store"])
    cat_merged = merge_with_sales(cat_detail, sales_by_cat, on_cols=["Store", "Category"])

    # Sort stores
    store_merged["_sort"] = store_merged["Store"].map(store_sort_key)
    store_merged = store_merged.sort_values("_sort").drop(columns="_sort")
    cat_merged["_sort"] = cat_merged["Store"].map(store_sort_key)
    cat_merged = cat_merged.sort_values(["_sort", "TRUE_AUDIT_COST"]).drop(columns="_sort")
    emp_detail["_sort"] = emp_detail["Store"].map(store_sort_key)
    emp_detail = emp_detail.sort_values(["_sort", "TRUE_AUDIT_COST"]).drop(columns="_sort")

    # Network totals
    net_adj = store_merged["TRUE_AUDIT_COST"].sum()
    net_cogs = store_merged["Store Sales COGS"].sum()
    net_pct = net_adj / net_cogs if net_cogs != 0 else None

    # -- Tabs --
    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 Location Summary",
        "📂 Category Detail",
        "👤 Employee Breakdown",
        "📄 Raw Data"
    ])

    # == Tab 1: Location Summary ==
    with tab1:
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Shrinkage", format_currency(net_adj))
        with col2:
            st.metric("Total Sales COGS", format_currency(net_cogs))
        with col3:
            st.metric("Network Shrinkage %", format_pct(net_pct))

        display_cols = ["Store", "OVERSOLD", "UNDERSOLD", "TRUE_AUDIT_COST", "Store Sales COGS", "Shrinkage %"]
        avail_cols = [c for c in display_cols if c in store_merged.columns]
        display_df = store_merged[avail_cols].copy()

        # Grand total row
        totals = {
            "Store": "GRAND TOTAL",
            "OVERSOLD": display_df["OVERSOLD"].sum(),
            "UNDERSOLD": display_df["UNDERSOLD"].sum(),
            "TRUE_AUDIT_COST": display_df["TRUE_AUDIT_COST"].sum(),
        }
        if "Store Sales COGS" in display_df.columns:
            totals["Store Sales COGS"] = display_df["Store Sales COGS"].sum()
            total_cogs = totals["Store Sales COGS"]
            totals["Shrinkage %"] = totals["TRUE_AUDIT_COST"] / total_cogs if total_cogs != 0 else None
        totals_df = pd.DataFrame([totals])
        display_with_total = pd.concat([display_df, totals_df], ignore_index=True)

        styled = style_shrinkage_table(display_with_total)
        st.dataframe(styled, use_container_width=True, hide_index=True)

        if exclude_reasons:
            st.caption(f"Excluded reasons: {', '.join(exclude_reasons)}")

        download_buttons(display_with_total, "location_summary", "loc")

    # == Tab 2: Category Detail ==
    with tab2:
        stores_available = sorted(cat_merged["Store"].unique(), key=store_sort_key)
        selected_stores = st.multiselect(
            "Filter by location:",
            options=stores_available,
            default=stores_available,
            key="cat_store_filter"
        )
        filtered_cat = cat_merged[cat_merged["Store"].isin(selected_stores)].copy()

        display_cols = ["Store", "Category", "Adjustments", "TRUE_AUDIT_COST", "Sales COGS", "Shrinkage %"]
        avail_cols = [c for c in display_cols if c in filtered_cat.columns]
        cat_display = filtered_cat[avail_cols].copy()

        fmt = {
            "TRUE_AUDIT_COST": "${:,.2f}",
            "Sales COGS": "${:,.2f}",
            "Shrinkage %": "{:.2%}",
        }
        styled_cat = cat_display.style.format(fmt, na_rep="N/A")
        st.dataframe(styled_cat, use_container_width=True, hide_index=True)

        st.caption(f"{len(cat_display)} rows")
        download_buttons(cat_display, "category_detail", "cat")

    # == Tab 3: Employee Breakdown ==
    with tab3:
        stores_available_emp = sorted(emp_detail["Store"].unique(), key=store_sort_key)
        selected_stores_emp = st.multiselect(
            "Filter by location:",
            options=stores_available_emp,
            default=stores_available_emp,
            key="emp_store_filter"
        )
        filtered_emp = emp_detail[emp_detail["Store"].isin(selected_stores_emp)].copy()

        # Merge store-level COGS for employee %
        filtered_emp = filtered_emp.merge(sales_by_store, on="Store", how="left")
        filtered_emp["% of Store COGS"] = filtered_emp.apply(
            lambda r: r["TRUE_AUDIT_COST"] / r["Store Sales COGS"]
            if pd.notna(r.get("Store Sales COGS")) and r.get("Store Sales COGS", 0) != 0
            else None,
            axis=1
        )

        display_cols = [
            "Store", "Employee Name", "Adjustments",
            "OVERSOLD", "OVERSOLD_count", "UNDERSOLD", "UNDERSOLD_count",
            "TRUE_AUDIT_COST", "% of Store COGS"
        ]
        avail_cols = [c for c in display_cols if c in filtered_emp.columns]
        emp_display = filtered_emp[avail_cols].copy()

        fmt_emp = {
            "OVERSOLD": "${:,.2f}",
            "UNDERSOLD": "${:,.2f}",
            "TRUE_AUDIT_COST": "${:,.2f}",
            "% of Store COGS": "{:.2%}",
        }
        styled_emp = emp_display.style.format(fmt_emp, na_rep="N/A")
        st.dataframe(styled_emp, use_container_width=True, hide_index=True)

        st.caption(f"{len(emp_display)} rows")
        download_buttons(emp_display, "employee_breakdown", "emp")

    # == Tab 4: Raw Data ==
    with tab4:
        raw = recon_df.copy()
        if exclude_reasons:
            raw = raw[~raw["Reason"].isin(exclude_reasons)]

        # Filters
        col_f1, col_f2, col_f3 = st.columns(3)
        with col_f1:
            shop_filter = st.multiselect(
                "Store:",
                options=sorted(raw["Store"].unique(), key=store_sort_key),
                key="raw_shop"
            )
        with col_f2:
            cat_filter = st.multiselect(
                "Category:",
                options=sorted(raw["Category Name"].dropna().unique()),
                key="raw_cat"
            )
        with col_f3:
            reason_filter = st.multiselect(
                "Reason:",
                options=sorted(raw["Reason"].dropna().unique()),
                key="raw_reason"
            )

        if shop_filter:
            raw = raw[raw["Store"].isin(shop_filter)]
        if cat_filter:
            raw = raw[raw["Category Name"].isin(cat_filter)]
        if reason_filter:
            raw = raw[raw["Reason"].isin(reason_filter)]

        display_raw_cols = [
            "Date", "Store", "Employee Name", "Inventory Name",
            "Product Name", "Brand Name", "Category Name",
            "Difference", "Cost per Unit", "COGS", "Reason", "Reason Note"
        ]
        avail_raw = [c for c in display_raw_cols if c in raw.columns]
        raw_display = raw[avail_raw].reset_index(drop=True)

        st.dataframe(raw_display, use_container_width=True, hide_index=True)
        st.caption(f"{len(raw_display)} rows")
        download_buttons(raw_display, "raw_data", "raw")


if __name__ == "__main__":
    main()
