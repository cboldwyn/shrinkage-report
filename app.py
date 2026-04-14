"""
Shrinkage Dashboard v2.0.0
Persistent shrinkage dashboard for Haven retail locations.

Tracks inventory adjustment costs against sales COGS with weekly/monthly
trend analysis. Data persists in Google Sheets, refreshed weekly via CSV upload.

Report types filter by reason groupings:
- Shrinkage (default): OVERSOLD + UNDERSOLD only
- All Adjustments: every reason, grouped by category
- Samples, Display, Damaged, Expired, Other: individual groups

CHANGELOG:
v2.0.0 (2026-04-14)
- Google Sheets persistence (data accumulates weekly)
- Report presets with reason groupings (replaces exclude filter)
- Weekly/monthly period toggle
- Plotly trend charts: network, per-store, reason composition, top categories
- Switched from exclude-based to include-based reason filtering
- Fixed bug: v1.0 included all reasons as "shrinkage" (should be OVERSOLD+UNDERSOLD only)
v1.0.0 (2026-03-31)
- Initial release
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import io
from datetime import datetime

try:
    from google.oauth2.service_account import Credentials
    import gspread
    from gspread_dataframe import get_as_dataframe
    HAS_GSPREAD = True
except ImportError:
    HAS_GSPREAD = False

# ============================================================================
# CONFIGURATION
# ============================================================================

VERSION = "2.3.0"

st.set_page_config(
    page_title=f"Shrinkage Dashboard v{VERSION}",
    page_icon="📉",
    layout="wide",
)

# -- Google Sheets --
# Update this URL after creating the "Haven Shrinkage Data" spreadsheet
SHEETS_URL = "https://docs.google.com/spreadsheets/d/1L2Obnx3PErGvGUpzB8KmDpJTw--4gjVFoBN-QB402-c"
RECON_WORKSHEET = "recon_data"
SALES_WORKSHEET = "sales_cogs"

# -- Store mapping --
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

# -- Required CSV columns --
RECON_REQUIRED_COLS = [
    "Date", "Shop", "Employee Name", "Category Name",
    "Difference", "Cost per Unit", "COGS", "Reason",
]
RECON_STORE_COLS = [
    "Date", "Shop", "Store", "Employee Name", "Category Name",
    "Inventory Name", "Product Name", "Brand Name",
    "Difference", "Cost per Unit", "COGS", "Reason", "Reason Note",
]
SALES_REQUIRED_COLS = ["Date", "Shop", "Product Category", "COGS"]

# -- Reason system --
ALL_REASONS = [
    "OVERSOLD", "UNDERSOLD", "DAMAGED", "WASTE_DISPLAY", "DISPLAY_SAMPLE",
    "SAMPLES", "WASTE_EXPIRED", "WASTE_RETURN", "WASTE_DISPOSAL",
    "AUDIT", "INCORRECT_QUANTITY", "OTHER",
    "PUBLIC_SAFETY_RECALL", "MANDATED_DESTRUCTION", "RETURN_TO_VENDOR",
]

REASON_GROUPS = {
    "Shrinkage": ["OVERSOLD", "UNDERSOLD"],
    "Samples": ["SAMPLES"],
    "Display": ["WASTE_DISPLAY", "DISPLAY_SAMPLE"],
    "Damaged": ["DAMAGED"],
    "Expired": ["WASTE_EXPIRED"],
    "Incorrect Qty": ["INCORRECT_QUANTITY"],
    "Other": [
        "WASTE_RETURN", "WASTE_DISPOSAL", "AUDIT",
        "OTHER", "PUBLIC_SAFETY_RECALL", "MANDATED_DESTRUCTION", "RETURN_TO_VENDOR",
    ],
}

REPORT_PRESETS = {
    "Shrinkage": ["Shrinkage"],
    "All Adjustments": list(REASON_GROUPS.keys()),
    "Samples": ["Samples"],
    "Display": ["Display"],
    "Damaged": ["Damaged"],
    "Expired": ["Expired"],
    "Other": ["Other"],
}

# -- Haven branding --
COLOR_PRIMARY = "#3DC0CC"
COLOR_ACCENT = "#FFCA45"
COLOR_ALERT = "#9E1F63"

GROUP_COLORS = {
    "Shrinkage": COLOR_PRIMARY,
    "Samples": "#8E44AD",
    "Display": COLOR_ACCENT,
    "Damaged": COLOR_ALERT,
    "Expired": "#E67E22",
    "Other": "#95A5A6",
}


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
    """Map Blaze shop name to short display name."""
    return STORE_NAME_MAP.get(full_name, full_name)


def store_sort_key(name):
    """Sort key to order stores consistently."""
    try:
        return STORE_ORDER.index(name)
    except ValueError:
        return len(STORE_ORDER)


def format_currency(val):
    if pd.isna(val):
        return "N/A"
    return f"${val:,.2f}"


def format_pct(val):
    if pd.isna(val):
        return "N/A"
    return f"{val:.2%}"


def get_week_id(dt):
    """Sunday-Saturday business week ID. Returns Sunday's date as 'YYYY-MM-DD'."""
    days_since_sunday = (dt.weekday() + 1) % 7
    sunday = dt - timedelta(days=days_since_sunday)
    return sunday.strftime("%Y-%m-%d")


def get_month_id(dt):
    """Year-month string from a date (e.g. '2026-03')."""
    return f"{dt.year}-{dt.month:02d}"


def week_id_to_label(week_id):
    """Convert week_id to 'Dec 28 - Jan 03' label."""
    try:
        sunday = datetime.strptime(week_id, "%Y-%m-%d").date()
        saturday = sunday + timedelta(days=6)
        if sunday.month == saturday.month:
            return f"{sunday.strftime('%b %d')} - {saturday.strftime('%d')}"
        return f"{sunday.strftime('%b %d')} - {saturday.strftime('%b %d')}"
    except Exception:
        return week_id


def month_id_to_label(month_id):
    """Convert '2026-03' to 'March 2026'."""
    try:
        year, month = int(month_id.split("-")[0]), int(month_id.split("-")[1])
        from datetime import date as date_cls
        return date_cls(year, month, 1).strftime("%B %Y")
    except Exception:
        return month_id


def period_label(period_id, period_key="weekly"):
    """Human-readable label for a period ID."""
    if period_key == "weekly":
        return week_id_to_label(period_id)
    return month_id_to_label(period_id)


def get_reasons_for_report(report_name, custom_groups=None):
    """Return flat list of reason strings for a report type."""
    if report_name == "Custom" and custom_groups:
        groups = custom_groups
    else:
        groups = REPORT_PRESETS.get(report_name, ["Shrinkage"])
    reasons = []
    for g in groups:
        reasons.extend(REASON_GROUPS.get(g, []))
    return reasons


def make_excel_download(dataframes_dict):
    """Create an Excel file with multiple sheets from {name: df}."""
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        for sheet_name, df in dataframes_dict.items():
            df.to_excel(writer, index=False, sheet_name=sheet_name[:31])
    buf.seek(0)
    return buf


# ============================================================================
# GOOGLE SHEETS I/O
# ============================================================================


def has_sheets_config():
    """Check if Google Sheets persistence is configured."""
    if not HAS_GSPREAD or not SHEETS_URL:
        return False
    try:
        return bool(st.secrets.get("google_sheets"))
    except Exception:
        return False


def get_gspread_client():
    """Authorize gspread with read+write access."""
    creds = Credentials.from_service_account_info(
        st.secrets["google_sheets"],
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)


@st.cache_data(ttl=300, show_spinner="Loading data from Google Sheets...")
def load_recon_from_sheets():
    """Read all recon data from Google Sheets."""
    try:
        client = get_gspread_client()
        sheet = client.open_by_url(SHEETS_URL)
        ws = sheet.worksheet(RECON_WORKSHEET)
        df = get_as_dataframe(ws, parse_dates=False, header=0)
        df = df.dropna(how="all")
        if df.empty:
            return pd.DataFrame()
        df["COGS"] = pd.to_numeric(df["COGS"], errors="coerce").fillna(0)
        df["Difference"] = pd.to_numeric(df["Difference"], errors="coerce").fillna(0)
        df["Cost per Unit"] = pd.to_numeric(df["Cost per Unit"], errors="coerce").fillna(0)
        df["_date"] = pd.to_datetime(df["Date"], format="mixed", errors="coerce")
        return df
    except Exception as e:
        st.error(f"Failed to load recon data: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300, show_spinner="Loading sales data from Google Sheets...")
def load_sales_from_sheets():
    """Read all sales COGS data from Google Sheets."""
    try:
        client = get_gspread_client()
        sheet = client.open_by_url(SHEETS_URL)
        ws = sheet.worksheet(SALES_WORKSHEET)
        df = get_as_dataframe(ws, parse_dates=False, header=0)
        df = df.dropna(how="all")
        if df.empty:
            return pd.DataFrame()
        df["Sales COGS"] = pd.to_numeric(df["Sales COGS"], errors="coerce").fillna(0)
        return df
    except Exception as e:
        st.error(f"Failed to load sales data: {e}")
        return pd.DataFrame()


def get_stored_week_ids():
    """Return set of week_ids already in Google Sheets recon data."""
    try:
        client = get_gspread_client()
        sheet = client.open_by_url(SHEETS_URL)
        ws = sheet.worksheet(RECON_WORKSHEET)
        col_values = ws.col_values(1)  # week_id is first column
        return set(col_values[1:])  # skip header
    except Exception:
        return set()


def append_to_sheets(df, worksheet_name):
    """Append rows to a Google Sheets worksheet. Writes headers if sheet is empty."""
    client = get_gspread_client()
    sheet = client.open_by_url(SHEETS_URL)
    ws = sheet.worksheet(worksheet_name)

    # Write header row if the sheet is empty
    existing = ws.get_all_values()
    if not existing:
        ws.append_row(df.columns.tolist(), value_input_option="USER_ENTERED")

    # Convert NaN/NaT to empty strings for JSON serialization
    clean = df.fillna("").astype(str)
    # Restore numeric columns as numbers (not strings)
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            clean[col] = df[col].fillna(0)
    rows = clean.values.tolist()
    ws.append_rows(rows, value_input_option="USER_ENTERED")


# ============================================================================
# DATA LOADING (CSV UPLOAD)
# ============================================================================


def load_recon_csv(uploaded_file):
    """Load and validate Inventory Reconciliation History CSV for upload."""
    df = pd.read_csv(uploaded_file, low_memory=False)
    ok, missing = validate_columns(df, RECON_REQUIRED_COLS, "Recon")
    if not ok:
        st.error(f"Missing columns in Inventory Reconciliation: {', '.join(missing)}")
        return None

    df["COGS"] = pd.to_numeric(df["COGS"], errors="coerce").fillna(0)
    df["Cost per Unit"] = pd.to_numeric(df["Cost per Unit"], errors="coerce").fillna(0)
    df["Difference"] = pd.to_numeric(df["Difference"], errors="coerce").fillna(0)
    df["Store"] = df["Shop"].map(short_store_name)
    df["_date"] = pd.to_datetime(df["Date"], format="mixed", errors="coerce")
    df["week_id"] = df["_date"].apply(lambda d: get_week_id(d) if pd.notna(d) else None)

    # Keep only needed columns for storage
    store_cols = ["week_id"] + [c for c in RECON_STORE_COLS if c in df.columns]
    result = df[store_cols].copy()
    result["uploaded_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return result


def load_sales_csv(uploaded_file):
    """Load Total Sales Detail CSV, extract needed columns, aggregate by week+store+category."""
    try:
        df = pd.read_csv(
            uploaded_file,
            usecols=["Date", "Shop", "Product Category", "COGS"],
            low_memory=False,
        )
    except ValueError:
        st.error(
            "Could not find required columns (Date, Shop, Product Category, COGS) "
            "in the Total Sales Detail CSV."
        )
        return None

    df["COGS"] = pd.to_numeric(df["COGS"], errors="coerce").fillna(0)
    df["_date"] = pd.to_datetime(df["Date"], format="mixed", errors="coerce")
    df["week_id"] = df["_date"].apply(lambda d: get_week_id(d) if pd.notna(d) else None)
    df["Store"] = df["Shop"].map(short_store_name)

    # Aggregate to week + store + category level (~200 rows per week)
    agg = (
        df.groupby(["week_id", "Store", "Product Category"], as_index=False)["COGS"]
        .sum()
        .rename(columns={"Product Category": "Category", "COGS": "Sales COGS"})
    )
    agg["uploaded_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return agg


# ============================================================================
# DATA PROCESSING
# ============================================================================


def aggregate_adjustments(recon_df, include_reasons=None):
    """Aggregate inventory adjustments filtered by included reasons.

    Returns: (store_summary, cat_detail, emp_detail) DataFrames.
    """
    df = recon_df.copy()
    if include_reasons:
        df = df[df["Reason"].isin(include_reasons)]

    if df.empty:
        empty = pd.DataFrame()
        return empty, empty, empty

    # Store level
    store_summary = (
        df.groupby("Store")
        .agg(
            Adjustments=("COGS", "count"),
            Gains=("COGS", lambda x: x[x > 0].sum()),
            Losses=("COGS", lambda x: x[x < 0].sum()),
            Net_Adjustment=("COGS", "sum"),
        )
        .reset_index()
    )

    # Store + Category level
    cat_detail = (
        df.groupby(["Store", "Category Name"])
        .agg(
            Adjustments=("COGS", "count"),
            Gains=("COGS", lambda x: x[x > 0].sum()),
            Losses=("COGS", lambda x: x[x < 0].sum()),
            Net_Adjustment=("COGS", "sum"),
        )
        .reset_index()
        .rename(columns={"Category Name": "Category"})
    )

    # Store + Employee level
    emp_detail = (
        df.groupby(["Store", "Employee Name"])
        .agg(
            Adjustments=("COGS", "count"),
            Gains=("COGS", lambda x: x[x > 0].sum()),
            Losses=("COGS", lambda x: x[x < 0].sum()),
            Net_Adjustment=("COGS", "sum"),
        )
        .reset_index()
    )

    return store_summary, cat_detail, emp_detail


def merge_with_sales(adj_df, sales_df, on_cols):
    """Merge adjustment aggregations with sales COGS and compute shrinkage %."""
    if adj_df.empty:
        return adj_df
    merged = adj_df.merge(sales_df, on=on_cols, how="left")
    cogs_col = "Sales COGS" if "Sales COGS" in merged.columns else "Store Sales COGS"
    merged["Shrinkage %"] = merged.apply(
        lambda r: r["Net_Adjustment"] / r[cogs_col]
        if pd.notna(r.get(cogs_col)) and r.get(cogs_col, 0) != 0
        else None,
        axis=1,
    )
    return merged


def build_period_trend(recon_df, sales_df, period="weekly", include_reasons=None):
    """Build period-level trend data for charts.

    Returns DataFrame with one row per period per store:
    period_id | Store | Net_Adjustment | Sales COGS | Shrinkage %
    """
    df = recon_df.copy()
    if include_reasons:
        df = df[df["Reason"].isin(include_reasons)]

    if df.empty or sales_df.empty:
        return pd.DataFrame()

    # Assign period_id
    if "_date" not in df.columns:
        df["_date"] = pd.to_datetime(df["Date"], format="mixed", errors="coerce")
    if period == "weekly":
        df["period_id"] = df["_date"].apply(
            lambda d: get_week_id(d) if pd.notna(d) else None
        )
        period_col = "week_id"
    else:
        df["period_id"] = df["_date"].apply(
            lambda d: get_month_id(d) if pd.notna(d) else None
        )
        period_col = "month_id"

    # Adjustments by period + store
    adj = (
        df.groupby(["period_id", "Store"], as_index=False)["COGS"]
        .sum()
        .rename(columns={"COGS": "Net_Adjustment"})
    )

    # Sales by period + store
    if period == "weekly":
        sales_period = sales_df.rename(columns={"week_id": "period_id"})
    else:
        # Derive month from week_id for sales data
        sales_period = sales_df.copy()
        def week_to_month_inner(wid):
            if pd.isna(wid) or not isinstance(wid, str):
                return None
            try:
                d = datetime.strptime(wid, "%Y-%m-%d").date()
                return f"{d.year}-{d.month:02d}"
            except Exception:
                return None
        sales_period["period_id"] = sales_period["week_id"].apply(week_to_month_inner)

    sales_agg = (
        sales_period.groupby(["period_id", "Store"], as_index=False)["Sales COGS"]
        .sum()
    )

    # Merge
    merged = adj.merge(sales_agg, on=["period_id", "Store"], how="left")
    merged["Shrinkage %"] = merged.apply(
        lambda r: r["Net_Adjustment"] / r["Sales COGS"]
        if pd.notna(r.get("Sales COGS")) and r.get("Sales COGS", 0) != 0
        else None,
        axis=1,
    )

    return merged.sort_values("period_id")


def build_reason_trend(recon_df, period="weekly"):
    """Build period-level data grouped by reason group for composition charts."""
    df = recon_df.copy()
    if "_date" not in df.columns:
        df["_date"] = pd.to_datetime(df["Date"], format="mixed", errors="coerce")

    if period == "weekly":
        df["period_id"] = df["_date"].apply(
            lambda d: get_week_id(d) if pd.notna(d) else None
        )
    else:
        df["period_id"] = df["_date"].apply(
            lambda d: get_month_id(d) if pd.notna(d) else None
        )

    # Map each reason to its group
    reason_to_group = {}
    for group, reasons in REASON_GROUPS.items():
        for r in reasons:
            reason_to_group[r] = group
    df["Reason Group"] = df["Reason"].map(reason_to_group).fillna("Other")

    agg = (
        df.groupby(["period_id", "Reason Group"], as_index=False)["COGS"]
        .sum()
        .rename(columns={"COGS": "Net_Adjustment"})
    )
    return agg.sort_values("period_id")


# ============================================================================
# CHART BUILDERS
# ============================================================================


def apply_period_labels(fig, period_ids):
    """Replace raw period_id x-axis ticks with human-readable date labels."""
    unique = sorted(set(str(p) for p in period_ids if pd.notna(p)))
    pkey = "weekly" if any("-W" in p for p in unique) else "monthly"
    labels = [period_label(p, pkey) for p in unique]
    fig.update_xaxes(ticktext=labels, tickvals=unique, tickangle=-45)


def build_network_trend(trend_data):
    """Network-level shrinkage % over time with rolling average."""
    if trend_data.empty:
        st.info("Not enough data for trend charts. Upload more weeks.")
        return

    network = (
        trend_data.groupby("period_id", as_index=False)
        .agg({"Net_Adjustment": "sum", "Sales COGS": "sum"})
    )
    network["Shrinkage %"] = network.apply(
        lambda r: r["Net_Adjustment"] / r["Sales COGS"]
        if r["Sales COGS"] != 0 else None, axis=1
    )
    network = network.dropna(subset=["Shrinkage %"]).sort_values("period_id")
    if network.empty:
        return

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=network["period_id"], y=network["Shrinkage %"],
        mode="lines+markers", name="Shrinkage %",
        line=dict(color=COLOR_PRIMARY, width=2),
        hovertemplate="%{x}: %{y:.2%}<extra></extra>",
    ))
    if len(network) >= 4:
        rolling = network["Shrinkage %"].rolling(4, min_periods=4).mean()
        fig.add_trace(go.Scatter(
            x=network["period_id"], y=rolling,
            mode="lines", name="4-period avg",
            line=dict(dash="dash", width=1, color=COLOR_PRIMARY),
            opacity=0.5, showlegend=True,
        ))
    fig.add_hline(y=0, line_dash="dot", line_color="gray", opacity=0.5)
    apply_period_labels(fig, network["period_id"])
    fig.update_layout(
        title="Network Shrinkage % Over Time",
        height=400, yaxis_tickformat=".2%",
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=-0.25),
    )
    st.plotly_chart(fig, use_container_width=True)


def build_store_trend(trend_data):
    """Per-store shrinkage % over time."""
    if trend_data.empty:
        return
    data = trend_data.dropna(subset=["Shrinkage %"]).copy()
    if data.empty:
        return
    data["_sort"] = data["Store"].map(store_sort_key)
    data = data.sort_values(["_sort", "period_id"])

    fig = px.line(
        data, x="period_id", y="Shrinkage %", color="Store",
        markers=True,
        labels={"period_id": "Period", "Shrinkage %": "Shrinkage %"},
    )
    fig.add_hline(y=0, line_dash="dot", line_color="gray", opacity=0.5)
    apply_period_labels(fig, data["period_id"])
    fig.update_layout(
        title="Shrinkage % by Store",
        height=500, yaxis_tickformat=".2%",
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=-0.35),
    )
    st.plotly_chart(fig, use_container_width=True)


def build_reason_composition(reason_trend):
    """Stacked area chart showing adjustment COGS by reason group over time."""
    if reason_trend.empty:
        return
    fig = px.area(
        reason_trend, x="period_id", y="Net_Adjustment", color="Reason Group",
        color_discrete_map=GROUP_COLORS,
        labels={"period_id": "Period", "Net_Adjustment": "Adjustment COGS ($)"},
    )
    apply_period_labels(fig, reason_trend["period_id"])
    fig.update_layout(
        title="Adjustment COGS by Reason Group",
        height=400,
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=-0.25),
    )
    st.plotly_chart(fig, use_container_width=True)


def build_top_categories(cat_detail, n=10):
    """Horizontal bar chart of top categories by absolute shrinkage."""
    if cat_detail.empty or "Shrinkage %" not in cat_detail.columns:
        return

    data = cat_detail.dropna(subset=["Shrinkage %"]).copy()
    data["abs_pct"] = data["Shrinkage %"].abs()
    top = data.nlargest(n, "abs_pct")

    if top.empty:
        return

    top["Label"] = top["Store"] + " / " + top["Category"]
    top = top.sort_values("Shrinkage %")

    colors = [COLOR_ALERT if v < 0 else COLOR_PRIMARY for v in top["Shrinkage %"]]

    fig = go.Figure(go.Bar(
        x=top["Shrinkage %"], y=top["Label"],
        orientation="h", marker_color=colors,
        hovertemplate="%{y}: %{x:.2%}<extra></extra>",
    ))

    fig.add_vline(x=0, line_color="gray", opacity=0.5)
    fig.update_layout(
        title=f"Top {n} Categories by Shrinkage %",
        height=max(300, n * 35),
        xaxis_tickformat=".1%", xaxis_title="Shrinkage %",
        yaxis_title="",
    )
    st.plotly_chart(fig, use_container_width=True)


# ============================================================================
# DISPLAY HELPERS
# ============================================================================


def style_shrinkage_table(df, pct_col="Shrinkage %"):
    """Conditional formatting for shrinkage percentage column."""
    def color_pct(val):
        if pd.isna(val):
            return ""
        if abs(val) > 0.05:
            return "background-color: #ffcccc"
        if abs(val) > 0.02:
            return "background-color: #fff3cd"
        return ""

    fmt = {
        "Gains": "${:,.2f}",
        "Losses": "${:,.2f}",
        "Net_Adjustment": "${:,.2f}",
        "Sales COGS": "${:,.2f}",
        "Store Sales COGS": "${:,.2f}",
        pct_col: "{:.2%}",
    }
    # Only format columns that exist
    fmt = {k: v for k, v in fmt.items() if k in df.columns}

    styled = df.style.map(color_pct, subset=[pct_col])
    styled = styled.format(fmt, na_rep="N/A")
    return styled


def download_buttons(df, label, key_prefix):
    """Render CSV and Excel download buttons."""
    col1, col2 = st.columns(2)
    with col1:
        csv_buf = io.StringIO()
        df.to_csv(csv_buf, index=False)
        st.download_button(
            "Download CSV", csv_buf.getvalue(),
            file_name=f"{label}.csv", mime="text/csv",
            key=f"{key_prefix}_csv",
        )
    with col2:
        excel_buf = make_excel_download({label: df})
        st.download_button(
            "Download Excel", excel_buf,
            file_name=f"{label}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"{key_prefix}_xlsx",
        )


# ============================================================================
# MAIN APPLICATION
# ============================================================================


def render_group_table(recon_df, sales_by_store, reasons, group_name, key):
    """Render a store-level adjustment table for a reason group."""
    store_agg, _, _ = aggregate_adjustments(recon_df, reasons)
    if store_agg.empty:
        st.caption(f"No {group_name.lower()} adjustments this period.")
        return
    merged = merge_with_sales(store_agg, sales_by_store, on_cols=["Store"])
    if not merged.empty:
        merged["_s"] = merged["Store"].map(store_sort_key)
        merged = merged.sort_values("_s").drop(columns="_s")
    net = merged["Net_Adjustment"].sum()
    count = int(merged["Adjustments"].sum())
    st.subheader(group_name)
    st.metric(f"Network Total", f"${net:,.2f} ({count} adjustments)")
    cols = ["Store", "Adjustments", "Net_Adjustment", "Store Sales COGS", "Shrinkage %"]
    avail = [c for c in cols if c in merged.columns]
    fmt = {"Net_Adjustment": "${:,.2f}", "Store Sales COGS": "${:,.2f}", "Shrinkage %": "{:.2%}"}
    styled = merged[avail].style.format(
        {k: v for k, v in fmt.items() if k in avail}, na_rep="N/A"
    )
    st.dataframe(styled, use_container_width=True, hide_index=True)
    download_buttons(merged[avail], f"{group_name.lower()}_by_store", key)


def compute_group_total(recon_df, reasons):
    """Quick sum of COGS for a reason group. Returns (net, count)."""
    filtered = recon_df[recon_df["Reason"].isin(reasons)] if not recon_df.empty else recon_df
    if filtered.empty:
        return 0, 0
    return filtered["COGS"].sum(), len(filtered)


def main():
    st.title(f"📉 Shrinkage Dashboard v{VERSION}")

    sheets_ok = has_sheets_config()

    # ----------------------------------------------------------------
    # Load persisted data
    # ----------------------------------------------------------------
    if sheets_ok:
        all_recon = load_recon_from_sheets()
        all_sales = load_sales_from_sheets()
    else:
        all_recon = st.session_state.get("recon_data", pd.DataFrame())
        all_sales = st.session_state.get("sales_data", pd.DataFrame())

    has_data = not all_recon.empty and not all_sales.empty

    # ----------------------------------------------------------------
    # Sidebar
    # ----------------------------------------------------------------
    st.sidebar.header("📊 Dashboard")

    # Period toggle
    period = st.sidebar.radio("View by", ["Weekly", "Monthly"], horizontal=True)
    period_key = "weekly" if period == "Weekly" else "monthly"

    # Period selector with human-readable labels
    selected_period = None
    periods_available = []
    prev_period = None
    if has_data:
        if "_date" not in all_recon.columns:
            all_recon["_date"] = pd.to_datetime(
                all_recon["Date"], format="mixed", errors="coerce"
            )
        recon_dates = all_recon["_date"].dropna()
        if not recon_dates.empty:
            if period_key == "weekly":
                periods_available = sorted(
                    recon_dates.apply(get_week_id).unique(), reverse=True
                )
            else:
                periods_available = sorted(
                    recon_dates.apply(get_month_id).unique(), reverse=True
                )
            if periods_available:
                selected_period = st.sidebar.selectbox(
                    "Period",
                    options=periods_available,
                    index=0,
                    format_func=lambda x: period_label(x, period_key),
                )
                # Get previous period for comparisons
                idx = periods_available.index(selected_period)
                if idx < len(periods_available) - 1:
                    prev_period = periods_available[idx + 1]

    st.sidebar.markdown("---")

    # Upload section
    st.sidebar.header("📂 Upload Data")
    if has_data:
        recon_weeks = sorted(all_recon.get("week_id", pd.Series()).dropna().unique())
        if recon_weeks:
            st.sidebar.success(
                f"{len(recon_weeks)} weeks loaded: "
                f"{week_id_to_label(recon_weeks[0])} to {week_id_to_label(recon_weeks[-1])}"
            )

    file_recon = st.sidebar.file_uploader(
        "Inventory Reconciliation History", type=["csv"], key="upload_recon",
        help="Blaze > Data Export > Inventory Reconciliation History",
    )
    file_sales = st.sidebar.file_uploader(
        "Total Sales Detail", type=["csv"], key="upload_sales",
        help="Blaze > Data Export > Total Sales Detail",
    )
    if file_recon and file_sales:
        if st.sidebar.button("Upload & Process", type="primary"):
            with st.spinner("Processing uploads..."):
                recon_upload = load_recon_csv(file_recon)
                sales_upload = load_sales_csv(file_sales)
                if recon_upload is not None and sales_upload is not None:
                    upload_weeks = set(recon_upload["week_id"].dropna().unique())
                    if sheets_ok:
                        existing = get_stored_week_ids()
                        dupes = upload_weeks & existing
                        if dupes:
                            st.sidebar.warning(
                                f"Skipping existing: {', '.join(week_id_to_label(w) for w in sorted(dupes))}"
                            )
                            recon_upload = recon_upload[~recon_upload["week_id"].isin(dupes)]
                            sales_upload = sales_upload[~sales_upload["week_id"].isin(dupes)]
                    if not recon_upload.empty:
                        if sheets_ok:
                            append_to_sheets(recon_upload, RECON_WORKSHEET)
                            append_to_sheets(sales_upload, SALES_WORKSHEET)
                            load_recon_from_sheets.clear()
                            load_sales_from_sheets.clear()
                        else:
                            prev_r = st.session_state.get("recon_data", pd.DataFrame())
                            prev_s = st.session_state.get("sales_data", pd.DataFrame())
                            st.session_state["recon_data"] = pd.concat([prev_r, recon_upload], ignore_index=True)
                            st.session_state["sales_data"] = pd.concat([prev_s, sales_upload], ignore_index=True)
                        st.sidebar.success(f"Uploaded {len(upload_weeks)} week(s)")
                        st.rerun()
                    else:
                        st.sidebar.info("No new data to upload.")

    st.sidebar.markdown("---")
    st.sidebar.caption(f"v{VERSION}")

    # ----------------------------------------------------------------
    # Reload after upload
    # ----------------------------------------------------------------
    if sheets_ok:
        all_recon = load_recon_from_sheets()
        all_sales = load_sales_from_sheets()
    else:
        all_recon = st.session_state.get("recon_data", pd.DataFrame())
        all_sales = st.session_state.get("sales_data", pd.DataFrame())
    has_data = not all_recon.empty and not all_sales.empty

    if not has_data:
        st.info("Upload Inventory Reconciliation History and Total Sales Detail CSVs in the sidebar to get started.")
        return

    if "_date" not in all_recon.columns:
        all_recon["_date"] = pd.to_datetime(all_recon["Date"], format="mixed", errors="coerce")
    if "Store" not in all_recon.columns:
        all_recon["Store"] = all_recon["Shop"].map(short_store_name)

    # ----------------------------------------------------------------
    # Filter to selected period
    # ----------------------------------------------------------------
    from datetime import date as date_cls

    def week_to_month(wid):
        if pd.isna(wid) or not isinstance(wid, str):
            return None
        try:
            d = datetime.strptime(wid, "%Y-%m-%d").date()
            return f"{d.year}-{d.month:02d}"
        except Exception:
            return None

    def get_period_data(recon, sales, pid, pkey):
        if not pid:
            return recon, sales
        if pkey == "weekly":
            return recon[recon["week_id"] == pid], sales[sales["week_id"] == pid]
        recon_c = recon.copy()
        recon_c["_month"] = recon_c["_date"].apply(lambda d: get_month_id(d) if pd.notna(d) else None)
        sales_c = sales.copy()
        sales_c["_month"] = sales_c["week_id"].apply(week_to_month)
        return (
            recon_c[recon_c["_month"] == pid],
            sales_c[sales_c["_month"] == pid].groupby(["Store", "Category"], as_index=False)["Sales COGS"].sum(),
        )

    period_recon, period_sales = get_period_data(all_recon, all_sales, selected_period, period_key)

    # Also get previous period for insights
    prev_recon, prev_sales = get_period_data(all_recon, all_sales, prev_period, period_key) if prev_period else (pd.DataFrame(), pd.DataFrame())

    # Sales by store (for merging)
    sales_by_store = period_sales.groupby("Store", as_index=False)["Sales COGS"].sum().rename(
        columns={"Sales COGS": "Store Sales COGS"}
    )

    # ----------------------------------------------------------------
    # Header
    # ----------------------------------------------------------------
    sel_label = period_label(selected_period, period_key) if selected_period else "All Data"
    st.markdown(f"### {sel_label}")

    # Compute all group totals for headlines
    shrinkage_reasons = get_reasons_for_report("Shrinkage")
    shrink_store, _, _ = aggregate_adjustments(period_recon, shrinkage_reasons)
    shrink_merged = merge_with_sales(shrink_store, sales_by_store, on_cols=["Store"])

    # Per-group totals
    group_totals = {}
    for gname, greasons in REASON_GROUPS.items():
        net, count = compute_group_total(period_recon, greasons)
        group_totals[gname] = {"net": net, "count": count}

    # Total across everything
    total_net = sum(g["net"] for g in group_totals.values())
    total_cogs = sales_by_store["Store Sales COGS"].sum() if not sales_by_store.empty else 0
    total_pct = total_net / total_cogs if total_cogs != 0 else None

    # Headline: total adjustments
    st.metric("Total Adjustments", f"{format_currency(total_net)} ({format_pct(total_pct)} of COGS)")

    # Sub-headlines: each group with % of COGS
    group_names = list(REASON_GROUPS.keys())
    cols = st.columns(len(group_names))
    for i, gname in enumerate(group_names):
        net = group_totals[gname]["net"]
        pct = net / total_cogs if total_cogs != 0 else None
        with cols[i]:
            st.metric(gname, format_currency(net))
            st.caption(format_pct(pct) + " of COGS" if pct is not None else "")

    # ----------------------------------------------------------------
    # Tabs
    # ----------------------------------------------------------------
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📈 Trends",
        "📊 Shrinkage by Location",
        "📦 Adjustments",
        "🔢 Incorrect Quantity",
        "👤 Employees",
        "📄 Raw Data",
    ])

    # == Tab 1: Trends ==
    with tab1:
        trend_data = build_period_trend(
            all_recon, all_sales, period=period_key,
            include_reasons=shrinkage_reasons,
        )
        build_network_trend(trend_data)
        build_store_trend(trend_data)

        st.markdown("---")
        st.subheader("All Adjustment Types Over Time")
        reason_trend = build_reason_trend(all_recon, period=period_key)
        build_reason_composition(reason_trend)

    # == Tab 2: Shrinkage by Location ==
    with tab2:
        st.caption(
            "Shrinkage = unexplained inventory variances (oversold + undersold). "
            "Excludes known adjustments like samples, display waste, and damaged goods."
        )
        if shrink_merged.empty:
            st.info("No shrinkage data for this period.")
        else:
            if not shrink_merged.empty:
                shrink_merged["_sort"] = shrink_merged["Store"].map(store_sort_key)
                shrink_merged = shrink_merged.sort_values("_sort").drop(columns="_sort")

            display_cols = [
                "Store", "Adjustments", "Gains", "Losses",
                "Net_Adjustment", "Store Sales COGS", "Shrinkage %",
            ]
            avail = [c for c in display_cols if c in shrink_merged.columns]
            display_df = shrink_merged[avail].copy()

            # Rename columns for readability
            col_rename = {
                "Gains": "Overages ($)",
                "Losses": "Shortages ($)",
                "Net_Adjustment": "Net ($)",
                "Store Sales COGS": "Sales COGS ($)",
                "Shrinkage %": "Rate",
            }
            display_df = display_df.rename(columns={k: v for k, v in col_rename.items() if k in display_df.columns})

            # Grand total
            totals = {"Store": "NETWORK TOTAL"}
            for c in display_df.columns:
                if c == "Store":
                    continue
                if c == "Rate":
                    net = display_df["Net ($)"].sum() if "Net ($)" in display_df.columns else 0
                    cogs = display_df["Sales COGS ($)"].sum() if "Sales COGS ($)" in display_df.columns else 0
                    totals[c] = net / cogs if cogs != 0 else None
                elif c == "Adjustments":
                    totals[c] = int(display_df[c].sum())
                else:
                    totals[c] = display_df[c].sum()

            display_with_total = pd.concat([display_df, pd.DataFrame([totals])], ignore_index=True)

            fmt = {
                "Overages ($)": "${:,.2f}",
                "Shortages ($)": "${:,.2f}",
                "Net ($)": "${:,.2f}",
                "Sales COGS ($)": "${:,.2f}",
                "Rate": "{:.2%}",
            }

            def color_rate(val):
                if pd.isna(val):
                    return ""
                if abs(val) > 0.05:
                    return "background-color: #ffcccc"
                if abs(val) > 0.02:
                    return "background-color: #fff3cd"
                return ""

            styled = display_with_total.style.format(
                {k: v for k, v in fmt.items() if k in display_with_total.columns}, na_rep="N/A"
            )
            if "Rate" in display_with_total.columns:
                styled = styled.map(color_rate, subset=["Rate"])
            st.dataframe(styled, use_container_width=True, hide_index=True)

            download_buttons(display_with_total, "shrinkage_by_location", "shrink")

    # == Tab 3: Adjustments ==
    with tab3:
        render_group_table(period_recon, sales_by_store, REASON_GROUPS["Samples"], "Samples", "samp")
        st.markdown("---")
        render_group_table(period_recon, sales_by_store, REASON_GROUPS["Display"], "Display", "disp")
        st.markdown("---")
        render_group_table(period_recon, sales_by_store, REASON_GROUPS["Damaged"], "Damaged", "dmg")
        st.markdown("---")
        render_group_table(period_recon, sales_by_store, REASON_GROUPS["Expired"], "Expired", "exp")
        st.markdown("---")

        # Other: headline + detail breakdown by individual reason
        st.subheader("Other")
        other_net, other_count = compute_group_total(period_recon, REASON_GROUPS["Other"])
        st.metric("Network Total", f"${other_net:,.2f} ({other_count} adjustments)")

        other_filtered = period_recon[period_recon["Reason"].isin(REASON_GROUPS["Other"])]
        if not other_filtered.empty:
            # Breakdown by individual reason across network
            reason_breakdown = (
                other_filtered.groupby("Reason")
                .agg(Adjustments=("COGS", "count"), Total=("COGS", "sum"))
                .sort_values("Total")
                .reset_index()
            )
            st.markdown("**By reason:**")
            fmt_rb = {"Total": "${:,.2f}"}
            st.dataframe(
                reason_breakdown.style.format(fmt_rb, na_rep="N/A"),
                use_container_width=True, hide_index=True,
            )

            # By store
            st.markdown("**By store:**")
            other_by_store = (
                other_filtered.groupby("Store")
                .agg(Adjustments=("COGS", "count"), Net_Adjustment=("COGS", "sum"))
                .reset_index()
            )
            other_by_store = merge_with_sales(other_by_store, sales_by_store, on_cols=["Store"])
            if not other_by_store.empty:
                other_by_store["_s"] = other_by_store["Store"].map(store_sort_key)
                other_by_store = other_by_store.sort_values("_s").drop(columns="_s")
            cols = ["Store", "Adjustments", "Net_Adjustment", "Store Sales COGS", "Shrinkage %"]
            avail = [c for c in cols if c in other_by_store.columns]
            fmt_o = {"Net_Adjustment": "${:,.2f}", "Store Sales COGS": "${:,.2f}", "Shrinkage %": "{:.2%}"}
            st.dataframe(
                other_by_store[avail].style.format({k: v for k, v in fmt_o.items() if k in avail}, na_rep="N/A"),
                use_container_width=True, hide_index=True,
            )
            download_buttons(other_by_store[avail], "other_by_store", "oth")

    # == Tab 4: Incorrect Quantity ==
    with tab4:
        iq_reasons = REASON_GROUPS["Incorrect Qty"]
        iq_filtered = period_recon[period_recon["Reason"].isin(iq_reasons)] if not period_recon.empty else period_recon

        if iq_filtered.empty:
            st.info("No incorrect quantity adjustments this period.")
        else:
            iq_net = iq_filtered["COGS"].sum()
            iq_count = len(iq_filtered)
            st.metric("Network Total", f"${iq_net:,.2f} ({iq_count} adjustments)")

            # Summary by store
            st.subheader("By Store")
            iq_store = (
                iq_filtered.groupby("Store")
                .agg(Adjustments=("COGS", "count"), Net_Adjustment=("COGS", "sum"))
                .reset_index()
            )
            iq_store_merged = merge_with_sales(iq_store, sales_by_store, on_cols=["Store"])
            if not iq_store_merged.empty:
                iq_store_merged["_s"] = iq_store_merged["Store"].map(store_sort_key)
                iq_store_merged = iq_store_merged.sort_values("_s").drop(columns="_s")
            cols_iq = ["Store", "Adjustments", "Net_Adjustment", "Store Sales COGS", "Shrinkage %"]
            avail_iq = [c for c in cols_iq if c in iq_store_merged.columns]
            fmt_iq = {"Net_Adjustment": "${:,.2f}", "Store Sales COGS": "${:,.2f}", "Shrinkage %": "{:.2%}"}
            st.dataframe(
                iq_store_merged[avail_iq].style.format(
                    {k: v for k, v in fmt_iq.items() if k in avail_iq}, na_rep="N/A"
                ),
                use_container_width=True, hide_index=True,
            )

            # Detail view
            st.subheader("Detail")
            detail_cols = [
                "Date", "Store", "Employee Name", "Product Name", "Category Name",
                "Difference", "Cost per Unit", "COGS", "Reason Note",
            ]
            avail_detail = [c for c in detail_cols if c in iq_filtered.columns]
            iq_detail = iq_filtered[avail_detail].copy()
            if not iq_detail.empty:
                iq_detail["_s"] = iq_filtered["Store"].map(store_sort_key)
                iq_detail = iq_detail.sort_values(["_s", "Date"]).drop(columns="_s")
            st.dataframe(iq_detail, use_container_width=True, hide_index=True)
            st.caption(f"{len(iq_detail)} adjustments")
            download_buttons(iq_detail, "incorrect_quantity_detail", "iq")

    # == Tab 5: Employees ==
    with tab5:
        _, _, emp_detail = aggregate_adjustments(period_recon, shrinkage_reasons)
        if emp_detail.empty:
            st.info("No employee shrinkage data for this period.")
        else:
            emp_with_cogs = emp_detail.merge(sales_by_store, on="Store", how="left")
            emp_with_cogs["% of Store COGS"] = emp_with_cogs.apply(
                lambda r: r["Net_Adjustment"] / r["Store Sales COGS"]
                if pd.notna(r.get("Store Sales COGS")) and r.get("Store Sales COGS", 0) != 0
                else None, axis=1,
            )
            emp_with_cogs["_s"] = emp_with_cogs["Store"].map(store_sort_key)
            emp_with_cogs = emp_with_cogs.sort_values(["_s", "Net_Adjustment"]).drop(columns="_s")

            stores_emp = sorted(emp_with_cogs["Store"].unique(), key=store_sort_key)
            selected_emp_stores = st.multiselect(
                "Filter by location:", options=stores_emp, default=stores_emp, key="emp_store_filter",
            )
            filtered_emp = emp_with_cogs[emp_with_cogs["Store"].isin(selected_emp_stores)]

            display_cols = ["Store", "Employee Name", "Adjustments", "Gains", "Losses", "Net_Adjustment", "% of Store COGS"]
            avail = [c for c in display_cols if c in filtered_emp.columns]
            fmt_emp = {"Gains": "${:,.2f}", "Losses": "${:,.2f}", "Net_Adjustment": "${:,.2f}", "% of Store COGS": "{:.2%}"}
            styled_emp = filtered_emp[avail].style.format(
                {k: v for k, v in fmt_emp.items() if k in avail}, na_rep="N/A"
            )
            st.dataframe(styled_emp, use_container_width=True, hide_index=True)
            st.caption(f"{len(filtered_emp)} employees")
            download_buttons(filtered_emp[avail], "employee_shrinkage", "emp")

    # == Tab 6: Raw Data ==
    with tab6:
        raw = period_recon.copy()
        col_f1, col_f2, col_f3 = st.columns(3)
        with col_f1:
            shop_filter = st.multiselect(
                "Store:", options=sorted(raw["Store"].dropna().unique(), key=store_sort_key), key="raw_shop",
            )
        with col_f2:
            reason_filter = st.multiselect(
                "Reason:", options=sorted(raw["Reason"].dropna().unique()), key="raw_reason",
            )
        with col_f3:
            cat_filter = st.multiselect(
                "Category:", options=sorted(raw["Category Name"].dropna().unique()), key="raw_cat",
            )
        if shop_filter:
            raw = raw[raw["Store"].isin(shop_filter)]
        if reason_filter:
            raw = raw[raw["Reason"].isin(reason_filter)]
        if cat_filter:
            raw = raw[raw["Category Name"].isin(cat_filter)]

        display_raw_cols = [
            "Date", "Store", "Employee Name", "Product Name", "Category Name",
            "Difference", "Cost per Unit", "COGS", "Reason", "Reason Note",
        ]
        avail_raw = [c for c in display_raw_cols if c in raw.columns]
        raw_display = raw[avail_raw].reset_index(drop=True)
        st.dataframe(raw_display, use_container_width=True, hide_index=True)
        st.caption(f"{len(raw_display)} rows")
        download_buttons(raw_display, "raw_data", "raw")


if __name__ == "__main__":
    main()
