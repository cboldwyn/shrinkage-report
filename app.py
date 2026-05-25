"""
Shrinkage Dashboard
Persistent shrinkage dashboard for Haven retail locations.

Tracks inventory adjustment costs against sales COGS with weekly/monthly
trend analysis. Data persists in Google Sheets, refreshed weekly via CSV upload.

Report types filter by reason groupings:
- Shrinkage (default): OVERSOLD + UNDERSOLD only
- All Adjustments: every reason, grouped by category
- Samples, Display, Damaged, Expired, Other: individual groups

CHANGELOG:
v2.6.7 (2026-05-25)
- Per-Store Homework drill table: cart-membership ✓ column removed. The
  "already in cart" count above the table is the cart-membership signal;
  the leftmost native streamlit selection checkboxes are the only
  checkmarks in the table.
v2.6.6 (2026-05-25)
- Reason Code Audit tab restructured. Long-format table + Min |COGS|
  filter removed. Cross-tab now supports multi-row + multi-column
  selection (cmd/shift-click) and feeds a transaction viewer below.
  Viewer scope is the union of selected (Shop, Category) rows AND
  selected Reason columns; selecting only rows scopes to all reasons
  for those scopes, selecting only columns scopes to all stores+cats
  for those reasons.
- Em-dash purge across the entire file (comments, docstrings, strings,
  zero-cell renderers, na_rep). Project voice rule now applies
  everywhere in the codebase.
- Date Timestamp fallback moved up to the all_recon load layer, so
  both the Reason Code Audit tab and the Per-Store Homework tab see
  the date column consistently.
v2.6.5 (2026-05-25)
- Drill table: fake select-all visual ✓ removed. Streamlit's st.dataframe
  multi-row checkboxes are user-click-only (no programmatic API), so we
  don't fake them. The Select-all-visible checkbox below the count line
  still switches the Add button to "Add all visible". that's the
  workaround for bulk-add until Streamlit ships programmatic selection.
- Column display: "Date Timestamp" renamed to "Date" across drill / cart
  / homework export via DRILL_DISPLAY_LABELS. Underlying CSV column name
  preserved.
- Date Timestamp fallback moved to period_recon_wb level: drill / cart /
  export all share the same source. Pre-v2.6.0 uploads that have only
  "Date" will see it aliased automatically.
- Google Sheet creation now uses Drive API directly with
  supportsAllDrives=true (works for Shared Drives too, not just My Drive).
- Drive folder config now visible inline (truncated ID) so it's obvious
  whether the secret is being read. On error, a Troubleshooting expander
  shows the resolved folder ID + service account email + common fixes.
v2.6.4 (2026-05-25)
- Homework export now contains ONLY the Explanations Needed tab. The
  Shrinkage summary tab is dropped (the combined Adjustment Breakdown
  on the page is the summary surface; the GM packet is transaction-level).
- Date Timestamp fallback: if persisted data was uploaded before v2.6.0
  and is missing Date Timestamp, the export falls back to the Date column
  so the GM always has a date column.
- Select all visible: ticking the checkbox now visibly fills the ✓ column
  on every visible row, so the user can see the selection is active.
- Google Sheet creation: supports a per-app Drive folder. When the
  service account hits its zero-byte storage quota (403), the app now
  surfaces inline setup steps with the service account email and the
  exact secrets line to add. Create-as-Google-Sheet button is disabled
  until at least one transaction is in the cart.
v2.6.3 (2026-05-25)
- Per-Store Adjustment Breakdown: multi-select (multi-row + multi-column) on
  the cross-tab. Cmd / shift-click to extend selection. Filter below now
  mirrors the cross-tab selection exactly (replaces, no more orphan filter
  entries left behind when the cross-tab visual deselects). Manual edits to
  the bottom multiselect filters still persist between cross-tab clicks.
- Per-Store Adjustment Breakdown column order: Category, %, Grand Total,
  Reasons (DNU first then Unknown then Approved), Sales COGS.
- New: "Create as Google Sheet" button next to the xlsx download. Creates a
  live Google Sheet (same two tabs, same formatting) and auto-shares with
  Charles. Lisa can then share the sheet link with the GM, who types
  explanations directly into the GM Explanation column. Last-created sheet
  URL is shown until a new one is created.
v2.6.2 (2026-05-25)
- Per-Store Homework: Shrinkage summary and Adjustment Breakdown merged into
  one table. Sales COGS and shrinkage % columns appended to the right of the
  cross-tab. Red highlight on % when the absolute breach exceeds 2. Click
  behavior preserved (row scopes by Category, column by Reason, both by cell).
- Reason code display: INCORRECT_QUANTITY shows as INCORRECT_QTY across the
  Reason Code Audit and Per-Store Homework cross-tabs to save column width.
- Per-Store Homework: Select-all-visible checkbox above the drill table.
  When ticked, the Add button adds every row in the current filtered view
  instead of just the rows the user clicked.
- Per-Store Homework: em dashes removed from on-page captions, subheaders,
  status lines, and zero-cell renderer to match Charles's voice rule.
- Homework workbook export: $ formatting on COGS / Cost per Unit / Sales COGS,
  % formatting on the rate column. Quantity columns get comma thousands.
  No more raw decimals in Lisa's GM packet.
v2.6.1 (2026-05-19)
- Reason Code Audit: cross-tab cells are now drillable. Click a (Shop, Category)
  row + a Reason column on the top cross-tab to open the underlying transactions
  panel below. Falls back to long-format row-pick when the cross-tab has no
  full (row+column) selection.
- Per-Store Homework: zero-shrink categories now appear in the Shrinkage summary
  as 0/0 rows so the Total reconciles cleanly to per-store Sales COGS.
- Per-Store Homework: Category multi-select now preserves the user's picks when
  the Adjustment Breakdown deselects. Cross-tab selection now ADDs to the filter
  set rather than replacing it. Same fix for the Reason filter.
- Per-Store Homework: Lisa drill column order locked left-to-right by priority:
  Date Timestamp / Store / Employee / Product / Difference / COGS / Reason /
  Reason Note / Batch / Metrc Adjustment / Cost per Unit.
- "Batch SKU" column now displays as "Blaze Batch" in all drill / cart / homework
  views and the downloaded workbook. (Underlying CSV column name unchanged.)
- Per-store Homework Excel download is always available once a store is picked,
  even with an empty cart. Workbook always has both Shrinkage and Explanations
  needed tabs (empty Shrinkage tab still has the category-header schema).
v2.6.0 (2026-05-14)
- New tab: ✅ Compliance Audit. Network-wide reason-code + dollar-amount scan
  matching the `Pivot Table2` layout from Lisa's monthly template. Renders
  BOTH a cross-tab (Shop × Category in rows, Reason in columns, SUM of COGS
  in cells; DNU columns first and colored red) AND a long-format
  sortable/filterable list (Compliance | Shop | Category | Reason |
  Adjustments | SUM of COGS) below. Filters: Store, Compliance status,
  Min |COGS|. Read-only. no transaction drill on this tab.
- New tab: 📝 Per-Store Homework. Replicates Lisa's per-store workbook flow
  end-to-end: pick a store → see her Shrinkage summary (per-Category
  OVERSOLD/UNDERSOLD/TAC/COGS/% + Store Total row) → see Pivot Table 3
  (Category × Reason long-format) → click a Cat-Reason row → drill panel
  shows underlying transactions → multi-row select + "➕ Add to Explanations
  Needed" button → cart shows flagged with view/remove/clear → download
  per-store Excel workbook with 'Shrinkage' and 'Explanations needed' sheets
  + GM Explanation column. Cart is single-store; switching stores clears
  the cart (download first to keep it). Uses st.dataframe row-select
  (not data_editor) for sticky-state-free flagging.
- DDE Adjustments tab: per-store table broken out by Display / Defective / Expired
  with sub-group totals as inline metrics.
- Fix: zero-shrink stores no longer drop from network % denominator. Tab 2 grand
  total Rate + trends per-period rate were inflating when stores had zero shrinkage.
- Recon column set expanded: Date Timestamp, Old Quantity, New Quantity, Batch SKU,
  Metrc Adjustment, Reconciliation No now persisted (re-upload to populate).
- Lisa drill display drops standalone Date column; Date Timestamp only.
v2.5.2 (2026-04-15)
- Legacy Shrink COGS denominator fix (uses full store sales)
v2.5.1, v2.5.0 (2026-04-15)
- Legacy Shrink tab with nested store expanders matching Georgina's old pivot
v2.4.x (2026-04-08 to 04-14)
- Headlines, reason mapping, DDE group, IQ separate, KeyError fixes
v2.3.0 (2026-04-07)
- Sun-Sat business weeks, Incorrect Qty tab, label fixes
v2.2.0, v2.1.0 (2026-04-04 to 04-06)
- UX overhaul for readability and actionable insights
v2.0.0 (2026-04-14)
- Google Sheets persistence, report presets, plotly trends
v1.0.0 (2026-03-31)
- Initial release
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import io
from datetime import datetime, timedelta

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

VERSION = "2.6.7"

# Email of the human owner of this app. used to auto-share newly-created
# homework Google Sheets so Charles can see them in his Drive.
USER_EMAIL = "charles@myhavenstores.com"

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
    "Date", "Date Timestamp", "Shop", "Store", "Reconciliation No",
    "Employee Name", "Category Name",
    "Inventory Name", "Product Name", "Brand Name", "Batch SKU",
    "Old Quantity", "New Quantity", "Difference", "Metrc Adjustment",
    "Cost per Unit", "COGS", "Reason", "Reason Note",
]
SALES_REQUIRED_COLS = ["Date", "Shop", "Product Category", "COGS"]

# Lisa's drill-down column subset. Keep order. Lisa scans left to right.
# Order locked 5/19 PM (DC/Retail Touch Base): the things she needs first are on the left;
# Batch / Metrc Adjustment / Cost per Unit pushed right of Difference/COGS/Reason/Reason Note.
LISA_DRILL_COLS = [
    "Date Timestamp", "Store", "Employee Name", "Product Name",
    "Difference", "COGS", "Reason", "Reason Note",
    "Batch SKU", "Metrc Adjustment", "Cost per Unit",
]

# Display labels. keep underlying CSV column names, only rename for display.
# - "Batch SKU" → "Blaze Batch": the column is the per-batch UID Lisa works
#   with; the CSV label is ambiguous.
# - "Date Timestamp" → "Date": the underlying CSV column is named
#   "Date Timestamp" but in drill / cart / export Lisa just wants "Date".
DRILL_DISPLAY_LABELS = {
    "Batch SKU": "Blaze Batch",
    "Date Timestamp": "Date",
}

# Display labels for reason codes. shortens to fit the combined Per-Store
# Adjustment Breakdown table without wrapping. Underlying constants and CSV
# values are untouched (filters / classification all still key on the raw code).
REASON_DISPLAY_LABELS = {
    "INCORRECT_QUANTITY": "INCORRECT_QTY",
}

# -- Reason system --
ALL_REASONS = [
    "OVERSOLD", "UNDERSOLD", "DAMAGED", "WASTE_DISPLAY", "DISPLAY_SAMPLE",
    "SAMPLES", "WASTE_EXPIRED", "WASTE_RETURN", "WASTE_DISPOSAL",
    "AUDIT", "INCORRECT_QUANTITY", "OTHER",
    "PUBLIC_SAFETY_RECALL", "MANDATED_DESTRUCTION", "RETURN_TO_VENDOR",
]

# Parent groups per reason mapping.
# "Not Billed" = costs Haven bears. "Billed" = recovered from vendor.
REASON_GROUPS = {
    # Not Billed (Haven's cost)
    "Shrinkage":      ["OVERSOLD", "UNDERSOLD"],
    "Incorrect Qty":  ["INCORRECT_QUANTITY"],
    "Audit":          ["AUDIT"],
    "Samples":        ["SAMPLES"],
    "Other":          ["OTHER"],
    # Billed to Vendor
    "DDE":            ["WASTE_DISPLAY", "DISPLAY_SAMPLE", "WASTE_RETURN", "DAMAGED",
                       "WASTE_DISPOSAL", "WASTE_EXPIRED"],
    "Recall":         ["PUBLIC_SAFETY_RECALL", "MANDATED_DESTRUCTION", "RETURN_TO_VENDOR"],
}

NOT_BILLED_GROUPS = ["Shrinkage", "Incorrect Qty", "Audit", "Samples", "Other"]
BILLED_GROUPS = ["DDE", "Recall"]

# Approved vs DNU Blaze Reasons per Reconciliation Reasons doc (11/4/25 update).
# Approved = on the active scenario matrix. DNU = available in Blaze but explicitly off-limits.
# Stores using DNU reasons get flagged for GM follow-up.
APPROVED_REASONS = {
    "OVERSOLD", "UNDERSOLD", "DAMAGED",
    "WASTE_DISPLAY", "DISPLAY_SAMPLE", "SAMPLES",
    "WASTE_EXPIRED", "WASTE_RETURN", "WASTE_DISPOSAL",
    "INCORRECT_QUANTITY", "OTHER",
}
DNU_REASONS = {
    "AUDIT", "PUBLIC_SAFETY_RECALL", "MANDATED_DESTRUCTION", "RETURN_TO_VENDOR",
    "FREE_CANNABIS_GOODS", "SCALE_VARIANCE", "THEFT", "VOLUNTARY_SURRENDER",
    "STORE_TRANSFER", "PO_ERROR",
}

# DDE sub-groups for detail breakdown
DDE_SUBGROUPS = {
    "Display":   ["WASTE_DISPLAY", "DISPLAY_SAMPLE"],
    "Defective": ["WASTE_RETURN", "DAMAGED", "WASTE_DISPOSAL"],
    "Expired":   ["WASTE_EXPIRED"],
}

# -- Haven branding --
COLOR_PRIMARY = "#3DC0CC"
COLOR_ACCENT = "#FFCA45"
COLOR_ALERT = "#9E1F63"

GROUP_COLORS = {
    "Shrinkage": COLOR_PRIMARY,
    "Samples": "#8E44AD",
    "DDE": COLOR_ACCENT,
    "Recall": "#2C3E50",
    "Other": "#95A5A6",
    "Display": COLOR_ACCENT,
    "Defective": COLOR_ALERT,
    "Expired": "#E67E22",
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
    elif report_name in REASON_GROUPS:
        return list(REASON_GROUPS[report_name])
    elif report_name == "All Adjustments":
        groups = list(REASON_GROUPS.keys())
    else:
        groups = ["Shrinkage"]
    reasons = []
    for g in groups:
        reasons.extend(REASON_GROUPS.get(g, []))
    return reasons


def _apply_drill_labels(df):
    """Rename drill columns for user-facing display (e.g., Batch SKU → Blaze Batch).
    Keeps the underlying column name in the source dataframe; only renames a copy."""
    rename_map = {k: v for k, v in DRILL_DISPLAY_LABELS.items() if k in df.columns}
    if not rename_map:
        return df
    return df.rename(columns=rename_map)


def make_excel_download(dataframes_dict):
    """Create an Excel file with multiple sheets from {name: df}."""
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        for sheet_name, df in dataframes_dict.items():
            df.to_excel(writer, index=False, sheet_name=sheet_name[:31])
    buf.seek(0)
    return buf


def _col_letter(n):
    """Convert 1-indexed column number to Excel/Sheets letter (1=A, 27=AA)."""
    result = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result


def _homework_drive_folder_id():
    """Drive folder ID for newly-created homework sheets, or None if unset.

    Configured via st.secrets["homework_drive_folder_id"] (top level) or
    st.secrets["google_sheets"]["homework_folder_id"] (nested). The folder
    must be owned by a human user and shared with the service account as
    Editor; the new sheet lives in that user's Drive (bypassing the
    service account's zero-byte storage quota).
    """
    try:
        top_level = st.secrets.get("homework_drive_folder_id")
        if top_level:
            return str(top_level)
    except Exception:
        pass
    try:
        gs_block = st.secrets.get("google_sheets", {})
        if isinstance(gs_block, dict):
            nested = gs_block.get("homework_folder_id")
        else:
            nested = gs_block["homework_folder_id"] if "homework_folder_id" in gs_block else None
        if nested:
            return str(nested)
    except Exception:
        pass
    return None


def _service_account_email():
    """Return the service account's client_email from secrets, or None."""
    try:
        gs_block = st.secrets.get("google_sheets", {})
        if isinstance(gs_block, dict):
            return gs_block.get("client_email")
        return gs_block["client_email"] if "client_email" in gs_block else None
    except Exception:
        return None


def make_homework_gsheet(sheets_dict, title, share_with=None, folder_id=None):
    """Upload the homework workbook to a new Google Sheet and return the URL.

    sheets_dict: {sheet_name: DataFrame}. same shape as the xlsx writer.
    title: Sheet title (will get a timestamp suffix).
    share_with: list of email addresses to grant writer access to (notify=False).
    folder_id: Drive folder ID to create the sheet inside. Folder must be
        shared with the service account as Editor. When provided, the new
        sheet is owned by the folder's owner (avoids service-account quota).

    Returns the spreadsheet URL string. Raises if Google Sheets isn't configured.
    """
    if not has_sheets_config():
        raise RuntimeError("Google Sheets is not configured for this app.")

    gc = get_gspread_client()
    ts = datetime.now().strftime("%Y-%m-%d %H%M")
    full_title = f"{title} ({ts})"

    # Create via the Drive API directly so we can pass supportsAllDrives=True
    # (gspread's Client.create doesn't, which breaks creation inside Shared
    # Drives). This also ensures the file lands in the folder owner's Drive
    # so the service account's zero-byte quota is irrelevant.
    if folder_id:
        payload = {
            "name": full_title,
            "mimeType": "application/vnd.google-apps.spreadsheet",
            "parents": [folder_id],
        }
        try:
            resp = gc.http_client.request(
                "post",
                "https://www.googleapis.com/drive/v3/files",
                json=payload,
                params={"supportsAllDrives": "true"},
            )
            spreadsheet_id = resp.json()["id"]
            sh = gc.open_by_key(spreadsheet_id)
        except Exception:
            # Fallback to gspread's stock create() if http_client path fails
            sh = gc.create(full_title, folder_id=folder_id)
    else:
        sh = gc.create(full_title)

    # Share with the requested emails so users can open + edit. Service account
    # remains the owner (counts against its Drive quota).
    for email in (share_with or []):
        try:
            sh.share(email, perm_type="user", role="writer", notify=False)
        except Exception:
            pass  # share failures are non-fatal; URL still works for service account

    DOLLAR_FMT = {"numberFormat": {"type": "CURRENCY", "pattern": "$#,##0.00"}}
    PCT_FMT = {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}
    QTY_FMT = {"numberFormat": {"type": "NUMBER", "pattern": "#,##0.00"}}
    HEADER_FMT = {"textFormat": {"bold": True}, "horizontalAlignment": "LEFT"}

    default_ws = sh.sheet1
    sheet_names = list(sheets_dict.keys())

    for idx, sheet_name in enumerate(sheet_names):
        df = sheets_dict[sheet_name]
        rows_needed = max(len(df) + 5, 20)
        cols_needed = max(len(df.columns) + 1, 10)
        if idx == 0:
            default_ws.update_title(sheet_name[:100])
            ws = default_ws
            ws.resize(rows=rows_needed, cols=cols_needed)
        else:
            ws = sh.add_worksheet(title=sheet_name[:100], rows=rows_needed, cols=cols_needed)

        # Build value matrix: headers + rows. Convert NaN to empty string;
        # keep numbers as floats so the numberFormat applies.
        header_row = [str(c) for c in df.columns]
        data_rows = []
        for _, row in df.iterrows():
            row_vals = []
            for v in row:
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    row_vals.append("")
                elif isinstance(v, (int, float)):
                    row_vals.append(float(v))
                else:
                    row_vals.append(str(v))
            data_rows.append(row_vals)
        values = [header_row] + data_rows
        ws.update(values=values, range_name="A1", value_input_option="USER_ENTERED")

        last_col = _col_letter(len(df.columns))
        last_row = len(df) + 1  # +1 for header row

        # Header formatting
        ws.format(f"A1:{last_col}1", HEADER_FMT)

        # Per-sheet number formats
        if sheet_name == "Shrinkage":
            # B-E currency (OVERSOLD / UNDERSOLD / TAC / Sales COGS); F percent
            for col_idx, h in enumerate(df.columns, start=1):
                letter = _col_letter(col_idx)
                if h in ("SUM of OVERSOLD", "SUM of UNDERSOLD",
                         "SUM of TRUE AUDIT COST", "SUM of COGS"):
                    ws.format(f"{letter}2:{letter}{last_row}", DOLLAR_FMT)
                elif h == "%":
                    ws.format(f"{letter}2:{letter}{last_row}", PCT_FMT)
        elif sheet_name == "Explanations needed":
            for col_idx, h in enumerate(df.columns, start=1):
                letter = _col_letter(col_idx)
                if h in ("COGS", "Cost per Unit"):
                    ws.format(f"{letter}2:{letter}{last_row}", DOLLAR_FMT)
                elif h == "Difference":
                    ws.format(f"{letter}2:{letter}{last_row}", QTY_FMT)

    return sh.url


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

    # Right-merge from sales so every (period, store) with sales is preserved, even when
    # that store had zero adjustments in the period. Otherwise zero-shrink stores drop from
    # the denominator and network/period rates inflate.
    merged = sales_agg.merge(adj, on=["period_id", "Store"], how="left")
    merged["Net_Adjustment"] = merged["Net_Adjustment"].fillna(0)
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


def filter_partial_weeks(trend_data):
    """Drop weeks with less than 50% of median sales COGS (partial data)."""
    if trend_data.empty:
        return trend_data
    weekly_cogs = trend_data.groupby("period_id")["Sales COGS"].sum()
    median_cogs = weekly_cogs.median()
    if median_cogs <= 0:
        return trend_data
    full_weeks = weekly_cogs[weekly_cogs >= median_cogs * 0.5].index
    return trend_data[trend_data["period_id"].isin(full_weeks)]


def build_network_trend(trend_data):
    """Network-level shrinkage rate over time (higher = worse)."""
    if trend_data.empty:
        st.info("Not enough data for trend charts. Upload more weeks.")
        return

    data = filter_partial_weeks(trend_data)
    network = (
        data.groupby("period_id", as_index=False)
        .agg({"Net_Adjustment": "sum", "Sales COGS": "sum"})
    )
    # Show as positive rate (higher = more shrinkage = worse)
    network["Shrinkage Rate"] = network.apply(
        lambda r: abs(r["Net_Adjustment"]) / r["Sales COGS"]
        if r["Sales COGS"] != 0 and r["Net_Adjustment"] < 0 else
        -abs(r["Net_Adjustment"]) / r["Sales COGS"]
        if r["Sales COGS"] != 0 else None, axis=1
    )
    network = network.dropna(subset=["Shrinkage Rate"]).sort_values("period_id")
    if network.empty:
        return

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=network["period_id"], y=network["Shrinkage Rate"],
        mode="lines+markers", name="Shrinkage Rate",
        line=dict(color=COLOR_ALERT, width=2),
        hovertemplate="%{x}: %{y:.2%}<extra></extra>",
    ))
    if len(network) >= 4:
        rolling = network["Shrinkage Rate"].rolling(4, min_periods=4).mean()
        fig.add_trace(go.Scatter(
            x=network["period_id"], y=rolling,
            mode="lines", name="4-period avg",
            line=dict(dash="dash", width=1, color=COLOR_ALERT),
            opacity=0.5, showlegend=True,
        ))
    fig.add_hline(y=0, line_dash="dot", line_color="gray", opacity=0.5)
    apply_period_labels(fig, network["period_id"])
    fig.update_layout(
        title="Network Shrinkage Rate (higher = worse)",
        height=400, yaxis_tickformat=".2%",
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=-0.25),
    )
    st.plotly_chart(fig, use_container_width=True)


def build_store_trend(trend_data):
    """Per-store shrinkage rate over time (higher = worse)."""
    if trend_data.empty:
        return
    data = filter_partial_weeks(trend_data)
    data = data.dropna(subset=["Shrinkage %"]).copy()
    if data.empty:
        return
    # Invert: positive = loss (worse)
    data["Shrinkage Rate"] = data["Shrinkage %"].apply(
        lambda v: abs(v) if v < 0 else -abs(v)
    )
    data["_sort"] = data["Store"].map(store_sort_key)
    data = data.sort_values(["_sort", "period_id"])

    fig = px.line(
        data, x="period_id", y="Shrinkage Rate", color="Store",
        markers=True,
        labels={"period_id": "Period", "Shrinkage Rate": "Shrinkage Rate"},
    )
    fig.add_hline(y=0, line_dash="dot", line_color="gray", opacity=0.5)
    apply_period_labels(fig, data["period_id"])
    fig.update_layout(
        title="Shrinkage Rate by Store (higher = worse)",
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
    # Date Timestamp fallback: pre-v2.6.0 uploads only have "Date". Alias once
    # at the top so every downstream tab (Reason Code Audit, Per-Store Homework,
    # cart, export) sees a consistent column.
    if "Date Timestamp" not in all_recon.columns and "Date" in all_recon.columns:
        all_recon["Date Timestamp"] = all_recon["Date"]

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

    total_cogs = sales_by_store["Store Sales COGS"].sum() if not sales_by_store.empty else 0

    # Haven's cost = not billed groups only
    haven_cost = sum(group_totals[g]["net"] for g in NOT_BILLED_GROUPS if g in group_totals)
    haven_pct = haven_cost / total_cogs if total_cogs != 0 else None

    # Billed to vendor
    billed_total = sum(group_totals[g]["net"] for g in BILLED_GROUPS if g in group_totals)

    # Headlines: Haven's cost (what actually costs us)
    st.markdown("**Haven's Cost** (not recovered from vendors)")
    h_cols = st.columns(len(NOT_BILLED_GROUPS) + 1)
    with h_cols[0]:
        st.metric("Total", f"{format_currency(haven_cost)}")
        st.caption(f"{format_pct(haven_pct)} of COGS")
    for i, gname in enumerate(NOT_BILLED_GROUPS):
        net = group_totals.get(gname, {}).get("net", 0)
        pct = net / total_cogs if total_cogs != 0 else None
        with h_cols[i + 1]:
            st.metric(gname, format_currency(net))
            st.caption(format_pct(pct) + " of COGS" if pct is not None else "")

    # Billed to vendor (separate, less alarming)
    st.markdown("**Billed to Vendor** (recovered)")
    b_cols = st.columns(len(BILLED_GROUPS) + 1)
    with b_cols[0]:
        st.metric("Total", format_currency(billed_total))
    for i, gname in enumerate(BILLED_GROUPS):
        net = group_totals.get(gname, {}).get("net", 0)
        with b_cols[i + 1]:
            st.metric(gname, format_currency(net))

    # ----------------------------------------------------------------
    # Tabs
    # ----------------------------------------------------------------
    tab1, tab2, tab_compliance, tab_workbook, tab3, tab4, tab5, tab6, tab7 = st.tabs([
        "📈 Trends",
        "📊 Shrinkage by Location",
        "✅ Reason Code Audit",
        "📝 Per-Store Homework",
        "📋 Legacy Shrink",
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

            # Grand total. use full network COGS across all stores (not just stores with
            # shrinkage in display_df) to keep zero-shrink stores in the denominator.
            full_network_cogs = sales_by_store["Store Sales COGS"].sum() if not sales_by_store.empty else 0
            totals = {"Store": "NETWORK TOTAL"}
            for c in display_df.columns:
                if c == "Store":
                    continue
                if c == "Rate":
                    net = display_df["Net ($)"].sum() if "Net ($)" in display_df.columns else 0
                    totals[c] = net / full_network_cogs if full_network_cogs != 0 else None
                elif c == "Sales COGS ($)":
                    totals[c] = full_network_cogs
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

            # Breakdown by individual reason within Shrinkage
            shrink_filtered = period_recon[period_recon["Reason"].isin(REASON_GROUPS["Shrinkage"])]
            if not shrink_filtered.empty:
                st.subheader("By Reason")
                reason_breakdown = (
                    shrink_filtered.groupby("Reason")
                    .agg(Adjustments=("COGS", "count"), Total=("COGS", "sum"))
                    .sort_values("Total")
                    .reset_index()
                )
                fmt_rb = {"Total": "${:,.2f}"}
                st.dataframe(
                    reason_breakdown.style.format(fmt_rb, na_rep="N/A"),
                    use_container_width=True, hide_index=True,
                )

    # == Tab Compliance Audit ==
    with tab_compliance:
        st.caption(
            "Network-wide reason-code compliance and dollar-amount audit. Matches the "
            "`Pivot Table2` layout in Lisa's monthly template (Shop × Category in rows, "
            "Reason in columns, SUM of COGS in cells). Click rows and columns on the "
            "cross-tab to scope the transaction viewer below. Multi-select with "
            "cmd/shift-click to drill across multiple cells at once."
        )

        if period_recon.empty:
            st.info("No reconciliation data for this period.")
        else:
            fc1, fc2 = st.columns(2)
            with fc1:
                ca_stores = st.multiselect(
                    "Store",
                    options=sorted(period_recon["Store"].dropna().unique(), key=store_sort_key),
                    key="ca_stores",
                )
            with fc2:
                ca_compliance = st.multiselect(
                    "Compliance",
                    options=["✅ Approved", "🚫 DNU", "⚠️ Unknown"],
                    key="ca_compliance",
                )

            ca_scope = period_recon.copy()
            if ca_stores:
                ca_scope = ca_scope[ca_scope["Store"].isin(ca_stores)]

            rollup = (
                ca_scope.groupby(["Store", "Category Name", "Reason"], as_index=False)
                .agg(Adjustments=("COGS", "count"), TRUE_AUDIT_COST=("COGS", "sum"))
                .rename(columns={"Category Name": "Category"})
            )

            def _classify_reason(r):
                if r in APPROVED_REASONS:
                    return "✅ Approved"
                if r in DNU_REASONS:
                    return "🚫 DNU"
                return "⚠️ Unknown"

            rollup["Compliance"] = rollup["Reason"].apply(_classify_reason)
            if ca_compliance:
                rollup = rollup[rollup["Compliance"].isin(ca_compliance)]

            dnu_mask = rollup["Reason"].isin(DNU_REASONS)
            dnu_rows = int(dnu_mask.sum())
            dnu_cogs = float(rollup.loc[dnu_mask, "TRUE_AUDIT_COST"].abs().sum())
            total_rows = len(rollup)
            distinct_stores = rollup["Store"].nunique()

            m1, m2, m3 = st.columns(3)
            with m1:
                st.metric("🚫 DNU rows", f"{dnu_rows:,}", help=f"${dnu_cogs:,.2f} absolute COGS")
            with m2:
                st.metric("Total rows", f"{total_rows:,}")
            with m3:
                st.metric("Distinct stores", f"{distinct_stores:,}")

            if rollup.empty:
                st.info("No rows match the current filters.")
            else:
                st.markdown("---")
                st.markdown("**Cross-tab.** Shop × Category in rows, Reason in columns, SUM of COGS in cells.")

                xt = pd.pivot_table(
                    rollup,
                    index=["Store", "Category"],
                    columns="Reason",
                    values="TRUE_AUDIT_COST",
                    aggfunc="sum",
                    fill_value=0,
                )
                reason_cols = list(xt.columns)
                dnu_cols = sorted([r for r in reason_cols if r in DNU_REASONS])
                appr_cols = sorted([r for r in reason_cols if r in APPROVED_REASONS])
                unk_cols = sorted([r for r in reason_cols if r not in DNU_REASONS and r not in APPROVED_REASONS])
                ordered_reasons = dnu_cols + unk_cols + appr_cols
                xt = xt[ordered_reasons]

                def _col_label(r):
                    prefix = "🚫 " if r in DNU_REASONS else "✅ " if r in APPROVED_REASONS else "⚠️ "
                    return f"{prefix}{REASON_DISPLAY_LABELS.get(r, r)}"
                xt_display = xt.rename(columns={r: _col_label(r) for r in xt.columns})
                dnu_col_labels = [_col_label(r) for r in dnu_cols]

                def _fmt_cell(v):
                    if pd.isna(v) or float(v) == 0.0:
                        return ""
                    return f"${v:,.2f}"

                def _style_xt(df):
                    styles = pd.DataFrame("", index=df.index, columns=df.columns)
                    for c in df.columns:
                        nonzero = df[c].abs() > 0.0
                        zero = ~nonzero
                        is_dnu = c in dnu_col_labels
                        is_unk = c not in dnu_col_labels and not c.startswith("✅ ")
                        styles.loc[zero, c] = "color: #cccccc; text-align: right;"
                        if is_dnu:
                            styles.loc[nonzero, c] = "background-color: #ffcccc; color: #800000; font-weight: 700; text-align: right;"
                        elif is_unk:
                            styles.loc[nonzero, c] = "background-color: #fff3cd; color: #664d03; font-weight: 700; text-align: right;"
                        else:
                            styles.loc[nonzero, c] = "color: #1a3a52; font-weight: 700; text-align: right;"
                    return styles

                fmt_xt = {c: _fmt_cell for c in xt_display.columns}
                xt_styled = xt_display.style.format(fmt_xt, na_rep="").apply(_style_xt, axis=None)

                # Multi-select cross-tab: cmd/shift-click rows and columns to
                # extend selection. The transaction viewer below pulls the union
                # of (selected Shop+Category) and (selected Reason) scopes.
                xt_event = st.dataframe(
                    xt_styled,
                    use_container_width=True,
                    height=440,
                    on_select="rerun",
                    selection_mode=["multi-row", "multi-column"],
                    key="ca_xt_event",
                )

                xt_sel_rows = []
                xt_sel_cols = []
                if xt_event is not None and hasattr(xt_event, "selection"):
                    xt_sel_rows = list(getattr(xt_event.selection, "rows", []) or [])
                    xt_sel_cols = list(getattr(xt_event.selection, "columns", []) or [])

                # Decode selections into raw Shop+Category tuples and raw Reason codes
                ca_selected_scopes = []  # list of (Shop, Category) tuples
                for r_idx in xt_sel_rows:
                    if 0 <= r_idx < len(xt.index):
                        idx_label = xt.index[r_idx]
                        if isinstance(idx_label, tuple) and len(idx_label) == 2:
                            ca_selected_scopes.append(idx_label)

                ca_selected_reasons = []
                rev_reason_labels = {v: k for k, v in REASON_DISPLAY_LABELS.items()}
                for col_label in xt_sel_cols:
                    raw = col_label
                    for prefix in ("🚫 ", "✅ ", "⚠️ "):
                        if col_label.startswith(prefix):
                            raw = col_label[len(prefix):]
                            break
                    raw = rev_reason_labels.get(raw, raw)
                    ca_selected_reasons.append(raw)

                # Transaction viewer: pull period_recon rows matching the selection.
                st.markdown("---")
                st.subheader("Transaction viewer")
                if not ca_selected_scopes and not ca_selected_reasons:
                    st.info("Click rows or columns on the cross-tab above to populate the transaction viewer. Cmd/shift-click to multi-select.")
                else:
                    ca_drill = period_recon.copy()
                    if ca_selected_scopes:
                        scope_mask = pd.Series(False, index=ca_drill.index)
                        for shop, cat in ca_selected_scopes:
                            scope_mask |= (
                                (ca_drill["Store"] == shop)
                                & (ca_drill["Category Name"] == cat)
                            )
                        ca_drill = ca_drill[scope_mask]
                    if ca_selected_reasons:
                        ca_drill = ca_drill[ca_drill["Reason"].isin(ca_selected_reasons)]

                    ca_drill = ca_drill.copy()
                    ca_drill["_abs"] = ca_drill["COGS"].abs()
                    ca_drill = ca_drill.sort_values("_abs", ascending=False).drop(columns="_abs")

                    drill_show_cols = [c for c in LISA_DRILL_COLS if c in ca_drill.columns]

                    # Selection summary
                    scope_parts = []
                    if ca_selected_scopes:
                        scope_parts.append(
                            f"{len(ca_selected_scopes)} scope(s): "
                            + ", ".join(f"{s} · {c}" for s, c in ca_selected_scopes[:3])
                            + (f", +{len(ca_selected_scopes) - 3} more" if len(ca_selected_scopes) > 3 else "")
                        )
                    if ca_selected_reasons:
                        scope_parts.append(
                            f"{len(ca_selected_reasons)} reason(s): "
                            + ", ".join(ca_selected_reasons[:5])
                            + (f", +{len(ca_selected_reasons) - 5} more" if len(ca_selected_reasons) > 5 else "")
                        )
                    st.caption(" · ".join(scope_parts))

                    tac = ca_drill["COGS"].sum()
                    st.markdown(
                        f"**{len(ca_drill):,} transaction(s).** Sum COGS ${tac:,.2f}"
                    )
                    st.caption("View only on this tab. Use the Per-Store Homework tab to flag transactions for a GM packet.")
                    st.dataframe(
                        _apply_drill_labels(ca_drill[drill_show_cols]),
                        use_container_width=True,
                        hide_index=True,
                        height=440,
                    )
                    download_buttons(
                        _apply_drill_labels(ca_drill[drill_show_cols]),
                        "compliance_drill",
                        "ca_drill_dl",
                    )

    # == Tab Per-Store Homework ==
    with tab_workbook:
        st.caption(
            "Build one store's GM homework. Pick a store, scan the combined "
            "Adjustment Breakdown (Sales COGS + % on the right, 2% breaches highlighted), "
            "click row / column / cell to scope the drill, flag transactions, "
            "download Explanations Needed. Switching stores clears the current cart "
            "(download first if you want to keep it)."
        )

        period_recon_wb = period_recon.copy() if not period_recon.empty else pd.DataFrame()

        # Date Timestamp fallback: if the persisted recon data was uploaded
        # before v2.6.0, "Date Timestamp" won't exist; alias from "Date" so
        # the drill / cart / homework export all carry a date column.
        if (not period_recon_wb.empty
                and "Date Timestamp" not in period_recon_wb.columns
                and "Date" in period_recon_wb.columns):
            period_recon_wb["Date Timestamp"] = period_recon_wb["Date"]

        if period_recon_wb.empty:
            st.info("No reconciliation data for this period.")
        else:
            def _txn_id_wb(row):
                return f"{row.get('Reconciliation No')}|{row.get('Batch SKU')}|{row.get('Difference')}|{row.get('COGS')}"
            period_recon_wb["_txn_id"] = period_recon_wb.apply(_txn_id_wb, axis=1)

            for k, default in [
                ("homework_cart", set()),
                ("homework_cart_store", None),
                ("lw_pivot_event_version", 0),
                ("lw_drill_event_version", 0),
                ("lw_cart_event_version", 0),
            ]:
                if k not in st.session_state:
                    st.session_state[k] = default

            if "Category" in period_sales.columns:
                cat_sales_wb = period_sales.groupby(["Store", "Category"], as_index=False)["Sales COGS"].sum()
            elif "Product Category" in period_sales.columns:
                cat_sales_wb = (
                    period_sales.groupby(["Store", "Product Category"], as_index=False)["Sales COGS"].sum()
                    .rename(columns={"Product Category": "Category"})
                )
            else:
                cat_sales_wb = pd.DataFrame(columns=["Store", "Category", "Sales COGS"])

            def _classify_reason_wb(r):
                if r in APPROVED_REASONS:
                    return "✅ Approved"
                if r in DNU_REASONS:
                    return "🚫 DNU"
                return "⚠️ Unknown"

            # ----- Store picker -----
            available_stores = [s for s in STORE_ORDER if s in period_recon_wb["Store"].unique()]
            picker_options = ["(pick a store)"] + available_stores
            picked = st.selectbox(
                "Pick a store",
                options=picker_options,
                key="lw_store_picker",
            )
            selected_store = None if picked == "(pick a store)" else picked

            # Cart-clears-on-switch: if the store selection changed, clear the cart.
            prev_store = st.session_state["homework_cart_store"]
            if selected_store != prev_store:
                if st.session_state["homework_cart"]:
                    st.session_state["homework_cart"] = set()
                    st.session_state["lw_pivot_event_version"] += 1
                    st.session_state["lw_drill_event_version"] += 1
                    st.session_state["lw_cart_event_version"] += 1
                st.session_state["homework_cart_store"] = selected_store

            if selected_store is None:
                st.info("👈 Pick a store above to start the drill flow.")
            else:
                shop_full = next(
                    (k for k, v in STORE_NAME_MAP.items() if v == selected_store),
                    selected_store,
                )
                store_recon = period_recon_wb[period_recon_wb["Store"] == selected_store]

                # ----- Combined Adjustment Breakdown (with Sales COGS + % cols) -----
                # Merges what used to be the separate Shrinkage summary into the
                # Adjustment Breakdown cross-tab so Lisa can scan reason mix AND
                # breach % from one table. Click behavior preserved: row scopes
                # by Category, column scopes by Reason, both scope to a cell.
                st.markdown("---")
                st.subheader(f"Adjustment Breakdown ({selected_store})")
                st.caption(
                    "Categories in rows, Reasons in columns, SUM of COGS in cells. "
                    "Sales COGS + shrinkage % on the right (red highlight when |%| > 2). "
                    "Click a row to scope by Category, a column header to scope by Reason, "
                    "both to drill into a cell. Click again to deselect."
                )

                # Universe of categories: union of sales + recon so that zero-shrink
                # categories still appear and the Grand Total reconciles to store
                # Sales COGS.
                store_sales_cats = set()
                if not cat_sales_wb.empty:
                    store_sales_cats = set(
                        cat_sales_wb.loc[cat_sales_wb["Store"] == selected_store, "Category"].dropna().unique()
                    )
                recon_cats = set(store_recon["Category Name"].dropna().unique()) if not store_recon.empty else set()
                all_cats = sorted(store_sales_cats | recon_cats)

                # Per-category Sales COGS lookup + store total (used by Grand Total row)
                sales_cogs_by_cat = {}
                if not cat_sales_wb.empty:
                    store_sales_slice = cat_sales_wb[cat_sales_wb["Store"] == selected_store]
                    sales_cogs_by_cat = dict(zip(store_sales_slice["Category"], store_sales_slice["Sales COGS"]))
                store_total_sales_cogs = float(
                    sales_by_store[sales_by_store["Store"] == selected_store]["Store Sales COGS"].sum()
                ) if not sales_by_store.empty else 0.0

                # (Per-cat shrink summary build dropped in v2.6.4: the homework
                # export now contains only the Explanations Needed tab. The combined
                # Adjustment Breakdown above is the on-page summary surface.)
                shrink_recon = store_recon[store_recon["Reason"].isin(["OVERSOLD", "UNDERSOLD"])]

                xt3 = pd.pivot_table(
                    store_recon,
                    index="Category Name",
                    columns="Reason",
                    values="COGS",
                    aggfunc="sum",
                    fill_value=0,
                    margins=True,
                    margins_name="Grand Total",
                ) if not store_recon.empty else pd.DataFrame(index=pd.Index([], name="Category Name"))

                # Reindex to include all_cats (sales-only cats show as zero rows so
                # the Grand Total reconciles to store Sales COGS).
                xt3 = xt3.reindex(all_cats + ["Grand Total"], fill_value=0)

                reason_cols_present = [c for c in xt3.columns if c != "Grand Total"]
                xt3_dnu_cols = sorted([r for r in reason_cols_present if r in DNU_REASONS])
                xt3_appr_cols = sorted([r for r in reason_cols_present if r in APPROVED_REASONS])
                xt3_unk_cols = sorted([r for r in reason_cols_present if r not in DNU_REASONS and r not in APPROVED_REASONS])

                # Append Sales COGS first (used in % calc), then % column
                sales_cogs_series = pd.Series(
                    {cat: float(sales_cogs_by_cat.get(cat, 0)) for cat in xt3.index if cat != "Grand Total"},
                    dtype=float,
                )
                sales_cogs_series["Grand Total"] = store_total_sales_cogs
                xt3["Sales COGS"] = xt3.index.map(sales_cogs_series).astype(float)

                # % = (OVERSOLD + UNDERSOLD) / Sales COGS per row
                ov = xt3["OVERSOLD"] if "OVERSOLD" in xt3.columns else 0
                un = xt3["UNDERSOLD"] if "UNDERSOLD" in xt3.columns else 0
                tac_series = ov + un
                if isinstance(tac_series, pd.Series):
                    pct_series = tac_series.divide(xt3["Sales COGS"]).where(xt3["Sales COGS"] != 0, other=pd.NA)
                else:
                    pct_series = pd.Series(pd.NA, index=xt3.index, dtype="object")
                xt3["%"] = pct_series

                # Final column order: Category (index) | % | Grand Total | Reasons (DNU, Unknown, Approved) | Sales COGS
                final_order = ["%"]
                if "Grand Total" in xt3.columns:
                    final_order.append("Grand Total")
                final_order += xt3_dnu_cols + xt3_unk_cols + xt3_appr_cols
                final_order.append("Sales COGS")
                xt3 = xt3[final_order]

                def _xt3_label(r):
                    if r in ("Grand Total", "Sales COGS", "%"):
                        return r
                    prefix = "🚫 " if r in DNU_REASONS else "✅ " if r in APPROVED_REASONS else "⚠️ "
                    return f"{prefix}{REASON_DISPLAY_LABELS.get(r, r)}"
                xt3_display = xt3.rename(columns={r: _xt3_label(r) for r in xt3.columns})
                xt3_dnu_labels = [_xt3_label(r) for r in xt3_dnu_cols]
                xt3_unk_labels = [_xt3_label(r) for r in xt3_unk_cols]

                def _xt3_fmt_dollar(v):
                    if pd.isna(v) or float(v) == 0.0:
                        return ""
                    return f"${v:,.2f}"

                def _xt3_fmt_pct(v):
                    if pd.isna(v):
                        return ""
                    return f"{v:.2%}"

                def _xt3_style(df):
                    styles = pd.DataFrame("", index=df.index, columns=df.columns)
                    for c in df.columns:
                        if c == "%":
                            pct_vals = pd.to_numeric(df[c], errors="coerce")
                            breach = (pct_vals.abs() > 0.02).fillna(False)
                            styles.loc[breach, c] = "background-color: #ffcccc; color: #800000; font-weight: 700; text-align: right;"
                            styles.loc[~breach, c] = "text-align: right;"
                            continue
                        if c == "Sales COGS":
                            styles.loc[:, c] = "color: #1a1a1a; background-color: #f7f7f7; text-align: right;"
                            continue
                        nonzero = df[c].abs() > 0.0
                        zero = ~nonzero
                        styles.loc[zero, c] = "color: #cccccc; text-align: right;"
                        if c in xt3_dnu_labels:
                            styles.loc[nonzero, c] = "background-color: #ffcccc; color: #800000; font-weight: 700; text-align: right;"
                        elif c in xt3_unk_labels:
                            styles.loc[nonzero, c] = "background-color: #fff3cd; color: #664d03; font-weight: 700; text-align: right;"
                        elif c == "Grand Total":
                            styles.loc[nonzero, c] = "color: #1a1a1a; font-weight: 700; background-color: #f0f0f0; text-align: right;"
                        else:
                            styles.loc[nonzero, c] = "color: #1a3a52; font-weight: 700; text-align: right;"
                    if "Grand Total" in df.index:
                        for c in df.columns:
                            base = styles.loc["Grand Total", c]
                            styles.loc["Grand Total", c] = (base + "; background-color: #f0f0f0; font-weight: 700;").lstrip("; ")
                    return styles

                fmt_xt3 = {c: _xt3_fmt_dollar for c in xt3_display.columns if c != "%"}
                fmt_xt3["%"] = _xt3_fmt_pct
                xt3_styled = xt3_display.style.format(fmt_xt3, na_rep="").apply(_xt3_style, axis=None)

                xt3_key = f"lw_xt3_event_v{st.session_state['lw_pivot_event_version']}_{selected_store}"
                xt3_event = st.dataframe(
                    xt3_styled,
                    use_container_width=True,
                    height=440,
                    on_select="rerun",
                    selection_mode=["multi-row", "multi-column"],
                    key=xt3_key,
                )

                # Decode multi-row + multi-column selection from the cross-tab.
                # Each click extends the selection; cmd/shift-click works natively.
                # Grand Total row + non-reason columns (%, Grand Total, Sales COGS) are
                # ignored for filter-scoping purposes.
                xt3_sel_rows = []
                xt3_sel_cols = []
                if xt3_event is not None and hasattr(xt3_event, "selection"):
                    xt3_sel_rows = list(getattr(xt3_event.selection, "rows", []) or [])
                    xt3_sel_cols = list(getattr(xt3_event.selection, "columns", []) or [])

                xt3_selected_cats = []
                for r_idx in xt3_sel_rows:
                    if 0 <= r_idx < len(xt3_display.index):
                        label = xt3_display.index[r_idx]
                        if label != "Grand Total":
                            xt3_selected_cats.append(label)

                xt3_selected_reasons = []
                reverse_reason_labels = {v: k for k, v in REASON_DISPLAY_LABELS.items()}
                for col_label in xt3_sel_cols:
                    if col_label in ("Grand Total", "Sales COGS", "%"):
                        continue
                    raw = col_label
                    for prefix in ("🚫 ", "✅ ", "⚠️ "):
                        if col_label.startswith(prefix):
                            raw = col_label[len(prefix):]
                            break
                    raw = reverse_reason_labels.get(raw, raw)
                    xt3_selected_reasons.append(raw)

                # Sync the drill filters to the cross-tab selection.
                # Replace (not additive): cross-tab is now the source of truth for scoping.
                # When the user manually edits the multiselect filters below, those edits
                # persist between cross-tab interactions. only an actual change to the
                # cross-tab selection overwrites the filter.
                cat_filter_key = f"lw_drill_cat_filter_{selected_store}"
                rsn_filter_key = f"lw_drill_reason_filter_{selected_store}"
                applied_sel_key = f"lw_xt_applied_sel_{selected_store}"
                current_xt_sel = (tuple(xt3_selected_cats), tuple(xt3_selected_reasons))
                prev_applied = st.session_state.get(applied_sel_key)
                if current_xt_sel != prev_applied:
                    st.session_state[cat_filter_key] = list(xt3_selected_cats)
                    st.session_state[rsn_filter_key] = list(xt3_selected_reasons)
                    st.session_state[applied_sel_key] = current_xt_sel

                # ----- Drill panel -----
                st.markdown("---")
                st.subheader("Drill (flag transactions for the homework)")
                st.caption(
                    "All reconciliations for this store. Pre-sorted by absolute COGS so the largest "
                    "amounts surface first. Each row carries its own Reason, flag any transaction, "
                    "not just OVERSOLD/UNDERSOLD shrinkage. Click on Adjustment Breakdown above to "
                    "scope automatically, or use the manual filters."
                )

                # "Store" is constant in this view, drop from the visible drill columns
                drill_cols = [c for c in LISA_DRILL_COLS if c in store_recon.columns and c != "Store"]
                missing_cols = [c for c in LISA_DRILL_COLS if c not in store_recon.columns and c != "Store"]
                if missing_cols:
                    st.caption(f"_Columns not in current data (re-upload to populate): {', '.join(missing_cols)}_")

                cat_options = sorted(store_recon["Category Name"].dropna().unique())
                reason_options = sorted(store_recon["Reason"].dropna().unique())
                fc1, fc2, fc3 = st.columns([2, 2, 1])
                with fc1:
                    drill_cat_filter = st.multiselect(
                        "Category filter",
                        options=cat_options,
                        key=cat_filter_key,
                        help="Empty = all categories",
                    )
                with fc2:
                    drill_reason_filter = st.multiselect(
                        "Reason filter",
                        options=reason_options,
                        key=rsn_filter_key,
                        help="Empty = all reasons",
                    )
                with fc3:
                    drill_dnu_only = st.checkbox(
                        "DNU only",
                        value=False,
                        key=f"lw_drill_dnu_{selected_store}",
                    )

                drill_scope = store_recon.copy()
                if drill_cat_filter:
                    drill_scope = drill_scope[drill_scope["Category Name"].isin(drill_cat_filter)]
                if drill_reason_filter:
                    drill_scope = drill_scope[drill_scope["Reason"].isin(drill_reason_filter)]
                if drill_dnu_only:
                    drill_scope = drill_scope[drill_scope["Reason"].isin(DNU_REASONS)]

                drill_scope = drill_scope.copy()
                drill_scope["_abs_cogs"] = drill_scope["COGS"].abs()
                drill_scope = drill_scope.sort_values("_abs_cogs", ascending=False).drop(columns="_abs_cogs")

                # Filter signature drives drill key + select-all key so both reset
                # cleanly when the filter changes.
                filter_sig = (
                    f"{tuple(sorted(drill_cat_filter))}_"
                    f"{tuple(sorted(drill_reason_filter))}_{drill_dnu_only}"
                )

                drill_tac_total = drill_scope["COGS"].sum()
                cart_set_view = st.session_state["homework_cart"]
                already_in_cart = int(drill_scope["_txn_id"].isin(cart_set_view).sum())
                st.markdown(
                    f"**{len(drill_scope):,} transaction(s) visible.** Sum COGS "
                    f"${drill_tac_total:,.2f} · {already_in_cart} already in cart"
                )

                # Select all visible: when ticked, every visible row shows ✓ as
                # a visual cue AND the Add button switches to "Add all visible"
                # (no need to click individual rows).
                select_all_key = f"lw_select_all_{selected_store}_{filter_sig}"
                select_all = st.checkbox(
                    f"☑ Select all visible ({len(drill_scope):,} rows)",
                    value=False,
                    key=select_all_key,
                    help="Tick to flag every row in the current filtered view at once.",
                )

                # No custom ✓ column. Cart membership shown via "already in cart"
                # count above; the native streamlit selection checkboxes on the
                # left of the dataframe are the only checkmarks in the table.
                drill_display = drill_scope[drill_cols + ["_txn_id"]].reset_index(drop=True)

                drill_key = (
                    f"lw_drill_event_v{st.session_state['lw_drill_event_version']}_"
                    f"{selected_store}_{filter_sig}"
                )

                drill_event = st.dataframe(
                    _apply_drill_labels(drill_display.drop(columns="_txn_id")),
                    use_container_width=True,
                    hide_index=True,
                    height=420,
                    on_select="rerun",
                    selection_mode="multi-row",
                    key=drill_key,
                )

                drill_sel = []
                if drill_event is not None and hasattr(drill_event, "selection"):
                    drill_sel = list(drill_event.selection.rows or [])
                drill_sel_txn_ids = (
                    [drill_display.iloc[i]["_txn_id"] for i in drill_sel] if drill_sel else []
                )

                if select_all:
                    add_txn_ids = list(drill_display["_txn_id"])
                    add_label = f"➕ Add all {len(add_txn_ids)} visible to Explanations Needed"
                else:
                    add_txn_ids = drill_sel_txn_ids
                    add_label = f"➕ Add {len(add_txn_ids)} to Explanations Needed"

                add_col, info_col = st.columns([1, 3])
                with add_col:
                    if st.button(
                        add_label,
                        disabled=not add_txn_ids,
                        type="primary",
                        key="lw_add_btn",
                    ):
                        st.session_state["homework_cart"] = (
                            st.session_state["homework_cart"] | set(add_txn_ids)
                        )
                        st.session_state["lw_drill_event_version"] += 1
                        st.rerun()
                with info_col:
                    if not add_txn_ids:
                        st.caption("_Click a row, or shift/Cmd-click for multi-select. Or tick Select all visible above._")

                # ----- Explanations Needed cart -----
                st.markdown("---")
                st.subheader(f"Explanations Needed cart ({selected_store})")

                cart_set = st.session_state["homework_cart"]
                cart_rows = store_recon[store_recon["_txn_id"].isin(cart_set)]
                cart_count = len(cart_rows)
                cart_tac_abs = float(cart_rows["COGS"].abs().sum())

                if cart_count == 0:
                    st.info("No transactions in the cart yet. Click a row in the Adjustment Breakdown, then add transactions in the drill panel. You can still download the Shrinkage summary below.")
                    cart_sorted = pd.DataFrame(columns=store_recon.columns)
                    drill_cols_for_cart = [c for c in LISA_DRILL_COLS if c in store_recon.columns]
                else:
                    st.markdown(
                        f"**{cart_count} transaction(s)** flagged for **{selected_store}**. "
                        f"Total |TAC| **${cart_tac_abs:,.2f}**"
                    )
                    cart_rollup = cart_rows.groupby(["Category Name", "Reason"]).agg(
                        Flagged=("COGS", "count"),
                        TAC=("COGS", "sum"),
                    ).reset_index().rename(columns={
                        "Category Name": "Category",
                        "TAC": "TRUE AUDIT COST",
                    })
                    cart_rollup["Compliance"] = cart_rollup["Reason"].apply(_classify_reason_wb)
                    cart_rollup = cart_rollup[["Category", "Reason", "Compliance", "Flagged", "TRUE AUDIT COST"]]
                    st.dataframe(
                        cart_rollup.style.format({"TRUE AUDIT COST": "${:,.2f}"}),
                        use_container_width=True, hide_index=True,
                    )

                    drill_cols_for_cart = [c for c in LISA_DRILL_COLS if c in cart_rows.columns]
                    cart_sorted = cart_rows.copy()
                    cart_sorted["_abs_cogs"] = cart_sorted["COGS"].abs()
                    cart_sorted = cart_sorted.sort_values("_abs_cogs", ascending=False).drop(columns="_abs_cogs")
                    with st.expander("View / remove flagged transactions"):
                        cart_view = cart_sorted[drill_cols_for_cart + ["_txn_id"]].reset_index(drop=True)

                        cart_key = f"lw_cart_event_v{st.session_state['lw_cart_event_version']}_{selected_store}"
                        cart_event = st.dataframe(
                            _apply_drill_labels(cart_view.drop(columns="_txn_id")),
                            use_container_width=True,
                            hide_index=True,
                            height=320,
                            on_select="rerun",
                            selection_mode="multi-row",
                            key=cart_key,
                        )

                        rm_sel = []
                        if cart_event is not None and hasattr(cart_event, "selection"):
                            rm_sel = list(cart_event.selection.rows or [])
                        rm_txn_ids = (
                            [cart_view.iloc[i]["_txn_id"] for i in rm_sel] if rm_sel else []
                        )
                        rmc1, rmc2 = st.columns([1, 1])
                        with rmc1:
                            if st.button(
                                f"➖ Remove {len(rm_txn_ids)} from cart",
                                disabled=not rm_txn_ids,
                                key="lw_remove_btn",
                            ):
                                st.session_state["homework_cart"] = (
                                    st.session_state["homework_cart"] - set(rm_txn_ids)
                                )
                                st.session_state["lw_cart_event_version"] += 1
                                st.rerun()
                        with rmc2:
                            if st.button(
                                f"🗑️ Clear {selected_store} cart",
                                key="lw_clear_btn",
                            ):
                                st.session_state["homework_cart"] = set()
                                st.session_state["lw_pivot_event_version"] += 1
                                st.session_state["lw_drill_event_version"] += 1
                                st.session_state["lw_cart_event_version"] += 1
                                st.rerun()

                # ----- Download per-store homework (Explanations Needed only) -----
                # Per Charles 5/25: drop the Shrinkage summary tab. The download
                # is the transaction-level packet for the store GM to fill in
                # explanations against. No category-summary noise.
                st.markdown("---")

                # Build the Explanations Needed cart export. Date Timestamp is
                # already aliased from Date in period_recon_wb if needed.
                if cart_count > 0:
                    cart_export = _apply_drill_labels(cart_sorted[drill_cols_for_cart].copy())
                else:
                    cart_export = pd.DataFrame(
                        columns=[DRILL_DISPLAY_LABELS.get(c, c) for c in drill_cols_for_cart]
                    )
                cart_export["GM Explanation"] = ""

                sheets = {"Explanations needed": cart_export}

                # Build the xlsx with currency / number formats.
                buf = io.BytesIO()
                with pd.ExcelWriter(buf, engine="openpyxl") as writer:
                    for sheet_name, df_out in sheets.items():
                        df_out.to_excel(writer, index=False, sheet_name=sheet_name[:31])

                    DOLLAR_FMT = '"$"#,##0.00;[Red]("$"#,##0.00)'
                    QTY_FMT = '#,##0.00;[Red]-#,##0.00'

                    if "Explanations needed" in writer.sheets:
                        ws_e = writer.sheets["Explanations needed"]
                        headers_e = [c.value for c in ws_e[1]]
                        for col_idx, h in enumerate(headers_e, start=1):
                            if h in ("COGS", "Cost per Unit"):
                                for row in ws_e.iter_rows(min_row=2, min_col=col_idx,
                                                          max_col=col_idx, max_row=ws_e.max_row):
                                    for cell in row:
                                        cell.number_format = DOLLAR_FMT
                            elif h == "Difference":
                                for row in ws_e.iter_rows(min_row=2, min_col=col_idx,
                                                          max_col=col_idx, max_row=ws_e.max_row):
                                    for cell in row:
                                        cell.number_format = QTY_FMT
                buf.seek(0)

                period_tag = selected_period or "all"
                safe_store = selected_store.replace(" ", "_").replace("/", "-")

                dl_col, gs_col = st.columns([1, 1])
                with dl_col:
                    st.download_button(
                        f"📥 Download {selected_store} Homework (xlsx)",
                        data=buf,
                        file_name=f"homework_{safe_store}_{period_tag}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        type="primary",
                        key="lw_homework_dl",
                    )
                with gs_col:
                    if has_sheets_config():
                        folder_id = _homework_drive_folder_id()
                        sa_email = _service_account_email()
                        if folder_id:
                            short_fid = folder_id[:8] + "…" + folder_id[-4:] if len(folder_id) > 16 else folder_id
                            st.caption(f"📁 Drive folder configured: `{short_fid}`")
                        else:
                            st.caption("⚠️ No `homework_drive_folder_id` secret set; will hit service account quota.")
                        if st.button(
                            f"🔗 Create as Google Sheet",
                            key="lw_homework_gs_btn",
                            help="Creates a live Google Sheet you can share with the GM directly.",
                            disabled=(cart_count == 0),
                        ):
                            with st.spinner("Creating Google Sheet..."):
                                try:
                                    gs_url = make_homework_gsheet(
                                        sheets,
                                        title=f"Homework {selected_store} {period_tag}",
                                        share_with=[USER_EMAIL],
                                        folder_id=folder_id,
                                    )
                                    st.session_state["lw_homework_gs_url"] = gs_url
                                    st.session_state["lw_homework_gs_label"] = (
                                        f"{selected_store} · {period_tag} · "
                                        f"{cart_count} flagged txn"
                                    )
                                except Exception as e:
                                    err_str = str(e)
                                    quota_hit = ("storage quota" in err_str.lower()
                                                 or "[403]" in err_str
                                                 or "userRateLimitExceeded" in err_str)
                                    st.error(f"Could not create Google Sheet: {e}")
                                    with st.expander("Troubleshooting"):
                                        st.markdown(
                                            f"**Service account email** (must be Editor on your folder):  \n"
                                            f"`{sa_email or '(not found in secrets)'}`\n\n"
                                            f"**Folder ID being used:**  \n"
                                            f"`{folder_id or '(none, secret missing)'}`\n\n"
                                            f"**Common fixes:**\n"
                                            f"- Verify the secret key is exactly `homework_drive_folder_id` "
                                            f"(top-level, NOT nested under `[google_sheets]`).\n"
                                            f"- Verify the folder above is shared with the service account "
                                            f"email as **Editor** (not Viewer).\n"
                                            f"- If the folder is in a Shared Drive, make sure the service "
                                            f"account is a member of that Shared Drive (Manage members).\n"
                                            f"- Click 'Reboot app' in Streamlit Cloud after saving secrets; "
                                            f"the secrets file is cached until reboot."
                                        )
                                        if quota_hit and not folder_id:
                                            st.markdown(
                                                f"**One-time setup (no folder configured yet):**\n\n"
                                                f"1. Create a folder in your Drive.\n"
                                                f"2. Share it with `{sa_email}` as Editor.\n"
                                                f"3. Copy folder ID from URL.\n"
                                                f"4. Add to Streamlit secrets: "
                                                f"`homework_drive_folder_id = \"<id>\"`\n"
                                                f"5. Reboot app."
                                            )
                        if cart_count == 0:
                            st.caption("_Flag transactions first (drill panel above), then create the sheet._")
                    else:
                        st.caption("Google Sheets not configured.")

                last_gs_url = st.session_state.get("lw_homework_gs_url")
                if last_gs_url:
                    st.success(
                        f"Last homework sheet ({st.session_state.get('lw_homework_gs_label', '')}): "
                        f"[Open in Google Sheets]({last_gs_url})"
                    )

                st.caption(
                    f"Workbook: 'Explanations needed' tab ({cart_count} flagged transaction(s) "
                    f"with a blank GM Explanation column for the store manager to fill in). "
                    f"COGS and Cost per Unit formatted as currency. Populated from the drill "
                    f"panel above (flag transactions and Add to Explanations Needed first)."
                )

    # == Tab 3: Legacy Shrink ==
    with tab3:
        st.caption(
            "OVERSOLD + UNDERSOLD by store and category. Matches Georgina's weekly shrinkage report."
        )

        # Sales by store+category for this period
        if "Category" in period_sales.columns:
            legacy_sales = period_sales.groupby(
                ["Store", "Category"], as_index=False
            )["Sales COGS"].sum()
        elif "Product Category" in period_sales.columns:
            legacy_sales = (
                period_sales.groupby(["Store", "Product Category"], as_index=False)
                ["Sales COGS"].sum()
                .rename(columns={"Product Category": "Category"})
            )
        else:
            legacy_sales = pd.DataFrame()

        legacy_reasons = ["OVERSOLD", "UNDERSOLD"]
        legacy_recon = period_recon[period_recon["Reason"].isin(legacy_reasons)] if not period_recon.empty else pd.DataFrame()

        if legacy_recon.empty:
            st.info("No shrinkage data for this period.")
        else:
            # Store + Category detail
            legacy_cat = (
                legacy_recon.groupby(["Store", "Category Name"])
                .agg(TRUE_AUDIT_COST=("COGS", "sum"))
                .reset_index()
                .rename(columns={"Category Name": "Category"})
            )
            if not legacy_sales.empty:
                legacy_cat = legacy_cat.merge(legacy_sales, on=["Store", "Category"], how="left")
                legacy_cat.rename(columns={"Sales COGS": "COGS"}, inplace=True)
                legacy_cat["%"] = legacy_cat.apply(
                    lambda r: r["TRUE_AUDIT_COST"] / r["COGS"]
                    if pd.notna(r.get("COGS")) and r.get("COGS", 0) != 0 else None,
                    axis=1,
                )

            # Store-level totals for sales COGS
            legacy_store_cogs = legacy_sales.groupby("Store", as_index=False)["Sales COGS"].sum() if not legacy_sales.empty else pd.DataFrame()

            # Network summary (use ALL sales COGS, not just categories with adjustments)
            net_tac = legacy_cat["TRUE_AUDIT_COST"].sum()
            net_cogs = sales_by_store["Store Sales COGS"].sum() if not sales_by_store.empty else 0
            net_pct = net_tac / net_cogs if net_cogs != 0 else None
            c1, c2, c3 = st.columns(3)
            with c1:
                st.metric("Network TRUE AUDIT COST", format_currency(net_tac))
            with c2:
                st.metric("Network COGS", format_currency(net_cogs))
            with c3:
                st.metric("Network %", format_pct(net_pct))

            # Nested: one expander per store with subtotal + category table
            fmt = {"TRUE AUDIT COST": "${:,.2f}", "COGS": "${:,.2f}", "%": "{:.2%}"}

            for store in sorted(legacy_cat["Store"].unique(), key=store_sort_key):
                store_data = legacy_cat[legacy_cat["Store"] == store].copy()
                store_tac = store_data["TRUE_AUDIT_COST"].sum()
                # Use full store COGS (all categories), not just categories with adjustments
                store_full_cogs = sales_by_store[sales_by_store["Store"] == store]["Store Sales COGS"].sum() if not sales_by_store.empty else 0
                store_pct = store_tac / store_full_cogs if store_full_cogs != 0 else None
                pct_str = f"{store_pct:.2%}" if store_pct is not None else "N/A"

                with st.expander(f"**{store}**  |  TRUE AUDIT COST: ${store_tac:,.2f}  |  COGS: ${store_full_cogs:,.2f}  |  {pct_str}"):
                    cat_display = store_data[["Category", "TRUE_AUDIT_COST", "COGS", "%"]].copy() if "%" in store_data.columns else store_data[["Category", "TRUE_AUDIT_COST"]].copy()
                    cat_display = cat_display.rename(columns={"TRUE_AUDIT_COST": "TRUE AUDIT COST"})
                    cat_display = cat_display.sort_values("TRUE AUDIT COST")

                    avail = [c for c in ["Category", "TRUE AUDIT COST", "COGS", "%"] if c in cat_display.columns]
                    styled = cat_display[avail].style.format(
                        {k: v for k, v in fmt.items() if k in avail}, na_rep="N/A"
                    )
                    st.dataframe(styled, use_container_width=True, hide_index=True)

            # Full download
            full_download = legacy_cat.rename(columns={"TRUE_AUDIT_COST": "TRUE AUDIT COST"}).copy()
            full_cols = [c for c in ["Store", "Category", "TRUE AUDIT COST", "COGS", "%"] if c in full_download.columns]
            download_buttons(full_download[full_cols], "legacy_shrink", "legacy")

    # == Tab 4: Adjustments ==
    with tab4:
        # DDE (Display, Defective, Expired) - billed to vendor
        st.subheader("DDE (Display, Defective, Expired)")
        dde_net, dde_count = compute_group_total(period_recon, REASON_GROUPS["DDE"])
        st.metric("Network Total", f"${dde_net:,.2f} ({dde_count} adjustments)")
        st.caption("Billed to vendor")

        # Sub-group totals as a row of metrics
        sub_cols_row = st.columns(len(DDE_SUBGROUPS))
        for (sub_name, sub_reasons), col in zip(DDE_SUBGROUPS.items(), sub_cols_row):
            sub_filtered = period_recon[period_recon["Reason"].isin(sub_reasons)] if not period_recon.empty else pd.DataFrame()
            sub_net = sub_filtered["COGS"].sum() if not sub_filtered.empty else 0
            sub_count = len(sub_filtered)
            with col:
                st.metric(sub_name, f"${sub_net:,.2f}", help=f"{sub_count} adjustments")

        # Per-store table broken out by Display / Defective / Expired
        dde_filtered = period_recon[period_recon["Reason"].isin(REASON_GROUPS["DDE"])] if not period_recon.empty else pd.DataFrame()
        if dde_filtered.empty:
            st.caption("No DDE adjustments this period.")
        else:
            sub_map = {r: sub for sub, rs in DDE_SUBGROUPS.items() for r in rs}
            dde_filtered = dde_filtered.copy()
            dde_filtered["_subgroup"] = dde_filtered["Reason"].map(sub_map)
            dde_by_store = (
                dde_filtered.groupby(["Store", "_subgroup"], as_index=False)["COGS"].sum()
                .pivot(index="Store", columns="_subgroup", values="COGS")
                .fillna(0)
                .reset_index()
            )
            for sub in DDE_SUBGROUPS.keys():
                if sub not in dde_by_store.columns:
                    dde_by_store[sub] = 0.0
            dde_by_store["DDE Total"] = dde_by_store[list(DDE_SUBGROUPS.keys())].sum(axis=1)
            dde_by_store = dde_by_store.merge(sales_by_store, on="Store", how="left")
            dde_by_store["% of COGS"] = dde_by_store.apply(
                lambda r: r["DDE Total"] / r["Store Sales COGS"]
                if pd.notna(r.get("Store Sales COGS")) and r.get("Store Sales COGS", 0) != 0
                else None,
                axis=1,
            )
            dde_by_store["_s"] = dde_by_store["Store"].map(store_sort_key)
            dde_by_store = dde_by_store.sort_values("_s").drop(columns="_s")

            sub_list = list(DDE_SUBGROUPS.keys())
            dde_cols = ["Store"] + sub_list + ["DDE Total", "Store Sales COGS", "% of COGS"]
            dde_display = dde_by_store[[c for c in dde_cols if c in dde_by_store.columns]].copy()

            # Grand total row. full network COGS denominator (zero-DDE stores stay in)
            full_cogs = sales_by_store["Store Sales COGS"].sum() if not sales_by_store.empty else 0
            totals = {"Store": "NETWORK TOTAL"}
            for c in dde_display.columns:
                if c == "Store":
                    continue
                if c == "Store Sales COGS":
                    totals[c] = full_cogs
                elif c == "% of COGS":
                    net = dde_display["DDE Total"].sum() if "DDE Total" in dde_display.columns else 0
                    totals[c] = net / full_cogs if full_cogs != 0 else None
                else:
                    totals[c] = dde_display[c].sum()
            dde_with_total = pd.concat([dde_display, pd.DataFrame([totals])], ignore_index=True)

            fmt_dde = {
                "Display": "${:,.2f}",
                "Defective": "${:,.2f}",
                "Expired": "${:,.2f}",
                "DDE Total": "${:,.2f}",
                "Store Sales COGS": "${:,.2f}",
                "% of COGS": "{:.2%}",
            }
            styled_dde = dde_with_total.style.format(
                {k: v for k, v in fmt_dde.items() if k in dde_with_total.columns}, na_rep="N/A"
            )
            st.dataframe(styled_dde, use_container_width=True, hide_index=True)
            download_buttons(dde_with_total, "dde_by_store", "dde")

        st.markdown("---")
        render_group_table(period_recon, sales_by_store, REASON_GROUPS["Samples"], "Samples", "samp")
        st.markdown("---")
        render_group_table(period_recon, sales_by_store, REASON_GROUPS["Recall"], "Recall", "rec")
        st.markdown("---")
        render_group_table(period_recon, sales_by_store, REASON_GROUPS["Other"], "Other", "oth")

        # Handle blank/no-reason entries
        no_reason = period_recon[
            period_recon["Reason"].isna() | (period_recon["Reason"].str.strip() == "")
        ] if not period_recon.empty else pd.DataFrame()
        if not no_reason.empty:
            st.markdown("---")
            st.subheader("No Reason")
            nr_net = no_reason["COGS"].sum()
            st.metric("Network Total", f"${nr_net:,.2f} ({len(no_reason)} adjustments)")
            nr_cols = ["Date", "Store", "Employee Name", "Product Name", "Difference", "COGS", "Reason Note"]
            avail_nr = [c for c in nr_cols if c in no_reason.columns]
            st.dataframe(no_reason[avail_nr], use_container_width=True, hide_index=True)

    # == Tab 5: Incorrect Quantity ==
    with tab5:
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

    # == Tab 6: Employees ==
    with tab6:
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

    # == Tab 7: Raw Data ==
    with tab7:
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
