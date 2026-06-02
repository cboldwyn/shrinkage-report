# Retail Reporting

Haven internal tool for automated weekly/monthly retail reporting. Shrinkage: inventory adjustment costs vs sales COGS by location, category, and employee. Discounts: every discount dollar reconciled to Loyalty / Employee / Promotional / Member-Manual buckets, with Albert's weekly cuts plus change-over-time. Both run off the same Blaze Total Sales Detail upload.

## Inputs

- **Inventory Reconciliation History** CSV from Blaze
- **Total Sales Detail** CSV from Blaze

## Deploy

Auto-deploys to Streamlit Cloud on push to main.
