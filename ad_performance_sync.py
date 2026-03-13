#!/usr/bin/env python3
"""
Impact.com Ad Performance → Google Sheets Pipeline
===================================================
Fetches all Ads/Creatives from Impact.com, retrieves partner-level
performance data (Actions + Clicks) for each Ad, normalises into flat
rows, and writes one Google Sheets tab per Ad (Ad_{AdID}).

Usage:
    python ad_performance_sync.py --no-server            # incremental
    python ad_performance_sync.py --no-server --full      # full 90-day refresh
"""

import os
import sys
import json
import time
import csv
import io
import base64
import logging
import argparse
from datetime import datetime, timezone, timedelta
from collections import defaultdict

import requests
import pandas as pd
from requests.auth import HTTPBasicAuth
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ── Logging ──────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("ad_perf_sync")

# ── Environment ──────────────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


def _require_env(name: str) -> str:
    val = os.getenv(name)
    if not val:
        log.error(f"FATAL: Missing required env var: {name}")
    return val or ""


ACCOUNT_SID = _require_env("IMPACT_ACCOUNT_SID")
AUTH_TOKEN = _require_env("IMPACT_AUTH_TOKEN")
AUTH = HTTPBasicAuth(ACCOUNT_SID, AUTH_TOKEN) if ACCOUNT_SID and AUTH_TOKEN else None

# Hard-coded target spreadsheet
SPREADSHEET_ID = os.getenv(
    "AD_PERFORMANCE_SPREADSHEET_ID",
    "1fg9yrhhFk2H-0vLvtG3jT9ouizszC7KxggEDaCsvs_8",
)

# ── Config ───────────────────────────────────────────────────────────
IMPACT_BASE = "https://api.impact.com"
PAGE_SIZE = 1000
MAX_PAGES = 200
MAX_RETRIES = 5
RATE_LIMIT_DELAY = 0.5
FULL_HISTORY_DAYS = 90
INCREMENTAL_HOURS = 48
CHUNK_DAYS = 30  # must be ≤ 45 (API limit)

# Click export settings
CLICK_POLL_INTERVAL = 5
CLICK_MAX_POLLS = 60
CLICK_BETWEEN_CAMPAIGNS = 15

HEADER_ROW = [
    "Partner ID", "Partner Name",
    "Ad ID", "Ad Name",
    "Impressions", "Clicks", "Conversions",
    "Revenue", "Cost", "Date",
]

# Sheet1 = high-level ad aggregate (one row per ad)
SHEET1_HEADER = [
    "Ad Name", "Ad Id", "Ad Type", "Landing Page",
    "Partners", "Clicks", "Actions", "Revenue",
    "Action Cost", "Total Cost", "CPC",
    "Start Date", "End Date",
]

SUMMARY_SHEET = "Summary"
SHEET1_NAME = "Sheet1"

# ══════════════════════════════════════════════════════════════════════
#  Google Sheets helpers
# ══════════════════════════════════════════════════════════════════════

def get_google_creds():
    """Return Google service-account credentials."""
    b64 = os.getenv("GOOGLE_CREDENTIALS_JSON")
    if b64:
        try:
            try:
                creds_json = json.loads(b64)
            except json.JSONDecodeError:
                creds_json = json.loads(base64.b64decode(b64))
            return service_account.Credentials.from_service_account_info(
                creds_json, scopes=["https://www.googleapis.com/auth/spreadsheets"],
            )
        except Exception as e:
            raise RuntimeError(f"Failed to parse GOOGLE_CREDENTIALS_JSON: {e}") from e
    if os.path.exists("credentials.json"):
        return service_account.Credentials.from_service_account_file(
            "credentials.json",
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
    raise RuntimeError("No Google credentials found (set GOOGLE_CREDENTIALS_JSON or provide credentials.json).")


def get_sheets_service():
    return build("sheets", "v4", credentials=get_google_creds())


def _sheets_retry(fn, label="sheets", max_attempts=4):
    """Execute a Sheets API call with retry + back-off."""
    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except Exception as e:
            err = str(e).lower()
            wait = 30 * attempt if ("quota" in err or "rate" in err) else 5 * attempt
            log.warning(f"[{label}] attempt {attempt}/{max_attempts} failed: {e}. Retrying in {wait}s…")
            if attempt == max_attempts:
                raise
            time.sleep(wait)


def ensure_sheet_exists(svc, title: str) -> bool:
    """Create sheet tab if it doesn't exist.  Returns True if created."""
    meta = svc.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
    existing = [s["properties"]["title"] for s in meta.get("sheets", [])]
    if title not in existing:
        svc.spreadsheets().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"requests": [{"addSheet": {"properties": {"title": title}}}]},
        ).execute()
        return True
    return False


def read_sheet_values(svc, range_: str):
    """Read values from a sheet range."""
    try:
        result = svc.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, range=range_,
        ).execute()
        return result.get("values", [])
    except Exception:
        return []


def clear_sheet(svc, title: str):
    """Clear all data from a sheet tab."""
    try:
        svc.spreadsheets().values().clear(
            spreadsheetId=SPREADSHEET_ID, range=f"'{title}'!A:Z",
        ).execute()
    except Exception as e:
        log.warning(f"clear_sheet({title}) error: {e}")


def clear_all_sheets(svc):
    """Delete all sheets except Sheet1 (which gets cleared instead)."""
    log.info("🗑️  Clearing ALL sheets for full refresh…")
    try:
        meta = svc.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
        sheets = meta.get("sheets", [])

        # Keep one sheet to avoid error (spreadsheet must have ≥1 sheet)
        # Ensure Sheet1 exists first
        titles = [s["properties"]["title"] for s in sheets]
        if SHEET1_NAME not in titles:
            svc.spreadsheets().batchUpdate(
                spreadsheetId=SPREADSHEET_ID,
                body={"requests": [{"addSheet": {"properties": {"title": SHEET1_NAME}}}]},
            ).execute()
            # Re-fetch metadata
            meta = svc.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
            sheets = meta.get("sheets", [])

        # Delete all sheets except Sheet1
        deletes = []
        for s in sheets:
            title = s["properties"]["title"]
            if title != SHEET1_NAME:
                deletes.append({"deleteSheet": {"sheetId": s["properties"]["sheetId"]}})

        if deletes:
            svc.spreadsheets().batchUpdate(
                spreadsheetId=SPREADSHEET_ID, body={"requests": deletes},
            ).execute()

        # Clear Sheet1
        clear_sheet(svc, SHEET1_NAME)
        log.info(f"   Deleted {len(deletes)} sheet(s), cleared {SHEET1_NAME}")
    except Exception as e:
        log.error(f"clear_all_sheets error: {e}", exc_info=True)


def write_sheet(svc, title: str, rows: list[list], append: bool = False):
    """Write (or append) rows to a sheet.  Creates sheet if missing."""
    ensure_sheet_exists(svc, title)
    if append:
        _sheets_retry(
            lambda: svc.spreadsheets().values().append(
                spreadsheetId=SPREADSHEET_ID,
                range=f"'{title}'!A:Z",
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": rows},
            ).execute(),
            label=f"append/{title}",
        )
    else:
        clear_sheet(svc, title)
        _sheets_retry(
            lambda: svc.spreadsheets().values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f"'{title}'!A1",
                valueInputOption="RAW",
                body={"values": rows},
            ).execute(),
            label=f"write/{title}",
        )


# ══════════════════════════════════════════════════════════════════════
#  HTTP helper with retry / back-off
# ══════════════════════════════════════════════════════════════════════

def _get(url: str, params: dict | None = None, label: str = "req",
         max_attempts: int = MAX_RETRIES) -> requests.Response | None:
    """GET with exponential back-off, 429 handling, and timeout."""
    for attempt in range(1, max_attempts + 1):
        try:
            resp = requests.get(
                url, params=params, auth=AUTH,
                headers={"Accept": "application/json"}, timeout=60,
            )
            if resp.status_code == 200:
                return resp
            if resp.status_code in (401, 403):
                log.error(f"[{label}] Auth error {resp.status_code}: {resp.text[:300]}")
                return None
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                wait = min(int(retry_after) if retry_after else 60 * attempt, 300)
                log.warning(f"[{label}] 429 rate-limited. Waiting {wait}s (attempt {attempt}/{max_attempts})…")
                time.sleep(wait)
                continue
            log.warning(f"[{label}] HTTP {resp.status_code}: {resp.text[:200]}")
            if attempt < max_attempts:
                time.sleep(2 ** attempt)
                continue
            return None
        except requests.exceptions.Timeout:
            log.warning(f"[{label}] Timeout (attempt {attempt}/{max_attempts})")
        except Exception as e:
            log.warning(f"[{label}] Exception (attempt {attempt}/{max_attempts}): {e}")
        time.sleep(2 ** attempt)
    log.error(f"[{label}] All {max_attempts} attempts failed.")
    return None


# ══════════════════════════════════════════════════════════════════════
#  1. Fetch all Ads / Creatives
# ══════════════════════════════════════════════════════════════════════

def fetch_all_ads() -> list[dict]:
    """
    GET /Advertisers/{SID}/Ads  — paginated.
    Returns list of ad dicts.
    """
    log.info("📦 Fetching all Ads / Creatives …")
    ads: list[dict] = []
    page = 1

    while page <= MAX_PAGES:
        resp = _get(
            f"{IMPACT_BASE}/Advertisers/{ACCOUNT_SID}/Ads",
            params={"Page": page, "PageSize": PAGE_SIZE},
            label=f"ads/p{page}",
        )
        if not resp:
            break

        data = resp.json()
        records = data.get("Ads", []) or data.get("Records", []) or data.get("ads", [])
        if not records:
            break

        for ad in records:
            start_dt = (ad.get("LimitedTimeStartDate", "") or "").split("T")[0]
            end_dt = (ad.get("LimitedTimeEndDate", "") or "").split("T")[0]
            ads.append({
                "Id":           str(ad.get("Id", "")),
                "Name":         ad.get("Name", ad.get("Title", "Unknown")),
                "CampaignId":   str(ad.get("CampaignId", "")),
                "CampaignName": ad.get("CampaignName", ad.get("ProgramName", "")),
                "Status":       ad.get("Status", ad.get("State", "Unknown")),
                "Type":         ad.get("Type", ad.get("AdType", "Unknown")),
                "LandingPage":  ad.get("LandingPage", ""),
                "StartDate":    start_dt,
                "EndDate":      end_dt,
            })

        if not data.get("@nextpageuri") and len(records) < PAGE_SIZE:
            break
        page += 1
        time.sleep(RATE_LIMIT_DELAY)

    log.info(f"   Found {len(ads)} ads across {page} page(s).")
    return ads


# ══════════════════════════════════════════════════════════════════════
#  2. Fetch Actions per campaign
# ══════════════════════════════════════════════════════════════════════

def _date_chunks(start: datetime, end: datetime) -> list[tuple[str, str]]:
    """Split [start, end) into ≤ CHUNK_DAYS ISO-8601 windows."""
    chunks = []
    cur = start.date() if isinstance(start, datetime) else start
    end_d = end.date() if isinstance(end, datetime) else end
    while cur < end_d:
        nxt = min(cur + timedelta(days=CHUNK_DAYS), end_d)
        chunks.append((
            f"{cur.isoformat()}T00:00:00-08:00",
            f"{nxt.isoformat()}T00:00:00-08:00",
        ))
        cur = nxt + timedelta(days=1)
    return chunks


def fetch_campaign_actions(campaign_id: str, start_date: datetime, end_date: datetime) -> list[dict]:
    """
    Fetch ALL Actions for a campaign across the date range.
    The Impact Actions API requires CampaignId and ≤ 45-day windows.
    """
    all_actions: list[dict] = []
    chunks = _date_chunks(start_date, end_date)

    for chunk_start, chunk_end in chunks:
        page = 1
        while page <= MAX_PAGES:
            params = {
                "CampaignId": campaign_id,
                "ActionDateStart": chunk_start,
                "ActionDateEnd": chunk_end,
                "Page": page,
                "PageSize": PAGE_SIZE,
            }
            resp = _get(
                f"{IMPACT_BASE}/Advertisers/{ACCOUNT_SID}/Actions",
                params=params,
                label=f"actions/cmp{campaign_id}/p{page}",
            )
            if not resp:
                break

            data = resp.json()
            actions = data.get("Actions", []) or data.get("Records", [])
            if not actions:
                break

            all_actions.extend(actions)

            if not data.get("@nextpageuri") and len(actions) < PAGE_SIZE:
                break
            page += 1
            time.sleep(RATE_LIMIT_DELAY)

    return all_actions


# ══════════════════════════════════════════════════════════════════════
#  3. Fetch Clicks via ClickExport (job-based)
# ══════════════════════════════════════════════════════════════════════

def fetch_clicks_for_campaign(campaign_id: str, start_date: str, end_date: str) -> dict:
    """
    Submit a ClickExport job, poll for completion, download CSV.
    Returns {(ad_id, partner_name, date): click_count}.
    """
    clicks: dict[tuple, int] = defaultdict(int)

    # Submit the export job — single attempt, skip if rate-limited
    url = f"{IMPACT_BASE}/Advertisers/{ACCOUNT_SID}/Programs/{campaign_id}/ClickExport"
    try:
        resp = requests.get(url, params={"DateStart": start_date, "DateEnd": end_date},
                            auth=AUTH, headers={"Accept": "application/json"}, timeout=30)
        if resp.status_code == 429:
            log.info(f"   ClickExport rate-limited — skipping clicks for campaign {campaign_id}")
            return clicks
        if resp.status_code != 200:
            log.warning(f"   ClickExport submit {resp.status_code} for campaign {campaign_id}")
            return clicks
    except Exception as e:
        log.warning(f"   ClickExport submit failed for campaign {campaign_id}: {e}")
        return clicks

    queued_uri = resp.json().get("QueuedUri", "")
    if "/Jobs/" not in queued_uri:
        log.warning(f"   No job ID in ClickExport response for campaign {campaign_id}")
        return clicks

    job_id = queued_uri.split("/Jobs/")[1].split("/")[0]
    log.info(f"   ClickExport job {job_id} submitted — polling…")

    # Poll for completion
    job_url = f"{IMPACT_BASE}/Advertisers/{ACCOUNT_SID}/Jobs/{job_id}"
    completed = False
    for poll in range(CLICK_MAX_POLLS):
        time.sleep(CLICK_POLL_INTERVAL)
        resp = _get(job_url, max_attempts=2, label=f"job_poll/{job_id}")
        if not resp:
            continue
        status = resp.json().get("Status", "UNKNOWN")
        if status == "COMPLETED":
            completed = True
            break
        if status in ("FAILED", "CANCELLED"):
            log.warning(f"   ClickExport job {status}")
            return clicks

    if not completed:
        log.warning(f"   ClickExport job timed out for campaign {campaign_id}")
        return clicks

    # Download CSV
    dl_resp = requests.get(
        f"{IMPACT_BASE}/Advertisers/{ACCOUNT_SID}/Jobs/{job_id}/Download",
        auth=AUTH, timeout=120,
    )
    if dl_resp.status_code != 200:
        log.warning(f"   ClickExport download failed: {dl_resp.status_code}")
        return clicks

    rows = list(csv.DictReader(io.StringIO(dl_resp.text)))
    log.info(f"   ClickExport: {len(rows)} click rows downloaded")

    for row in rows:
        event_date = (row.get("EventDate", "") or "").split("T")[0]
        ad_id = str(row.get("AdId", row.get("CreativeId", "")))
        partner_name = row.get("MediaName", row.get("MediaPartnerName", "Unknown"))
        if ad_id and event_date:
            clicks[(ad_id, partner_name, event_date)] += 1

    return clicks


# ══════════════════════════════════════════════════════════════════════
#  4. Aggregate actions by Ad (includes ALL AdIds, not just known ads)
# ══════════════════════════════════════════════════════════════════════

def aggregate_actions_by_ad(
    raw_actions: list[dict],
    ads_lookup: dict[str, dict],
    campaign_name: str,
    click_data: dict[tuple, int] | None = None,
) -> dict[str, list[dict]]:
    """
    Aggregate metrics per (AdId, PartnerId, Date).
    Includes ALL AdIds found in actions — creates synthetic ad entries
    for AdIds not in ads_lookup.
    Returns {ad_id: [row_dict, ...]}.
    """
    buckets: dict[tuple, dict] = {}

    for act in raw_actions:
        ad_id = str(act.get("AdId", ""))
        if not ad_id:
            continue

        event_date = (act.get("EventDate", "") or "").split("T")[0]
        partner_id = str(act.get("MediaPartnerId", act.get("PartnerId", "")))
        partner_name = act.get("MediaPartnerName", act.get("PartnerName", "Unknown"))

        # Use ad name from known ads lookup, or fall back to campaign name
        if ad_id in ads_lookup:
            ad_name = ads_lookup[ad_id].get("Name", "Unknown")
        else:
            ad_name = f"Ad {ad_id} ({campaign_name})"

        key = (ad_id, partner_id, event_date)
        if key not in buckets:
            # Look up clicks for this (ad, partner, date)
            click_count = 0
            if click_data:
                click_count = click_data.get((ad_id, partner_name, event_date), 0)

            buckets[key] = {
                "partner_id":   partner_id,
                "partner_name": partner_name,
                "ad_id":        ad_id,
                "ad_name":      ad_name,
                "impressions":  0,
                "clicks":       click_count,
                "conversions":  0,
                "revenue":      0.0,
                "cost":         0.0,
                "date":         event_date,
            }

        row = buckets[key]
        row["conversions"] += 1
        row["revenue"] += float(act.get("Amount", 0) or 0)
        row["cost"] += float(act.get("Payout", 0) or 0)

    # Group by ad_id
    result: dict[str, list[dict]] = defaultdict(list)
    for row in buckets.values():
        result[row["ad_id"]].append(row)
    return dict(result)


# ══════════════════════════════════════════════════════════════════════
#  5. Data transformation / normalisation
# ══════════════════════════════════════════════════════════════════════

def normalise(rows: list[dict]) -> pd.DataFrame:
    """Convert list of row dicts → deduplicated, sorted DataFrame."""
    if not rows:
        return pd.DataFrame(columns=HEADER_ROW)

    df = pd.DataFrame(rows)
    df = df.rename(columns={
        "partner_id":   "Partner ID",
        "partner_name": "Partner Name",
        "ad_id":        "Ad ID",
        "ad_name":      "Ad Name",
        "impressions":  "Impressions",
        "clicks":       "Clicks",
        "conversions":  "Conversions",
        "revenue":      "Revenue",
        "cost":         "Cost",
        "date":         "Date",
    })
    df = df[HEADER_ROW]

    for col in ("Impressions", "Clicks", "Conversions"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    for col in ("Revenue", "Cost"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0).round(2)

    df = df.drop_duplicates(subset=["Partner ID", "Ad ID", "Date"], keep="last")
    df = df.sort_values("Date", ascending=True).reset_index(drop=True)
    return df


# ══════════════════════════════════════════════════════════════════════
#  6. Google Sheets write logic
# ══════════════════════════════════════════════════════════════════════

def _df_to_values(df: pd.DataFrame) -> list[list]:
    """Convert DataFrame rows to list-of-lists (strings) for Sheets API."""
    return df.astype(str).values.tolist()


def write_ad_sheet(svc, ad_id: str, df: pd.DataFrame, full_refresh: bool):
    """Write (or append) data for one Ad to its dedicated sheet tab."""
    title = f"Ad_{ad_id}"
    rows_with_header = [HEADER_ROW] + _df_to_values(df)

    if full_refresh:
        log.info(f"   ✏️  Full-write {title}: {len(df)} rows")
        write_sheet(svc, title, rows_with_header, append=False)
    else:
        # Incremental: read existing keys, append only new rows
        ensure_sheet_exists(svc, title)
        existing_data = read_sheet_values(svc, f"'{title}'!A:J")

        if len(existing_data) <= 1:
            log.info(f"   ✏️  Initial-write {title}: {len(df)} rows")
            write_sheet(svc, title, rows_with_header, append=False)
        else:
            existing_keys = set()
            for row in existing_data[1:]:
                if len(row) >= 10:
                    existing_keys.add((row[0], row[2], row[9]))

            new_rows = []
            for _, r in df.iterrows():
                key = (str(r["Partner ID"]), str(r["Ad ID"]), str(r["Date"]))
                if key not in existing_keys:
                    new_rows.append([str(r[c]) for c in HEADER_ROW])

            if new_rows:
                log.info(f"   ➕ Append {title}: {len(new_rows)} new rows (skipped {len(df) - len(new_rows)} dupes)")
                write_sheet(svc, title, new_rows, append=True)
            else:
                log.info(f"   ⏭️  {title}: 0 new rows (all duplicates)")


def write_sheet1_aggregate(svc, all_ads: list[dict], ad_metrics: dict[str, dict]):
    """
    Write Sheet1 with one row per Ad that has data,
    showing aggregate metrics.
    """
    rows = []
    for ad_id, m in sorted(ad_metrics.items(), key=lambda x: -x[1].get("revenue", 0)):
        ad_info = None
        for a in all_ads:
            if a["Id"] == ad_id:
                ad_info = a
                break

        partners = m.get("partners", 0)
        clicks = m.get("clicks", 0)
        actions = m.get("actions", 0)
        revenue = round(m.get("revenue", 0.0), 2)
        action_cost = round(m.get("cost", 0.0), 2)
        total_cost = action_cost
        cpc = round(total_cost / clicks, 2) if clicks > 0 else 0.0

        rows.append([
            ad_info["Name"] if ad_info else f"Ad {ad_id}",
            ad_id,
            ad_info.get("Type", "") if ad_info else "",
            ad_info.get("LandingPage", "") if ad_info else "",
            partners, clicks, actions, revenue,
            action_cost, total_cost, cpc,
            ad_info.get("StartDate", "") if ad_info else "",
            ad_info.get("EndDate", "") if ad_info else "",
        ])

    write_sheet(svc, SHEET1_NAME, [SHEET1_HEADER] + rows, append=False)
    log.info(f"   📊 Sheet1 updated: {len(rows)} ads with data (sorted by revenue desc)")
    if rows:
        # Log top 5 for verification
        for r in rows[:5]:
            log.info(f"      {r[0][:40]:40s} | Actions={r[6]:>4} | Rev=${r[7]:>10} | Cost=${r[8]:>10} | Partners={r[4]}")


def write_summary_sheet(svc, ads: list[dict], sync_time: str):
    """Write or update Summary sheet with ad list + last sync time."""
    header = [["Ad ID", "Ad Name", "Campaign", "Status", "Type", "Last Sync"]]
    rows = []
    for ad in ads:
        rows.append([
            ad["Id"], ad["Name"],
            ad.get("CampaignName", ""),
            ad.get("Status", ""),
            ad.get("Type", ""),
            sync_time,
        ])
    write_sheet(svc, SUMMARY_SHEET, header + rows, append=False)
    log.info(f"   📋 Summary sheet updated: {len(rows)} ads")


# ══════════════════════════════════════════════════════════════════════
#  7. Determine date range
# ══════════════════════════════════════════════════════════════════════

def determine_date_range(svc, full_refresh: bool) -> tuple[datetime, datetime, bool]:
    """Return (start, end, is_full_refresh)."""
    end = datetime.now(timezone.utc)

    if full_refresh:
        start = end - timedelta(days=FULL_HISTORY_DAYS)
        log.info(f"📅 Full refresh: {start.date()} → {end.date()}")
        return start, end, True

    try:
        values = read_sheet_values(svc, f"'{SUMMARY_SHEET}'!F2:F2")
        if values and values[0]:
            last_sync_str = values[0][0]
            last_sync = datetime.fromisoformat(last_sync_str.replace("Z", "+00:00"))
            start = last_sync - timedelta(hours=INCREMENTAL_HOURS)
            log.info(f"📅 Incremental: {start.date()} → {end.date()} (last sync: {last_sync_str})")
            return start, end, False
    except Exception as e:
        log.warning(f"Could not read last sync date: {e}")

    start = end - timedelta(days=FULL_HISTORY_DAYS)
    log.info(f"📅 First run (no previous sync). Full pull: {start.date()} → {end.date()}")
    return start, end, True


# ══════════════════════════════════════════════════════════════════════
#  8. Main pipeline
# ══════════════════════════════════════════════════════════════════════

def run_pipeline(full_refresh: bool = False) -> int:
    """
    Main entry point:
      1. Fetch all ads
      2. Group ads by campaign
      3. For each campaign → fetch actions + clicks → group by ad → write
      4. Write Sheet1 aggregate + Summary sheet
    """
    log.info("=" * 60)
    log.info(f"🚀 Ad Performance Sync | mode={'FULL' if full_refresh else 'INCREMENTAL'}")
    log.info("=" * 60)

    if not AUTH:
        raise RuntimeError("Cannot run: missing IMPACT_ACCOUNT_SID or IMPACT_AUTH_TOKEN.")

    sheets_svc = get_sheets_service()
    start_date, end_date, is_full = determine_date_range(sheets_svc, full_refresh)

    # ── Clear all sheets on full refresh ─────────────────────────────
    if is_full:
        clear_all_sheets(sheets_svc)

    # ── Step 1: fetch all ads ────────────────────────────────────────
    ads = fetch_all_ads()
    if not ads:
        log.warning("⚠️  No ads returned from Impact API. Nothing to sync.")
        return 0

    ads_lookup: dict[str, dict] = {ad["Id"]: ad for ad in ads}

    # ── Step 2: group ads by campaign ────────────────────────────────
    campaigns: dict[str, list[dict]] = defaultdict(list)
    for ad in ads:
        cid = ad["CampaignId"]
        if cid:
            campaigns[cid].append(ad)

    log.info(f"   {len(ads)} ads across {len(campaigns)} campaigns")

    # ── Step 3: fetch data per campaign ──────────────────────────────
    total_rows = 0
    errors = []
    ads_with_data = set()
    ad_metrics: dict[str, dict] = {}

    start_str = (start_date.date() if isinstance(start_date, datetime) else start_date).isoformat()
    end_str = (end_date.date() if isinstance(end_date, datetime) else end_date).isoformat()

    for i, (campaign_id, campaign_ads) in enumerate(campaigns.items(), 1):
        campaign_name = campaign_ads[0].get("CampaignName", campaign_id)
        log.info(f"── Campaign [{i}/{len(campaigns)}] {campaign_name} ({len(campaign_ads)} ads) ──")

        try:
            # Fetch all actions for this campaign
            raw_actions = fetch_campaign_actions(campaign_id, start_date, end_date)
            log.info(f"   Fetched {len(raw_actions)} raw actions")

            # Fetch clicks for this campaign (best-effort, non-blocking)
            click_data = {}
            try:
                click_data = fetch_clicks_for_campaign(campaign_id, start_str, end_str)
            except Exception as e:
                log.warning(f"   ClickExport failed for campaign {campaign_id}: {e}")

            if not raw_actions:
                log.info(f"   ⏭️  No actions for campaign {campaign_name}")
                continue

            # Group/aggregate by Ad — includes ALL AdIds from actions
            ad_data = aggregate_actions_by_ad(
                raw_actions, ads_lookup, campaign_name, click_data,
            )
            log.info(f"   {len(ad_data)} unique ads found in actions")

            # Write per-ad sheets + collect metrics
            for ad_id, ad_rows in ad_data.items():
                # Aggregate metrics for Sheet1
                partner_ids = set()
                total_actions = 0
                total_revenue = 0.0
                total_cost = 0.0
                total_clicks = 0
                for row in ad_rows:
                    partner_ids.add(row["partner_id"])
                    total_actions += row["conversions"]
                    total_revenue += row["revenue"]
                    total_cost += row["cost"]
                    total_clicks += row["clicks"]

                ad_metrics[ad_id] = {
                    "partners": len(partner_ids),
                    "clicks":   total_clicks,
                    "actions":  total_actions,
                    "revenue":  total_revenue,
                    "cost":     total_cost,
                }

                df = normalise(ad_rows)
                if df.empty:
                    continue

                try:
                    write_ad_sheet(sheets_svc, ad_id, df, full_refresh=is_full)
                    total_rows += len(df)
                    ads_with_data.add(ad_id)
                except Exception as e:
                    log.error(f"   ❌ Failed writing sheet for Ad {ad_id}: {e}", exc_info=True)
                    errors.append((ad_id, str(e)))

        except Exception as e:
            log.error(f"   ❌ Failed for campaign {campaign_id}: {e}", exc_info=True)
            for ad in campaign_ads:
                errors.append((ad["Id"], str(e)))
            continue

    # ── Step 4: write Sheet1 + Summary ───────────────────────────────
    try:
        write_sheet1_aggregate(sheets_svc, ads, ad_metrics)
    except Exception as e:
        log.error(f"Failed to write Sheet1: {e}", exc_info=True)

    sync_time = datetime.now(timezone.utc).isoformat()
    try:
        write_summary_sheet(sheets_svc, ads, sync_time)
    except Exception as e:
        log.error(f"Failed to write Summary sheet: {e}", exc_info=True)

    # ── Done ─────────────────────────────────────────────────────────
    log.info("=" * 60)
    if errors:
        log.warning(f"⚠️  {len(errors)} ad(s) had errors: {[e[0] for e in errors]}")
    log.info(f"🎉 Sync complete — {total_rows} rows | {len(ads_with_data)} ad sheets | {len(ad_metrics)} ads with metrics")
    log.info("=" * 60)
    return total_rows


# ══════════════════════════════════════════════════════════════════════
#  CLI entry point
# ══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Impact.com Ad Performance → Google Sheets sync")
    parser.add_argument("--full", action="store_true", help="Force full 90-day refresh")
    parser.add_argument("--no-server", action="store_true", help="Run headless (no Flask)")
    args = parser.parse_args()

    try:
        rows = run_pipeline(full_refresh=args.full)
        log.info(f"Exit 0 — {rows} rows synced.")
        sys.exit(0)
    except Exception as e:
        log.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
