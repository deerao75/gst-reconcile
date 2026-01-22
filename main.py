import io
import re
import os
from dotenv import load_dotenv
load_dotenv(override=False)  # ensures both Flask and RQ worker see .env
import math
from datetime import datetime
from typing import Optional, Dict, List, Tuple
from collections import defaultdict
from flask import Flask, render_template, request, send_file, flash, redirect, url_for, session, jsonify, after_this_request
import pandas as pd
import secrets
# ------- NEW: Queue & Google Drive imports -------
from redis import Redis
from rq import Queue, get_current_job
from rq.job import Job
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
# NEW: add these imports near the other top-level imports in main.py
import sqlalchemy as sa
from sqlalchemy import inspect
from datetime import date, timedelta
from flask_mail import Mail, Message
from itsdangerous import URLSafeTimedSerializer


# -------------------- Flask & App Config --------------------
app = Flask(__name__)
# Use SECRET_KEY from environment (Render), fallback for local dev
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-key-for-local-use-only")
# File upload limit
app.config['MAX_CONTENT_LENGTH'] = 25 * 1024 * 1024  # 25 MB

# Email Configuration (Update with your real details)
app.config['MAIL_SERVER'] = 'smtp.gmail.com'
app.config['MAIL_PORT'] = 587
app.config['MAIL_USE_TLS'] = True
app.config['MAIL_USERNAME'] = 'info@acertax.com'  # REPLACE THIS
app.config['MAIL_PASSWORD'] = 'jfqz ajoy ehiv uixg'   # REPLACE THIS (App Password)
app.config['MAIL_DEFAULT_SENDER'] = 'info@acertax.com' # REPLACE THIS

mail = Mail(app)
s = URLSafeTimedSerializer(app.config['SECRET_KEY'])
# -------------------------

# Database Configuration
database_url = os.environ.get("DATABASE_URL")
database_path = os.environ. get("DATABASE_PATH")

if database_url:
    # PostgreSQL (e. g., on cloud platforms)
    app.config["SQLALCHEMY_DATABASE_URI"] = database_url.replace("postgres://", "postgresql://")
elif database_path:
    # Docker environment with shared volume
    os. makedirs(os.path. dirname(database_path), exist_ok=True)
    app.config["SQLALCHEMY_DATABASE_URI"] = f"sqlite:///{database_path}"
else:
    # Local development or e2enetworks (no Docker)
    render_disk_path = "/mnt/data"
    if os.path. exists(render_disk_path):
        db_path = os. path.join(render_disk_path, "users.db")
    else:
        db_path = "users.db"
    app.config["SQLALCHEMY_DATABASE_URI"] = f"sqlite:///{db_path}"

app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

# ------- Initialize SQLAlchemy (for auth) -------
from flask_sqlalchemy import SQLAlchemy
db = SQLAlchemy(app)

# Ensure all tables exist on startup
with app.app_context():
    db.create_all()

class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(120), unique=True, nullable=False)
    password_hash = db.Column(db.String(200), nullable=False)
    subscribed = db.Column(db.Boolean, default=False)
    # Add subscription expiry date column (can be NULL)
    subscription_expiry_date = db.Column(db.Date, nullable=True)
    is_admin = db.Column(db.Boolean, default=False, nullable=False)
    def __repr__(self):
        return f'<User {self.email}>'

import threading
_tables_created = False
_tables_lock = threading.Lock()

class ReconciliationReport(db. Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db. Integer, db.ForeignKey('user.id'), nullable=False)

    # Metadata
    report_name = db. Column(db.String(200))
    generated_at = db.Column(db.DateTime, default=datetime.utcnow)

    # Summary Stats
    total_matched_value = db.Column(db.Float, default=0.0)
    total_mismatched_value = db.Column(db.Float, default=0.0)
    row_count = db.Column(db.Integer, default=0)

    # Link to file
    drive_file_id = db.Column(db.String(100), nullable=False)
    status = db.Column(db.String(20), default="completed")

    user = db.relationship('User', backref=db.backref('reports', lazy=True))

# --- DB Helper Update ---
# Find your 'ensure_user_schema' function or 'create_tables_once'.
# Ensure db.create_all() is called to generate this new table.

@app.before_request
def clear_stale_session_if_user_missing():
    # If browser has a session that claims to be logged_in but the user record
    # does not exist in the database, clear the session. Prevents "ghost" login.
    if session.get('logged_in') and session.get('email'):
        try:
            u = User.query.filter_by(email=session.get('email')).first()
        except Exception:
            u = None
        if not u:
            # remove any stale session cookies / keys
            session.clear()

# REPLACE your existing create_tables_once() with this version
_tables_created = False
_tables_lock = threading.Lock()

@app.before_request
def create_tables_once():
    global _tables_created
    if not _tables_created:
        with _tables_lock:
            if not _tables_created:
                # Create missing tables (will not alter existing ones)
                db.create_all()
                # Try to ensure the new column exists in the users table
                try:
                    ensure_user_schema()
                except Exception:
                    # ensure_user_schema logs internally
                    pass
                _tables_created = True

# ------- NEW: Redis Queue (for background jobs) -------
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
rconn = Redis.from_url(REDIS_URL)
q = Queue("reconcile", connection=rconn)

# ------- NEW: Google Drive service (service account) -------
SCOPES = ['https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_FILE = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', 'cred.json')
DRIVE_FOLDER_ID = os.environ.get('DRIVE_FOLDER_ID', None)


def get_credentials():
    """Creates credentials from the service account file."""
    return service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )

def _drive_service():
    """Get the Drive service resource with retries."""
    creds = get_credentials()

    for attempt in range(1, 4):
        try:
            # Try to build the service (this refreshes the token if needed)
            service = build('drive', 'v3', credentials=creds)
            return service
        except Exception as e:
            print(f"[Error] Failed to connect to Drive API (Attempt {attempt}/3). Retrying in 2s... Error: {e}")
            time.sleep(2)

    raise Exception("Failed to connect to Google Drive API after 3 attempts.")

import time # Add this at the top of main.py if missing

def upload_to_drive(local_path: str, filename: str) -> str:
    service = _drive_service()
    metadata = {'name': filename}
    if DRIVE_FOLDER_ID:
        metadata['parents'] = [DRIVE_FOLDER_ID]

    # Retry up to 3 times
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # Re-create the media object for each attempt to reset the stream
            media = MediaFileUpload(local_path, resumable=True)

            file = service.files().create(
                body=metadata,
                media_body=media,
                fields='id',
                supportsAllDrives=True
            ).execute()

            return file.get('id')

        except Exception as e:
            app.logger.warning(f"Upload failed (Attempt {attempt + 1}/{max_retries}): {e}")
            if attempt == max_retries - 1:
                # If this was the last attempt, raise the error to fail the job
                raise e
            # Wait 5 seconds before trying again
            time.sleep(5)

def download_from_drive(file_id, destination_path):
    """Downloads a file from Google Drive with 3 retries on failure."""
    creds = get_credentials()
    service = build('drive', 'v3', credentials=creds)
    request = service.files().get_media(fileId=file_id)

    last_exception = None

    for attempt in range(1, 4):
        try:
            fh = io.FileIO(destination_path, 'wb')
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
                # Optional: print(f"Download {int(status.progress() * 100)}%.")

            # If we get here, it worked!
            fh.close()
            return

        except Exception as e:
            last_exception = e
            print(f"[Error] Download failed (Attempt {attempt}/3). Retrying in 5s... Error: {e}")
            time.sleep(5)

    # If we exit the loop, all 3 attempts failed
    print(f"[Fatal] Could not download file {file_id} after 3 attempts.")
    raise last_exception

# -------------------- Column candidates --------------------
# GSTR-2B (flattened) candidates
INVOICE_CANDIDATES_2B = [
    "invoice details invoice number", "invoice number", "invoice no", "inv no", "inv number",
    "invoice", "invoice details inv no", "invoice details document number",
    "note no", "debit note no", "credit note no", "note number", "doc number", "doc no", "document number", "document no."
]
# Generic GSTIN (B2B)
GSTIN_CANDIDATES_2B = [
    "gstin of supplier", "gstin", "gst no", "gst number", "gstn", "supplier gstin"
]
# Stronger GSTIN for CDNR (explicit 2B wording first)
GSTIN_CANDIDATES_CDNR = [
    "gstin of supplier", "gstin", "gst no", "gst number", "gstin number"
]
DATE_CANDIDATES_2B = [
    "invoice details invoice date", "invoice date", "doc date", "document date", "date"
]
# NEW: explicit note number/date/type candidates for CDNR
NOTE_NO_CANDIDATES_2B = [
    "note no", "note number", "credit note no", "debit note no", "cn no", "dn no", "document number", "document no."
]
NOTE_DATE_CANDIDATES_2B = [
    "note date", "document date", "doc date", "credit note date", "debit note date"
]
NOTE_TYPE_CANDIDATES_2B = [
    "note type", "credit/debit note", "cr/dr", "type of note", "cd note type", "cr/dr note"
]

INV_TYPE_CANDIDATES = [
    "document type", "doc type", "invoice type", "inv type", "type", "type of transaction"
]

CGST_CANDIDATES_2B = ["cgst", "central tax", "central tax amount", "cgst amount"]
SGST_CANDIDATES_2B = ["sgst", "state tax", "state/ut tax", "state tax amount", "sgst amount", "utgst", "utgst amount"]
IGST_CANDIDATES_2B = ["igst", "integrated tax", "integrated tax amount", "igst amount"]
TAXABLE_CANDIDATES_2B = ["taxable value", "taxable amount", "assessable value", "taxable"]
TOTAL_TAX_CANDIDATES_2B = ["total tax", "total tax amount", "tax amount"]
INVOICE_VALUE_CANDIDATES_2B = ["invoice value", "total invoice value", "value of invoice", "invoice total"]
CESS_CANDIDATES_2B = ["cess", "cess amount"]

# Purchase Register candidates (prefer vendor invoice notions)
INVOICE_CANDIDATES_PR = [
    "vendor inv no", "vendor invoice no", "vendor invoice number",
    "supplier inv no", "supplier invoice no", "supplier invoice number",
    "party invoice no", "party inv no", "bill no", "bill number", "doc no", "doc number", "document number","document no."
    # generic fallbacks
    "invoice number", "invoice no", "inv no", "inv number", "invoice",
    # allow common accounting label
    "doc no"
]
GSTIN_CANDIDATES_PR = [
    # strong preference for supplier/vendor
    "supplier gstin", "vendor gstin", "party gstin", "gstin of supplier",
    # fallbacks
    "gstin", "gst no", "gst number", "gstn"
]
DATE_CANDIDATES_PR = [
    "invoice date", "vendor invoice date", "supplier invoice date", "bill date", "doc date", "document date", "date"
]
CGST_CANDIDATES_PR = ["cgst", "cgst amount", "central tax", "central tax amount", "cgst tax"]
SGST_CANDIDATES_PR = ["sgst", "sgst amount", "state tax", "state tax amount", "sgst tax"]
IGST_CANDIDATES_PR = ["igst", "igst amount", "integrated tax", "integrated tax amount", "igst tax"]
TAXABLE_CANDIDATES_PR = ["taxable value", "taxable amount", "assessable value", "taxable"]
TOTAL_TAX_CANDIDATES_PR = ["total tax", "total tax amount", "tax amount"]
INVOICE_VALUE_CANDIDATES_PR = ["invoice value", "total invoice value", "value of invoice", "invoice total"]
CESS_CANDIDATES_PR = ["cess", "cess amount"]

# Avoid confusing PR invoice with doc/voucher and PR GSTIN with recipient/company
AVOID_DOC_LIKE_FOR_PR = ["voucher no", "voucher number"]
AVOID_RECIPIENT_GSTIN_FOR_PR = ["our gstin", "recipient gstin", "buyer gstin"]

# keep-only extras
VENDOR_NAME_PR_CANDIDATES = [
    "vendor name", "supplier name", "party name", "name of supplier", "vendor", "supplier"
]

TRADE_NAME_2B_CANDIDATES = [
    "trade or legal name", "trade/legal name", "trade name", "legal name",
    "supplier trade name", "supplier legal name", "recipient trade name"
]

GSTR1_STATUS_2B_CANDIDATES = [
    "gstr-1 filing status", "gstr1 filing status", "filing status", "filing status details",
    "gstr1 status", "gstr-1 status", "status", "tax period", "return period", "period"
]

# -------------------- Column detection helpers --------------------
def _norm(s: str) -> str:
    return re.sub(r'[^a-z0-9]', '', str(s).strip().lower())

def _softnorm(s: str) -> str:
    if s is None:
        return ""
    s = str(s).replace("\xa0", " ")
    s = re.sub(r'\s+', ' ', s).strip()
    return s.lower()

def normalize_columns(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, str]]:
    original_cols = list(df.columns)
    norm_to_original: Dict[str, str] = {}
    cleaned = []
    for c in original_cols:
        norm = _softnorm(c)
        if norm not in norm_to_original:
            norm_to_original[norm] = str(c)
        cleaned.append(str(c).replace("\xa0", " ").strip())
    df = df.copy()
    df.columns = cleaned
    return df, norm_to_original

def _score_columns(df: pd.DataFrame, candidates: List[str], avoid_terms: Optional[List[str]] = None,
                   extra_penalties: Optional[List[str]] = None):
    avoid_terms = avoid_terms or []
    extra_penalties = extra_penalties or []
    cand_norms = [_norm(c) for c in candidates]
    avoid_norms = [_norm(a) for a in avoid_terms]
    penalty_norms = [_norm(a) for a in extra_penalties]
    scores = []
    for col in df.columns:
        n = _norm(col)
        score = 0
        if any(n == cn for cn in cand_norms): score += 5
        if any(cn in n for cn in cand_norms): score += 3
        if any(n.startswith(cn) for cn in cand_norms): score += 2
        if "inv" in n: score += 1
        if "gst" in n: score += 1
        if any(an in n for an in avoid_norms): score -= 4
        if any(pn in n for pn in penalty_norms): score -= 3
        scores.append((score, col))
    return scores

def _pick_column(df: pd.DataFrame, candidates: List[str], avoid_terms: Optional[List[str]] = None,
                 extra_penalties: Optional[List[str]] = None) -> Optional[str]:
    scores = _score_columns(df, candidates, avoid_terms, extra_penalties)
    best = max(scores, key=lambda x: x[0]) if scores else None
    return best[1] if best and best[0] > 0 else None

def flatten_columns(cols) -> List[str]:
    if isinstance(cols, pd.MultiIndex):
        out = []
        for tup in cols:
            parts = []
            for x in tup:
                if x is None:
                    continue
                s = str(x).replace("\xa0", " ").strip()
                if not s or s.lower().startswith("unnamed:") or s.lower() == "nan":
                    continue
                parts.append(s)
            out.append(" ".join(parts))
        return out

    # --- START: THIS IS THE FIX ---
    # When it's not a MultiIndex, it's a regular Index.
    # We iterate through it directly and clean up each column name.
    return [str(c).replace("\xa0", " ").strip() for c in cols]
    # --- END: THIS IS THE FIX ---

def _find_optional_col(df: pd.DataFrame, pools: List[List[str]]) -> Optional[str]:
    for cands in pools:
        col = _pick_column(df, cands)
        if col:
            return col
    return None

# ---------- Value-based detectors (safety net) ----------
GSTIN_REGEX = re.compile(r"^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z][A-Z0-9]Z[A-Z0-9]$", re.IGNORECASE)

def _looks_like_gstin_series(s: pd.Series) -> float:
    if s is None: return 0.0
    vals = s.dropna().astype(str).str.strip().head(500)
    if vals.empty: return 0.0
    hits = vals.str.fullmatch(GSTIN_REGEX).sum()
    return hits / max(1, len(vals))

def _looks_like_invoice_series(s: pd.Series) -> float:
    if s is None: return 0.0
    vals = s.dropna().astype(str).str.strip().head(500)
    if vals.empty: return 0.0
    def is_inv(x: str) -> bool:
        if GSTIN_REGEX.fullmatch(x): return False
        if len(x) < 1 or len(x) > 30: return False
        if re.fullmatch(r"[A-Za-z]+", x): return False
        if re.fullmatch(r"0+", x): return False
        if re.fullmatch(r"[-/\.]+", x): return False
        if not re.search(r"\d", x): return False
        return True
    hits = sum(1 for x in vals if is_inv(x))
    return hits / max(1, len(vals))

def pick_gstin_by_values(df: pd.DataFrame, prefer_supplier: bool = False) -> Optional[str]:
    best = (0.0, None)
    for col in df.columns:
        score = _looks_like_gstin_series(df[col])
        if score <= 0: continue
        name = _norm(col)
        if prefer_supplier and any(k in name for k in ["supplier", "vendor", "party"]):
            score += 0.2
        if any(k in name for k in ["company", "recipient", "customer", "buyer", "our"]):
            score -= 0.2
        if score > best[0]:
            best = (score, col)
    return best[1]

def pick_invoice_by_values(df: pd.DataFrame) -> Optional[str]:
    best = (0.0, None)
    for col in df.columns:
        n = _norm(col)
        if any(k in n for k in ["gstin", "gst", "tax", "cgst", "sgst", "igst", "amount", "value", "taxable",
                                 "company", "recipient", "buyer", "customer", "date", "period"]):
            continue
        if any(k in n for k in ["document", "docnumber", "voucher"]):
            pass
        score = _looks_like_invoice_series(df[col])
        if score > best[0]:
            best = (score, col)
    return best[1]

# -------------------- Normalization of cell values --------------------
def as_text(x) -> str:
    if x is None or (isinstance(x, float) and math.isnan(x)): return ""
    if isinstance(x, int): return str(x).strip()
    if isinstance(x, float):
        if x.is_integer(): return str(int(x)).strip()
        s = f"{x}".strip()
        return re.sub(r'\.0+$', '', s)
    s = str(x).replace("\xa0", " ").strip()
    return re.sub(r'\.0+$', '', s.replace(",", ""))

def clean_gstin(value) -> str:
    return re.sub(r'\s+', '', as_text(value).upper())

# --- Improved invoice normalizer (handles FY suffix/prefix like "699/25-26", "FY25-26/699") ---
_FY_TAIL = re.compile(r"([/\-]20?\d{2}[ \-\/]?20?\d{2}|[/\-]\d{2}[/\-]\d{2}|[/\-]\d{2}[\-]\d{2})$", re.I)
_FY_HEAD = re.compile(r"^(?:FY|FY-)?20?\d{2}[ \-\/]?20?\d{2}[/\-]", re.I)

def inv_basic(s) -> str:
    v = as_text(s).upper()
    v = re.sub(r'\s+', '', v)
    # remove common FY prefix like "FY25-26/" or "2025-26/"

    v = _FY_HEAD.sub('', v)
    # remove trailing FY chunk like "/25-26" or "/2025-26"
    v = _FY_TAIL.sub('', v)
    if re.search(r'\d', v):
        v = re.sub(r'^0+(?=[A-Z0-9])', '', v)
    return v

def _parse_excel_serial(s: str):
    try:
        return pd.to_datetime(float(s), origin='1899-12-30', unit='D', errors='coerce')
    except Exception:
        return pd.NaT

def parse_date_cell(x) -> Optional[datetime.date]:
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return None
    s = str(x).strip()
    if not s:
        return None

    # Handle datetime/Timestamp objects first
    if isinstance(x, (pd.Timestamp, datetime)):
        try:
            return pd.to_datetime(x).date()
        except Exception:
            pass

    # Try DD-MM-YYYY and DD/MM/YYYY first (your preferred format)
    for fmt in ("%d-%m-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            continue

    # Handle Excel serial numbers (5-6 digit numbers)
    if re.fullmatch(r"\d{4,6}", s):
        try:
            dt = pd.to_datetime(float(s), origin='1899-12-30', unit='D', errors='coerce')
            if not pd.isna(dt):
                return dt.date()
        except Exception:
            pass

    # Fallback: try pandas with dayfirst=True
    try:
        dt = pd.to_datetime(s, dayfirst=True, errors='coerce')
        if not pd.isna(dt):
            return dt.date()
    except Exception:
        pass

    return None

def format_date_display(d) -> str:
    """Format date object as DD-MM-YYYY string for display"""
    if d is None or pd.isna(d):
        return ""
    if isinstance(d, str):
        s = d.strip()
        if not s:
            return ""
        # If already in DD-MM-YYYY format, return as-is
        if re.match(r'\d{2}-\d{2}-\d{4}', s):
            return s
        # Try to parse and reformat
        parsed = parse_date_cell(s)
        if parsed:
            return parsed.strftime("%d-%m-%Y")
        return s
    if isinstance(d, (datetime, pd.Timestamp)):
        return d.strftime("%d-%m-%Y")
    if hasattr(d, 'strftime'):  # datetime.date
        return d.strftime("%d-%m-%Y")
    return str(d)

def date_key_yyyymmdd(x) -> str:
    """Unambiguous key for comparing dates."""
    d = parse_date_cell(x)
    return d.strftime("%Y%m%d") if d else ""

def date_iso_yyyy_mm_dd(x) -> str:
    """Force ISO text output so Excel cannot reformat to MM-DD-YYYY."""
    d = parse_date_cell(x)
    return d.strftime("%Y-%m-%d") if d else ""

def parse_amount(x) -> float:
    s = as_text(x)
    if not s: return 0.0
    try:
        return round(float(s), 2)
    except Exception:
        s2 = re.sub(r'[^0-9\.\-]', '', s)
        try:
            return round(float(s2), 2)
        except Exception:
            return 0.0

def concat_key(gstin: str, inv: str) -> str:
    return f"{gstin}|{inv}"

def round_rupee(x) -> int:
    try:
        return int(round(parse_amount(x), 0))
    except Exception:
        return 0

# -------------------- Consolidation (SUMIF-style) --------------------
def consolidate_by_key(
    df: pd.DataFrame,
    gstin_col: str,
    inv_col: str,
    date_col: Optional[str],
    numeric_cols: List[str],
    text_cols: Optional[List[str]] = None
) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    text_cols = text_cols or []
    work = df.copy()

    # --- NEW: Remove Bottom "Total" Rows ---
    # Strategy: identifying rows where the GSTIN or Invoice Number column
    # explicitly says "Total" (common in Excel exports).
    # We do NOT search the whole column, just check if the value is "Total"
    # to avoid false positives.

    def is_total_label(val):
        s = str(val).strip().lower()
        return s == "total" or s.startswith("total ")

    # If a row has "Total" in the GSTIN or Invoice Number column, it is likely a footer.
    # We filter these out before processing.
    mask_is_total = work[gstin_col].apply(is_total_label) | work[inv_col].apply(is_total_label)
    work = work[~mask_is_total].copy()

    # Additionally, verify numeric columns. If a row has text like "Total" in a
    # numeric column (which `parse_amount` would turn to 0), we should ensure
    # we aren't keeping a footer row that just happened to have a blank GSTIN.
    # (This step is often implicitly handled by the dropna/groupby, but being explicit helps).
    # --- END NEW ---

    work["_GST_KEY"] = work[gstin_col].map(clean_gstin)
    work[inv_col] = work[inv_col].map(as_text)
    work["_INV_KEY"] = work[inv_col].map(inv_basic)

    # Filter out rows where the keys became empty after cleaning (e.g. empty rows or pure text rows)
    work = work[work["_GST_KEY"] != ""].copy()

    for c in numeric_cols:
        if c in work.columns:
            work[c] = work[c].map(parse_amount)

    if date_col and date_col in work.columns:
        work["_DATE_TMP"] = work[date_col].map(parse_date_cell)
    else:
        work["_DATE_TMP"] = None

    agg_dict = {}
    for c in numeric_cols:
        if c in work.columns: agg_dict[c] = "sum"

    for c in text_cols:
        if c in work.columns: agg_dict[c] = "first"

    agg_dict["_DATE_TMP"] = "min"
    agg_dict[gstin_col] = "first"
    agg_dict[inv_col] = "first"

    protected = set(["_GST_KEY", "_INV_KEY", "_DATE_TMP"] + [gstin_col, inv_col] + numeric_cols + text_cols)
    for c in work.columns:
        if c not in protected and c not in agg_dict:
            agg_dict[c] = "first"

    grouped = work.groupby(["_GST_KEY", "_INV_KEY"], dropna=False).agg(agg_dict).reset_index()
    if "_DATE_TMP" in grouped.columns:
        grouped[date_col or "Invoice Date (derived)"] = grouped["_DATE_TMP"]
        grouped.drop(columns=["_DATE_TMP"], inplace=True)
    return grouped
# -------------------- Reconciliation (with rounding + "Almost Match") --------------------
def build_lookup(df_2b: pd.DataFrame, inv_col_2b: str, gstin_col_2b: str) -> Dict[str, List[int]]:
    lookup: Dict[str, List[int]] = defaultdict(list)
    for idx, row in df_2b.iterrows():
        gst = clean_gstin(row.get(gstin_col_2b, ""))
        inv = inv_basic(row.get(inv_col_2b, ""))
        if gst and inv:
            lookup[concat_key(gst, inv)].append(idx)
    return lookup

def reconcile(
    df_pr: pd.DataFrame, df_2b: pd.DataFrame,
    inv_col_pr: str, gstin_col_pr: str, date_col_pr: Optional[str], cgst_col_pr: Optional[str], sgst_col_pr: Optional[str], igst_col_pr: Optional[str],
    inv_col_2b: str, gstin_col_2b: str, date_col_2b: Optional[str], cgst_col_2b: Optional[str], sgst_col_2b: Optional[str], igst_col_2b: Optional[str],
) -> pd.DataFrame:
    b2_lookup = build_lookup(df_2b, inv_col_2b, gstin_col_2b)
    mappings, remarks, reasons = [], [], []

    for _, row in df_pr.iterrows():
        gst_pr = clean_gstin(row.get(gstin_col_pr, ""))
        inv_pr = inv_basic(row.get(inv_col_pr, ""))
        if not gst_pr or not inv_pr:
            mappings.append("Not Matched"); remarks.append("no GSTIN+Invoice in PR"); reasons.append("missing GSTIN/Invoice in PR"); continue

        key = concat_key(gst_pr, inv_pr)
        cand_idxs = b2_lookup.get(key, [])
        if not cand_idxs:
            mappings.append("Not Matched"); remarks.append("no GSTIN+Invoice match"); reasons.append(""); continue

        idx2b = cand_idxs[0]
        row2b = df_2b.iloc[idx2b]
        mismatches = []

        if date_col_pr and date_col_2b:
            d_pr = parse_date_cell(row.get(date_col_pr, ""))
            d_2b = parse_date_cell(row2b.get(date_col_2b, ""))
            if (d_pr or d_2b) and (d_pr != d_2b):
                mismatches.append("Invoice Date")

        def eq_amt_round_abs(a, b):
            return abs(round_rupee(a)) == abs(round_rupee(b))

        if cgst_col_pr and cgst_col_2b:
            if not eq_amt_round_abs(row.get(cgst_col_pr, 0), row2b.get(cgst_col_2b, 0)):
                mismatches.append("CGST")
        if sgst_col_pr and sgst_col_2b:
            if not eq_amt_round_abs(row.get(sgst_col_pr, 0), row2b.get(sgst_col_2b, 0)):
                mismatches.append("SGST")
        if igst_col_pr and igst_col_2b:
            if not eq_amt_round_abs(row.get(igst_col_pr, 0), row2b.get(igst_col_2b, 0)):
                mismatches.append("IGST")

        if mismatches:
            remarks.append("mismatch")
            extra = []
            if len(cand_idxs) > 1:
                extra.append(f"multiple matches in 2B ({len(cand_idxs)})")
            reasons.append("; ".join(mismatches + extra))
            mappings.append("Almost Matched")
        else:
            extra = []
            if len(cand_idxs) > 1:
                extra.append(f"multiple matches in 2B ({len(cand_idxs)})")
            remarks.append("All fields matched" + ("" if not extra else f" ({'; '.join(extra)})"))
            reasons.append("")
            mappings.append("Matched")

    out = df_pr.copy()
    out["Mapping"] = mappings
    out["Remarks"] = remarks
    out["Reason"] = reasons
    return out

# --- HELPER FUNCTIONS (Must be at Global Scope for RQ Worker) ---

def _pick_col_contains(df, pattern):
    """Helper to find a column matching a regex pattern."""
    if df is None: return None
    import re
    pat = re.compile(pattern, re.I)
    for c in df.columns:
        if pat.search(str(c)): return c
    return None

def _find_optional_col(df, pools):
    """Helper to find the first matching column from a list of candidate lists."""
    # Ensure _pick_column is available or defined
    for cands in pools:
        col = _pick_column(df, cands)
        if col: return col
    return None

# -------------------- Pairwise combined output --------------------
def build_pairwise_recon(
    df_pr: pd.DataFrame, df_2b: pd.DataFrame,
    inv_pr: str, gst_pr: str, date_pr: str, cgst_pr: str, sgst_pr: str, igst_pr: str,
    inv_2b: str, gst_2b: str, date_2b: str, cgst_2b: str, sgst_2b: str, igst_2b: str,
    inv_type_pr: Optional[str] = None, inv_type_2b: Optional[str] = None
):
    pr = df_pr.copy()
    b2 = df_2b.copy()

    for c in list(pr.columns):
        if c not in ["_GST_KEY", "_INV_KEY"]:
            pr.rename(columns={c: f"{c}_PR"}, inplace=True)

    for c in list(b2.columns):
        if c not in ["_GST_KEY", "_INV_KEY"]:
            b2.rename(columns={c: f"{c}_2B"}, inplace=True)

    merged = pd.merge(pr, b2, on=["_GST_KEY", "_INV_KEY"], how="outer")

    # --- NEW: Aggressive Orphan Rescue Logic ---
    # This detects matches where BOTH the GSTIN (typo) and Invoice (format) differ.
    # Example: 07AA...Z0 vs 07AA...ZO  AND  INV/25-26/01 vs INV/2526/01

    merged["_FUZZY_REASON"] = None

    inv_pr_col_chk = f"{inv_pr}_PR" if inv_pr else None
    inv_2b_col_chk = f"{inv_2b}_2B" if inv_2b else None
    b2_cols_all_chk = [c for c in merged.columns if c.endswith("_2B")]

    def fuzzy_gst(s):
        """Normalize GSTIN: 0->O, Z->2, I/L->1, B->8"""
        if not isinstance(s, str): return str(s) if s else ""
        # We map everything to a standard set to ensure "0" matches "O"
        return s.upper().replace('O', '0').replace('Z', '2').replace('I', '1').replace('L', '1').replace('B', '8')

    def fuzzy_inv(s):
        """Normalize Invoice: Remove all non-alphanumeric characters"""
        if not isinstance(s, str): return str(s) if s else ""
        # Strips /, -, space, dot to match "FY25-26" with "FY2526"
        return re.sub(r'[^A-Z0-9]', '', s.upper())

    # Map: (fuzzy_gst, fuzzy_inv) -> list of indices for 2B Orphans
    b2_orphan_map = defaultdict(list)

    # 1. Identify "Missing in PR" rows (2B Orphans) and store them
    for idx in merged.index:
        has_2b = pd.notna(merged.at[idx, inv_2b_col_chk]) if inv_2b_col_chk in merged.columns else False
        has_pr = pd.notna(merged.at[idx, inv_pr_col_chk]) if inv_pr_col_chk in merged.columns else False

        # If it's in 2B but not PR, it's a candidate for rescue
        if has_2b and not has_pr:
            g = merged.at[idx, "_GST_KEY"]
            i = merged.at[idx, "_INV_KEY"]
            # Create a simplified key
            key = (fuzzy_gst(g), fuzzy_inv(i))
            b2_orphan_map[key].append(idx)

    drop_indices = []

    # 2. Iterate "Missing in 2B" rows (PR Orphans) and look for a fuzzy match
    for idx in merged.index:
        has_2b = pd.notna(merged.at[idx, inv_2b_col_chk]) if inv_2b_col_chk in merged.columns else False
        has_pr = pd.notna(merged.at[idx, inv_pr_col_chk]) if inv_pr_col_chk in merged.columns else False

        # If it's in PR but not 2B
        if has_pr and not has_2b:
            g = merged.at[idx, "_GST_KEY"]
            i = merged.at[idx, "_INV_KEY"]
            key = (fuzzy_gst(g), fuzzy_inv(i))

            # Check if we have a 2B orphan with the same fuzzy key
            if key in b2_orphan_map:
                candidates = b2_orphan_map[key]
                if candidates:
                    # Match found! Take the first candidate.
                    match_idx = candidates.pop(0)
                    drop_indices.append(match_idx)

                    # Copy all 2B data columns from the matched 2B row to this PR row
                    for c in b2_cols_all_chk:
                        merged.at[idx, c] = merged.at[match_idx, c]

                    # Determine reason for fuzzy match
                    orig_2b_gst = merged.at[match_idx, "_GST_KEY"]
                    orig_2b_inv = merged.at[match_idx, "_INV_KEY"]

                    reasons = []
                    # Check GSTIN difference (ignoring fuzzy normalization)
                    if g != orig_2b_gst:
                        reasons.append("GSTIN")
                    # Check Invoice difference
                    if i != orig_2b_inv:
                        reasons.append("Invoice No")

                    if not reasons: reasons.append("Typos")

                    merged.at[idx, "_FUZZY_REASON"] = ", ".join(reasons)

    # 3. Clean up: Remove the 2B orphan rows that we just merged into PR rows
    if drop_indices:
        merged.drop(index=drop_indices, inplace=True)
        merged.reset_index(drop=True, inplace=True)
    # --- END NEW LOGIC ---

    pr_cols_all = [c for c in merged.columns if c.endswith("_PR")]
    b2_cols_all = [c for c in merged.columns if c.endswith("_2B")]

    if pr_cols_all:
        pr_orig_present = merged[pr_cols_all].fillna("").astype(str).apply(lambda col: col.str.strip() != "").any(axis=1)
    else:
        pr_orig_present = pd.Series(False, index=merged.index)

    key_grp = merged.groupby(["_GST_KEY", "_INV_KEY"], dropna=False)
    for col in pr_cols_all:
        merged[col] = key_grp[col].transform(lambda s: s.ffill().bfill())
    for col in b2_cols_all:
        merged[col] = key_grp[col].transform(lambda s: s.ffill().bfill())

    inv_pr_col = f"{inv_pr}_PR" if inv_pr else None
    gst_pr_col = f"{gst_pr}_PR" if gst_pr else None
    inv_2b_col = f"{inv_2b}_2B" if inv_2b else None
    gst_2b_col = f"{gst_2b}_2B" if gst_2b else None
    date_pr_col = f"{date_pr}_PR" if date_pr else None
    date_2b_col = f"{date_2b}_2B" if date_2b else None
    cgst_pr_col = f"{cgst_pr}_PR" if cgst_pr else None
    cgst_2b_col = f"{cgst_2b}_2B" if cgst_2b else None
    sgst_pr_col = f"{sgst_pr}_PR" if sgst_pr else None
    sgst_2b_col = f"{sgst_2b}_2B" if sgst_2b else None
    igst_pr_col = f"{igst_pr}_PR" if igst_pr else None
    igst_2b_col = f"{igst_2b}_2B" if igst_2b else None
    inv_type_pr_col = f"{inv_type_pr}_PR" if inv_type_pr else None
    inv_type_2b_col = f"{inv_type_2b}_2B" if inv_type_2b else None

    mapping, remarks, reason = [], [], []
    for idx, r in merged.iterrows():
        # --- NEW: Apply Fuzzy Status ---
        fuzzy_rsn = r.get("_FUZZY_REASON")
        if fuzzy_rsn:
            mapping.append("Almost Matched")
            remarks.append("mis-match")
            reason.append(fuzzy_rsn)
            continue
        # -------------------------------

        pr_inv_val = as_text(r.get(inv_pr_col, "")) if inv_pr_col in merged.columns else ""
        pr_gst_val = clean_gstin(r.get(gst_pr_col, "")) if gst_pr_col in merged.columns else ""
        b2_inv_val = as_text(r.get(inv_2b_col, "")) if inv_2b_col in merged.columns else ""
        b2_gst_val = clean_gstin(r.get(gst_2b_col, "")) if gst_2b_col in merged.columns else ""

        pr_present = bool(pr_inv_val) or bool(pr_gst_val)
        b2_present = bool(b2_inv_val) or bool(b2_gst_val)

        is_pr_only = pr_present and not b2_present
        is_2b_only = b2_present and not pr_present

        if is_pr_only:
            mapping.append("Not Matched"); remarks.append("missing in 2B"); reason.append(""); continue
        if is_2b_only:
            mapping.append("Not Matched"); remarks.append("missing in PR"); reason.append(""); continue

        mismatches = []
        if date_pr_col and date_2b_col:
            k_pr = date_key_yyyymmdd(r.get(date_pr_col, ""))
            k_2b = date_key_yyyymmdd(r.get(date_2b_col, ""))
            if (k_pr or k_2b) and (k_pr != k_2b):
                mismatches.append("Invoice Date")

        def neq_round_abs(a, b):
            return abs(round_rupee(a)) != abs(round_rupee(b))

        if cgst_pr_col and cgst_2b_col and neq_round_abs(r.get(cgst_pr_col, 0), r.get(cgst_2b_col, 0)): mismatches.append("CGST")
        if sgst_pr_col and sgst_2b_col and neq_round_abs(r.get(sgst_pr_col, 0), r.get(sgst_2b_col, 0)): mismatches.append("SGST")
        if igst_pr_col and igst_2b_col and neq_round_abs(r.get(igst_pr_col, 0), r.get(igst_2b_col, 0)): mismatches.append("IGST")

        if mismatches:
            mapping.append("Almost Matched"); remarks.append("mismatch"); reason.append("; ".join(mismatches))
        else:
            mapping.append("Matched"); remarks.append("All fields matched"); reason.append("")

    out = merged.copy()
    out["Mapping"] = mapping
    out["Remarks"] = remarks
    out["Reason"] = reason

    if inv_pr_col in out.columns:
        out[inv_pr_col] = out[inv_pr_col].map(as_text)
        mask_fix = (out[inv_pr_col].isin(["", "0"])) | (out[inv_pr_col].isna())
        if isinstance(pr_orig_present, pd.Series):
            mask_fix = mask_fix & pr_orig_present.reindex(out.index).fillna(False)
        out.loc[mask_fix, inv_pr_col] = out.loc[mask_fix, "_INV_KEY"]

    source_sheet_col_2b = "_SOURCE_SHEET_2B"
    if source_sheet_col_2b in out.columns and gst_2b_col in out.columns:
        cdnr_mask = (out[source_sheet_col_2b] == "B2B-CDNR")
        out.loc[cdnr_mask, gst_2b_col] = out.loc[cdnr_mask, "_GST_KEY"]

    def get_final_type(row):
        pr_type = as_text(row.get(inv_type_pr_col, '')) if inv_type_pr_col else ''
        if pr_type:
            return pr_type

        b2_type = as_text(row.get(inv_type_2b_col, '')) if inv_type_2b_col else ''
        if b2_type:
            return b2_type

        note_type = row.get("_NOTE_TYPE_2B", "")
        if note_type == "credit": return "Credit Note"
        if note_type == "debit": return "Debit Note"

        source_sheet = row.get("_SOURCE_SHEET_2B", "")
        if source_sheet == "B2B": return "Invoice"

        if pd.notna(row.get(inv_pr_col)):
             return "Invoice"

        return ""

    out["Invoice Type"] = out.apply(get_final_type, axis=1)

    def pick_name_col(columns, candidates):
        cnorm = [_norm(c) for c in candidates]
        best = None; best_score = -1
        for col in columns:
            n = _norm(col); score = 0
            if any(n == c for c in cnorm): score += 4
            if any(c in n for c in cnorm): score += 2
            if "name" in n: score += 1
            if "date" in n or "period" in n or "month" in n: score -= 10
            if score > best_score: best, best_score = col, score
        return best if best_score > 0 else None

    def pick_from_list(columns, candidates):
        cnorm = [_norm(c) for c in candidates]
        best = None; best_score = -1
        for col in columns:
            n = _norm(col); score = 0
            if any(n == c for c in cnorm): score += 4
            if any(c in n for c in cnorm): score += 2
            if "period" in n: score += 3
            if "name" in n: score += 1
            if score > best_score: best, best_score = col, score
        return best if best_score > 0 else None

    pr_cols_all = [c for c in out.columns if c.endswith("_PR")]
    b2_cols_all = [c for c in out.columns if c.endswith("_2B")]

    vendor_name_pr_col = pick_name_col(pr_cols_all, [f"{x}_PR" for x in VENDOR_NAME_PR_CANDIDATES])
    vendor_name_2b_col = pick_name_col(b2_cols_all, [f"{x}_2B" for x in TRADE_NAME_2B_CANDIDATES + VENDOR_NAME_PR_CANDIDATES])

    gstin_to_name = {}
    def is_valid_name(name_str: str) -> bool:
        if not name_str or pd.isna(name_str): return False
        return not bool(GSTIN_REGEX.fullmatch(name_str.strip()))

    def populate_name_map(df, gstin_col_key, name_col):
        if name_col and gstin_col_key in df.columns:
            for _, row in df[[gstin_col_key, name_col]].drop_duplicates().dropna().iterrows():
                gstin = clean_gstin(row[gstin_col_key])
                name = as_text(row[name_col])
                if gstin and is_valid_name(name):
                    if gstin not in gstin_to_name or len(name) > len(gstin_to_name[gstin]):
                        gstin_to_name[gstin] = name

    populate_name_map(out, "_GST_KEY", vendor_name_pr_col)
    populate_name_map(out, "_GST_KEY", vendor_name_2b_col)

    if gstin_to_name:
        out["Vendor Name"] = out["_GST_KEY"].apply(lambda x: gstin_to_name.get(clean_gstin(x), ""))
    else:
        out["Vendor Name"] = ""

    gstr1_status_2b = pick_from_list(b2_cols_all, [f"{x}_2B" for x in GSTR1_STATUS_2B_CANDIDATES])

    pair_cols = [
        gst_pr_col, gst_2b_col,
        inv_pr_col, inv_2b_col,
        date_pr_col, date_2b_col,
        cgst_pr_col, cgst_2b_col,
        sgst_pr_col, sgst_2b_col,
        igst_pr_col, igst_2b_col,
    ]
    pair_cols = [c for c in pair_cols if c and c in out.columns]

    keep_extra = [c for c in [gstr1_status_2b, "Invoice Type"] if c and c in out.columns]

    front = ["_GST_KEY", "_INV_KEY", "Mapping", "Remarks", "Reason", "Vendor Name"]
    final_cols = front + keep_extra + pair_cols

    final_cols = [c for c in final_cols if c in out.columns]

    out = out[final_cols]

    # --- Force ISO date display as TEXT for both PR and 2B ---
    # This prevents Excel from flipping to MM-DD-YYYY.
    if date_pr_col and date_pr_col in out.columns:
        out[date_pr_col] = out[date_pr_col].apply(date_iso_yyyy_mm_dd)
    if date_2b_col and date_2b_col in out.columns:
        out[date_2b_col] = out[date_2b_col].apply(date_iso_yyyy_mm_dd)
    # ---------------------------------------------------------

    return out, {
        "cgst_pr_col": cgst_pr_col, "sgst_pr_col": sgst_pr_col, "igst_pr_col": igst_pr_col,
        "cgst_2b_col": cgst_2b_col, "sgst_2b_col": sgst_2b_col, "igst_2b_col": igst_2b_col,
        "gstr1_status_2b_col": gstr1_status_2b
    }

# -------------------- Dashboard --------------------
def build_dashboard(df_recon: pd.DataFrame, cols: Dict[str, Optional[str]]) -> pd.DataFrame:
    cgst_pr = cols.get("cgst_pr_col")
    sgst_pr = cols.get("sgst_pr_col")
    igst_pr = cols.get("igst_pr_col")
    cgst_2b = cols.get("cgst_2b_col")
    sgst_2b = cols.get("sgst_2b_col")
    igst_2b = cols.get("igst_2b_col")

    def _sum(series_name: Optional[str], status: Optional[str]) -> float:
        if not series_name or series_name not in df_recon.columns:
            return 0.0
        s = pd.to_numeric(df_recon[series_name], errors="coerce").fillna(0)
        if status and "Mapping" in df_recon.columns:
            s = s[df_recon["Mapping"] == status]
        return float(s.sum())

    def block(status: Optional[str]) -> dict:
        return {
            ("Matched" if status == "Matched" else
             "Almost Matched" if status == "Almost Matched" else
             "Not Matched" if status == "Not Matched" else
             "Total", "PR"): [
                _sum(cgst_pr, status),
                _sum(sgst_pr, status),
                _sum(igst_pr, status),
                _sum(cgst_pr, status) + _sum(sgst_pr, status) + _sum(igst_pr, status),
            ],
            ("Matched" if status == "Matched" else
             "Almost Matched" if status == "Almost Matched" else
             "Not Matched" if status == "Not Matched" else
             "Total", "GSTR 2B"): [
                _sum(cgst_2b, status),
                _sum(sgst_2b, status),
                _sum(igst_2b, status),
                _sum(cgst_2b, status) + _sum(sgst_2b, status) + _sum(igst_2b, status),
            ],
        }

    rows = ["CGST", "SGST", "IGST", "Total"]
    data_cols = {}
    for st in ["Matched", "Almost Matched", "Not Matched", None]:
        data_cols.update(block(st))

    left = {("Status", "Report"): rows}

    cols_mi = pd.MultiIndex.from_tuples(
        list(left.keys()) + list(data_cols.keys()),
        names=["", ""]
    )
    df = pd.DataFrame(
        list(zip(*([*left.values(), *data_cols.values()])))
    )
    df.columns = cols_mi
    return df


@app.route("/", methods=["GET"])
def index():
    # Default to False
    is_active = False

    # Check if user is logged in
    if session.get('logged_in'):
        # Fetch the user record from the database
        user = User.query.get(session.get('user_id'))
        if user:
            # Check both the 'subscribed' flag and the expiry date
            subscribed = getattr(user, "subscribed", False)
            expiry = getattr(user, "subscription_expiry_date", None)
            try:
                # User is active ONLY if subscribed is True AND date is valid/future
                if subscribed and expiry and expiry >= date.today():
                    is_active = True
            except Exception:
                pass

    # Pass the result to the template so the JavaScript check works
    return render_template("index.html", is_active_subscriber=is_active)

# REPLACEMENT FUNCTION: Protects the reconciliation module.
@app.route("/verify", methods=["POST"])
def verify_columns():
    """
    Handle uploaded GSTR-2B and Purchase Register files, detect columns and render
    the verification page. Defensive about missing subscription fields to avoid
    AttributeError when deployed DB schema is out-of-sync.
    """
    # --- Access Control Check ---
    if not session.get('logged_in'):
        flash('Please log in to access this feature.', 'warning')
        return redirect(url_for('login'))

    user = User.query.filter_by(email=session.get('email')).first()

    # Defensive access of attributes (avoids AttributeError if DB column missing)
    subscribed = getattr(user, "subscribed", False) if user else False
    expiry = getattr(user, "subscription_expiry_date", None) if user else None

    try:
        is_active_subscriber = bool(user and subscribed and expiry and expiry >= date.today())
    except Exception:
        # expiry may not be a date object (or may be a string) — treat as not active.
        is_active_subscriber = False

    if not is_active_subscriber:
        flash('Your subscription has expired. Please choose a plan to continue.', 'danger')
        return redirect(url_for('subscribe'))
    # --- End of Access Control ---

    # Get uploaded files and format choice
    file_2b = request.files.get("gstr2b")
    file_pr = request.files.get("purchase_register")
    gstr2b_format = (request.form.get("gstr2b_format") or "portal").strip()

    if not file_2b or not file_pr:
        flash("Please upload both files: GSTR-2B and Purchase Register.", "warning")
        return redirect(url_for("index"))

    # Save uploaded files to temporary files and persist paths in session
    tmp2b = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
    tmppr = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
    try:
        file_2b.stream.seek(0)
        tmp2b.write(file_2b.read())
        tmp2b.close()
        file_pr.stream.seek(0)
        tmppr.write(file_pr.read())
        tmppr.close()
    except Exception as e:
        try:
            tmp2b.close()
            tmppr.close()
        except Exception:
            pass
        try:
            os.remove(tmp2b.name)
        except Exception:
            pass
        try:
            os.remove(tmppr.name)
        except Exception:
            pass
        flash("Failed to read uploaded files. Please try again.", "danger")
        return redirect(url_for("index"))

    # Store temp file paths + chosen format in session for the next step (confirm/reconcile)
    session["tmp2b"] = tmp2b.name
    session["tmppr"] = tmppr.name
    session["gstr2b_format"] = gstr2b_format

    # --- VALIDATION START: Check file contents ---

    # 1. Validate GSTR-2B File
    try:
        with pd.ExcelFile(tmp2b.name) as xls_2b:
            sheets_2b = xls_2b.sheet_names
    except Exception as e:
        flash("Failed to read the GSTR-2B file. Ensure it is a valid Excel file.", "danger")
        try:
            os.remove(tmp2b.name); os.remove(tmppr.name)
        except: pass
        return redirect(url_for("index"))

    # Strict Rule: GSTR-2B must have a 'B2B' sheet
    if "B2B" not in sheets_2b:
        flash("Invalid GSTR-2B File: The file uploaded as GSTR-2B does not contain a 'B2B' sheet.", "danger")
        try:
            os.remove(tmp2b.name); os.remove(tmppr.name)
        except: pass
        return redirect(url_for("index"))

    # 2. Validate Purchase Register (Swap Check)
    try:
        with pd.ExcelFile(tmppr.name) as xls_pr:
            sheets_pr = xls_pr.sheet_names

            # Logic: If PR file has BOTH 'B2B' and 'B2B-CDNR', it is almost certainly the GSTR-2B file uploaded by mistake.
            if "B2B" in sheets_pr and "B2B-CDNR" in sheets_pr:
                flash("Incorrect File: It looks like you uploaded the GSTR-2B file in the Purchase Register slot. Please check your files.", "warning")
                try:
                    os.remove(tmp2b.name); os.remove(tmppr.name)
                except: pass
                return redirect(url_for("index"))

            # Logic: If PR file matches the 2B file exactly (Same sheets)
            if sheets_pr == sheets_2b:
                flash("Duplicate Files: You seem to have uploaded the same file in both slots.", "warning")
                try:
                    os.remove(tmp2b.name); os.remove(tmppr.name)
                except: pass
                return redirect(url_for("index"))

    except Exception as e:
        flash("Failed to read the Purchase Register file. Ensure it is a valid Excel file.", "danger")
        try:
            os.remove(tmp2b.name); os.remove(tmppr.name)
        except: pass
        return redirect(url_for("index"))

    # --- VALIDATION END ---

    # Proceed with existing logic variables
    sheet_names = sheets_2b
    present_sheets = [sn for sn in ["B2B", "B2B-CDNR"] if sn in sheet_names]

    # Header rows depend on portal vs flat format
    header_rows = [4, 5] if gstr2b_format == "portal" else 0

    # Read and normalize B2B and CDNR sheets (if present)
    df_b2b = None
    df_cdnr = None
    try:
        if "B2B" in present_sheets:
            df_b2b = pd.read_excel(tmp2b.name, sheet_name="B2B", header=header_rows, engine="openpyxl", dtype=str)
            df_b2b = df_b2b.dropna(how="all")
            df_b2b.columns = flatten_columns(df_b2b.columns)
            df_b2b, _ = normalize_columns(df_b2b)

        if "B2B-CDNR" in present_sheets:
            df_cdnr = pd.read_excel(tmp2b.name, sheet_name="B2B-CDNR", header=header_rows, engine="openpyxl", dtype=str)
            df_cdnr = df_cdnr.dropna(how="all")
            df_cdnr.columns = flatten_columns(df_cdnr.columns)
            df_cdnr, _ = normalize_columns(df_cdnr)
    except Exception as e:
        flash("Failed to parse sheets from the GSTR-2B file. Ensure the file has expected structure.", "danger")
        return redirect(url_for("index"))

    # Read and normalize Purchase Register
    try:
        df_pr_raw = pd.read_excel(tmppr.name, engine="openpyxl", dtype=str)
        df_pr_raw, _ = normalize_columns(df_pr_raw)
    except Exception as e:
        flash("Failed to read the Purchase Register file. Ensure it is a valid Excel file.", "danger")
        return redirect(url_for("index"))

    # Heuristic column picks for B2B (invoice-based)
    inv_2b_b2b = _pick_column(df_b2b, INVOICE_CANDIDATES_2B) if df_b2b is not None else None
    gst_2b_b2b = _pick_column(df_b2b, GSTIN_CANDIDATES_2B) if df_b2b is not None else None
    date_2b_b2b = _pick_column(df_b2b, DATE_CANDIDATES_2B) if df_b2b is not None else None
    cgst_2b_b2b = _pick_column(df_b2b, CGST_CANDIDATES_2B) if df_b2b is not None else None
    sgst_2b_b2b = _pick_column(df_b2b, SGST_CANDIDATES_2B) if df_b2b is not None else None
    igst_2b_b2b = _pick_column(df_b2b, IGST_CANDIDATES_2B) if df_b2b is not None else None

    # Heuristic column picks for CDNR (note-based)
    note_2b_cdnr = _pick_column(df_cdnr, NOTE_NO_CANDIDATES_2B) if df_cdnr is not None else None
    notedate_2b_cdnr = _pick_column(df_cdnr, NOTE_DATE_CANDIDATES_2B) if df_cdnr is not None else None
    note_type_2b_cdnr = _pick_column(df_cdnr, NOTE_TYPE_CANDIDATES_2B) if df_cdnr is not None else None
    gst_2b_cdnr = _pick_column(df_cdnr, GSTIN_CANDIDATES_CDNR) if df_cdnr is not None else None
    cgst_2b_cdnr = _pick_column(df_cdnr, CGST_CANDIDATES_2B) if df_cdnr is not None else None
    sgst_2b_cdnr = _pick_column(df_cdnr, SGST_CANDIDATES_2B) if df_cdnr is not None else None
    igst_2b_cdnr = _pick_column(df_cdnr, IGST_CANDIDATES_2B) if df_cdnr is not None else None

    # Heuristic column picks for Purchase Register
    inv_pr = _pick_column(df_pr_raw, INVOICE_CANDIDATES_PR, avoid_terms=AVOID_DOC_LIKE_FOR_PR,
                          extra_penalties=["gstin", "company", "recipient"])
    gst_pr = _pick_column(df_pr_raw, GSTIN_CANDIDATES_PR, extra_penalties=AVOID_RECIPIENT_GSTIN_FOR_PR)
    date_pr = _pick_column(df_pr_raw, DATE_CANDIDATES_PR)
    cgst_pr = _pick_column(df_pr_raw, CGST_CANDIDATES_PR)
    sgst_pr = _pick_column(df_pr_raw, SGST_CANDIDATES_PR)
    igst_pr = _pick_column(df_pr_raw, IGST_CANDIDATES_PR)

    # Value-based fallbacks if heuristics fail
    if not gst_pr or gst_pr not in df_pr_raw.columns:
        guess = pick_gstin_by_values(df_pr_raw, prefer_supplier=True)
        if guess:
            gst_pr = guess
    if not inv_pr or inv_pr not in df_pr_raw.columns:
        guess = pick_invoice_by_values(df_pr_raw)
        if guess:
            inv_pr = guess

    cols_b2b = sorted(df_b2b.columns) if df_b2b is not None else []
    cols_cdnr = sorted(df_cdnr.columns) if df_cdnr is not None else []
    cols_pr = sorted(df_pr_raw.columns)

    # Render the verify page with detected columns and choices prefilled
    return render_template(
        "verify.html",
        cols_pr=cols_pr,
        inv_pr=inv_pr, gst_pr=gst_pr, date_pr=date_pr, cgst_pr=cgst_pr, sgst_pr=sgst_pr, igst_pr=igst_pr,
        cols_2b_b2b=cols_b2b,
        inv_2b_b2b=inv_2b_b2b, gst_2b_b2b=gst_2b_b2b, date_2b_b2b=date_2b_b2b,
        cgst_2b_b2b=cgst_2b_b2b, sgst_2b_b2b=sgst_2b_b2b, igst_2b_b2b=igst_2b_b2b,
        cols_2b_cdnr=cols_cdnr,
        note_2b_cdnr=note_2b_cdnr, notedate_2b_cdnr=notedate_2b_cdnr, gst_2b_cdnr=gst_2b_cdnr,
        cgst_2b_cdnr=cgst_2b_cdnr, sgst_2b_cdnr=sgst_2b_cdnr, igst_2b_cdnr=igst_2b_cdnr,
        note_type_2b_cdnr=note_type_2b_cdnr,
        has_cdnr=("B2B-CDNR" in present_sheets)
    )

# ---------- helpers ----------

# NEW: add this helper function (place it near your other DB helpers, before create_tables_once)
def ensure_user_schema():
    """
    Ensure the User table has the subscription_expiry_date column.
    Safe, simple ALTER TABLE to add the DATE column if missing.
    Works for sqlite and postgres; logs and continues if it cannot alter.
    """
    try:
        engine = db.get_engine()
        inspector = inspect(engine)
        table_name = User.__table__.name
        # get existing column names
        cols = [c['name'] for c in inspector.get_columns(table_name)]
        if 'subscription_expiry_date' in cols:
            return  # already present

        dialect = engine.dialect.name
        sql = f'ALTER TABLE "{table_name}" ADD COLUMN subscription_expiry_date DATE'
        # Execute ALTER for sqlite/postgres or generic
        engine.execute(sa.text(sql))
        app.logger.info("Added subscription_expiry_date column to %s (dialect=%s)", table_name, dialect)
    except Exception as e:
        # Log but don't crash the app; verify endpoint is made defensive separately.
        app.logger.exception("Could not ensure User schema: %s", e)

def _pick_best_note_col(df: pd.DataFrame, primary: Optional[str]) -> Optional[str]:
    if df is None or df.empty:
        return primary
    def non_empty_ratio(col):
        if col not in df.columns:
            return 0.0
        s = df[col].astype(str).str.strip()
        return (s != "").mean()
    if (not primary) or (non_empty_ratio(primary) < 0.30):
        best_col, best_score = None, 0.0
        for c in NOTE_NO_CANDIDATES_2B:
            if c in df.columns:
                sc = non_empty_ratio(c)
                if sc > best_score:
                    best_col, best_score = c, sc
        return best_col or primary
    return primary

def _rescue_empty_inv_keys(df: pd.DataFrame, inv_col: str) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    def _is_empty_key(s: pd.Series) -> pd.Series:
        return s.isna() | (s.astype(str).str.strip() == "")
    mask_empty = _is_empty_key(df["_INV_KEY"])
    if mask_empty.any() and inv_col in df.columns:
        df.loc[mask_empty, "_INV_KEY"] = df.loc[mask_empty, inv_col].map(inv_basic)
    mask_still = _is_empty_key(df["_INV_KEY"])
    if mask_still.any():
        df.loc[mask_still, "_INV_KEY"] = df.index[mask_still].map(lambda i: f"__CDNR_ROW__{i}__")
    return df

def _append_if_missing(target_list: List[str], candidates: List[Optional[str]]) -> None:
    for c in candidates:
        if c and (c not in target_list):
            target_list.append(c)

# main.py

def _run_reconciliation_pipeline(tmp2b_path: str, tmppr_path: str, gstr2b_format: str, target_return_period: str,
                                 # B2B (invoice-based)
                                 inv_2b_b2b_sel: str, gst_2b_b2b_sel: str, date_2b_b2b_sel: str, cgst_2b_b2b_sel: str, sgst_2b_b2b_sel: str, igst_2b_b2b_sel: str,
                                 # CDNR (note-based)
                                 note_2b_cdnr_sel: str, gst_2b_cdnr_sel: str, notedate_2b_cdnr_sel: str, cgst_2b_cdnr_sel: str, sgst_2b_cdnr_sel: str, igst_2b_cdnr_sel: str,
                                 # PR
                                 inv_pr_sel: str, gst_pr_sel: str, date_pr_sel: str, cgst_pr_sel: str, sgst_pr_sel: str, igst_pr_sel: str) -> bytes:

    with pd.ExcelFile(tmp2b_path) as xls:
        sheet_names = set(xls.sheet_names)
    has_b2b = "B2B" in sheet_names
    has_cdnr = "B2B-CDNR" in sheet_names
    if not has_b2b:
        raise ValueError("Could not find a 'B2B' sheet in the GSTR-2B file.")

    header_rows = [4, 5] if gstr2b_format == "portal" else 0

    def load_sheet(name):
        df = pd.read_excel(tmp2b_path, sheet_name=name, header=header_rows)
        df = df.dropna(how="all")
        if df.empty:
            return df
        df.columns = flatten_columns(df.columns)
        df, _ = normalize_columns(df)
        df["_SOURCE_SHEET"] = name
        return df

    # 1. Define Helper to Filter Imports/Overseas
    # Returns: (filtered_df, captured_imports_df)
    def _filter_imports_overseas(df):
        if df is None or df.empty: return df, pd.DataFrame()

        type_candidates = [
            "purchase type", "transaction type", "nature of transaction",
            "type of purchase", "supply type", "nature", "voucher type", "details"
        ]
        target_col = _find_optional_col(df, [type_candidates])

        # Fallback search
        if not target_col:
            for c in df.columns:
                cn = str(c).lower()
                if "type" in cn and ("purchase" in cn or "trans" in cn or "nature" in cn):
                    target_col = c
                    break

        if target_col:
            exclude_terms = ["import of goods", "imported goods", "import", "overseas"]
            def is_excluded(val):
                s = str(val).lower().strip()
                if not s: return False
                for term in exclude_terms:
                    if term in s: return True
                return False

            mask_exclude = df[target_col].apply(is_excluded)

            # Split the data
            df_imports = df[mask_exclude].copy() # Captured rows
            df_clean = df[~mask_exclude].copy()  # Remaining rows

            return df_clean, df_imports

        return df, pd.DataFrame()

    # 2. Load and Filter Sheets
    df_b2b_raw = load_sheet("B2B") if has_b2b else pd.DataFrame()

    # FIX: Capture imports from B2B (Row 1 Format) separately
    df_b2b_raw, df_b2b_imports = _filter_imports_overseas(df_b2b_raw)

    df_cdnr_raw = load_sheet("B2B-CDNR") if has_cdnr else pd.DataFrame()

    df_impg_raw = load_sheet("IMPG") if "IMPG" in sheet_names else pd.DataFrame()

    df_pr_raw = pd.read_excel(tmppr_path, engine="openpyxl", dtype=str)
    df_pr_raw, pr_norm_map = normalize_columns(df_pr_raw)

    # Filter PR (we just discard PR imports, so we use underscore _)
    df_pr_raw, _ = _filter_imports_overseas(df_pr_raw)

    # 3. CRITICAL: Force Negative Signs on CDNR Immediately
    def _force_negative_cdnr(df):
        if df is None or df.empty: return df
        df = df.copy()

        target_cols = []
        for c in df.columns:
            cn = str(c).lower()

            keywords = [
                "tax", "value", "amount", "cess",
                "igst", "cgst", "sgst",
                "integrated", "central", "state"
            ]

            if any(x in cn for x in keywords):
                # FIX: "Integrated" contains the word "rate".
                # We must NOT exclude it. Only exclude "rate" if it is NOT "integrated".
                is_rate_col = ("rate" in cn) and ("integrated" not in cn)

                if not is_rate_col:
                    target_cols.append(c)

        # Function to flip sign
        def flip_sign_row(row, col_name):
            val = parse_amount(row[col_name])
            # Check ENTIRE row text for 'Debit' or 'Dr' to keep it positive
            row_text = " ".join(row.astype(str)).lower()
            if "debit" in row_text or " dr " in row_text or " dr." in row_text:
                return abs(val)
            else:
                return -abs(val)

        for col in target_cols:
            df[col] = df.apply(lambda r: flip_sign_row(r, col), axis=1)

        return df

    if not df_cdnr_raw.empty:
        df_cdnr_raw = _force_negative_cdnr(df_cdnr_raw)

    # 4. Create 'Signed' variables immediately
    df_b2b_raw_signed = df_b2b_raw.copy()
    df_cdnr_raw_signed = df_cdnr_raw.copy() # This is now already negative
    df_impg_raw_signed = df_impg_raw.copy()

    def match_provided(df: pd.DataFrame, provided: str) -> Optional[str]:
        if not provided:
            return None
        p = _softnorm(provided)
        for c in df.columns:
            if _softnorm(c) == p:
                return c
        if p in pr_norm_map:
            return pr_norm_map[p]
        return None

    def ensure_col(df, provided, candidates, avoid=None, penalties=None, value_picker=None):
        if df is None or df.empty:
            return None
        col = match_provided(df, provided)
        if col and col in df.columns:
            return col
        pick = _pick_column(df, candidates, avoid_terms=avoid, extra_penalties=penalties)
        if pick:
            return pick
        if value_picker:
            guess = value_picker(df)
            if guess:
                return guess
        return None

    def _pick_col_contains(df, pattern):
        if df is None:
            return None
        pat = re.compile(pattern, re.I)
        for c in df.columns:
            if pat.search(str(c)):
                return c
        return None
    cdnr_note_hard  = _pick_col_contains(df_cdnr_raw, r"\bnote\s*number\b")
    cdnr_ndate_hard = _pick_col_contains(df_cdnr_raw, r"\bnote\s*date\b")

    inv_2b_b2b = ensure_col(df_b2b_raw, inv_2b_b2b_sel, INVOICE_CANDIDATES_2B)
    gst_2b_b2b = ensure_col(df_b2b_raw, gst_2b_b2b_sel, GSTIN_CANDIDATES_2B)
    date_2b_b2b = ensure_col(df_b2b_raw, date_2b_b2b_sel, DATE_CANDIDATES_2B)
    cgst_2b_b2b = ensure_col(df_b2b_raw, cgst_2b_b2b_sel, CGST_CANDIDATES_2B)
    sgst_2b_b2b = ensure_col(df_b2b_raw, sgst_2b_b2b_sel, SGST_CANDIDATES_2B)
    igst_2b_b2b = ensure_col(df_b2b_raw, igst_2b_b2b_sel, IGST_CANDIDATES_2B)

    note_2b_cdnr     = ensure_col(df_cdnr_raw, note_2b_cdnr_sel, NOTE_NO_CANDIDATES_2B)
    notedate_2b_cdnr = ensure_col(df_cdnr_raw, notedate_2b_cdnr_sel, NOTE_DATE_CANDIDATES_2B)
    note_type_2b = ensure_col(df_cdnr_raw, "", NOTE_TYPE_CANDIDATES_2B)
    if cdnr_note_hard:
        note_2b_cdnr = cdnr_note_hard
    if cdnr_ndate_hard:
        notedate_2b_cdnr = cdnr_ndate_hard
    note_2b_cdnr = _pick_best_note_col(df_cdnr_raw, note_2b_cdnr)

    gst_2b_cdnr = ensure_col(df_cdnr_raw, gst_2b_cdnr_sel, GSTIN_CANDIDATES_CDNR)
    cgst_2b_cdnr = ensure_col(df_cdnr_raw, cgst_2b_cdnr_sel, CGST_CANDIDATES_2B)
    sgst_2b_cdnr = ensure_col(df_cdnr_raw, sgst_2b_cdnr_sel, SGST_CANDIDATES_2B)
    igst_2b_cdnr = ensure_col(df_cdnr_raw, igst_2b_cdnr_sel, IGST_CANDIDATES_2B)

    inv_type_pr = ensure_col(df_pr_raw, "", INV_TYPE_CANDIDATES)
    inv_type_2b_b2b = ensure_col(df_b2b_raw, "", INV_TYPE_CANDIDATES)
    inv_type_2b_cdnr = ensure_col(df_cdnr_raw, "", INV_TYPE_CANDIDATES)

    # ========== BUILD LOOKUPS FROM RAW DATA BEFORE ANY PROCESSING ==========
    # Calculate header offset for Excel row numbers
    # Calculate header offset for Excel row numbers
    # Pandas uses 0-based row indices; the last header row index is max(header_rows)
    # Excel row for pandas index i = i + (last_header_index + 2)
    if isinstance(header_rows, (list, tuple)):
        header_offset = max(header_rows) + 2
    else:
        # header_rows is an int (e.g. 0) -> last header index = header_rows
        header_offset = header_rows + 2

    row_num_lookup = {}
    period_lookup = {}

    # Process B2B sheet for lookups
    if df_b2b_raw is not None and not df_b2b_raw.empty and gst_2b_b2b and inv_2b_b2b:
        gstr1_period_col_b2b = _find_optional_col(df_b2b_raw, [GSTR1_STATUS_2B_CANDIDATES])
        for idx, row in df_b2b_raw.iterrows():
            gst = clean_gstin(row.get(gst_2b_b2b, ""))
            inv = inv_basic(row.get(inv_2b_b2b, ""))
            if gst and inv:
                key = (gst, inv)
                # Excel row number: pandas index + header offset
                excel_row = idx + header_offset

                # Store row numbers (handle multiple rows with same key)
                if key not in row_num_lookup:
                    row_num_lookup[key] = []
                row_num_lookup[key].append(excel_row)

                # Store period (use first occurrence)
                if key not in period_lookup and gstr1_period_col_b2b:
                    period_lookup[key] = as_text(row.get(gstr1_period_col_b2b, ""))

    # Process CDNR sheet for lookups
    if df_cdnr_raw is not None and not df_cdnr_raw.empty and gst_2b_cdnr and note_2b_cdnr:
        gstr1_period_col_cdnr = _find_optional_col(df_cdnr_raw, [GSTR1_STATUS_2B_CANDIDATES])
        for idx, row in df_cdnr_raw.iterrows():
            gst = clean_gstin(row.get(gst_2b_cdnr, ""))
            inv = inv_basic(row.get(note_2b_cdnr, ""))
            if gst and inv:
                key = (gst, inv)
                # Excel row number: pandas index + header offset
                excel_row = idx + header_offset

                # Store row numbers (handle multiple rows with same key)
                if key not in row_num_lookup:
                    row_num_lookup[key] = []
                row_num_lookup[key].append(excel_row)

                # Store period (use first occurrence)
                if key not in period_lookup and gstr1_period_col_cdnr:
                    period_lookup[key] = as_text(row.get(gstr1_period_col_cdnr, ""))

    # Convert row number lists to comma-separated strings
    for key in row_num_lookup:
        row_num_lookup[key] = ", ".join(map(str, sorted(row_num_lookup[key])))
    # ========== END OF LOOKUP BUILDING ==========

    def apply_signs(df, note_type_col: Optional[str]) -> pd.DataFrame:
        if df.empty:
            return df
        df = df.copy()

        # Helper to normalize note type text
        def normalize_note_type(val: str) -> str:
            s = as_text(val).lower()
            if re.search(r'\bdebit\b|\bdn\b|\bdr\b', s):  return "debit"
            return "credit" # Default to credit for normalization context

        # 1. Determine the Note Type for every row
        if note_type_col and note_type_col in df.columns:
            # Trust the specific column if provided
            df["_NOTE_TYPE"] = df[note_type_col].map(normalize_note_type)
        else:
            # Fallback inference
            def infer(row):
                # STRICT RULE: If this row comes from the CDNR sheet, it is CREDIT (Negative)
                # unless we find "debit" in a known note-type-like column.
                if row.get("_SOURCE_SHEET") == "B2B-CDNR":
                    # Check if there's any hint of "Debit" in any likely column
                    # (We check the note number or similar fields just in case)
                    for col in row.index:
                        val = str(row[col]).lower()
                        if "debit" in val or "dr" in val:
                             # Be careful not to match "Address" or random text,
                             # but if the user hasn't mapped a note type col, this is rare.
                             # Safest bet for CDNR sheet is Credit.
                             pass
                    return "credit"

                # For B2B sheet, it's an invoice (positive)
                return "invoice"

            df["_NOTE_TYPE"] = df.apply(infer, axis=1)

        # 2. FORCE OVERRIDE for CDNR Sheet
        # If the row is from B2B-CDNR, force it to be 'credit' (negative)
        # UNLESS it was explicitly detected as 'debit' above.
        if "_SOURCE_SHEET" in df.columns:
            # Everything in CDNR is credit unless explicitly debit
            mask_cdnr = (df["_SOURCE_SHEET"] == "B2B-CDNR") & (df["_NOTE_TYPE"] != "debit")
            df.loc[mask_cdnr, "_NOTE_TYPE"] = "credit"

        # 3. Apply the Signs
        numeric_cols = [c for c in (
            CGST_CANDIDATES_2B + SGST_CANDIDATES_2B + IGST_CANDIDATES_2B +
            TAXABLE_CANDIDATES_2B + TOTAL_TAX_CANDIDATES_2B + INVOICE_VALUE_CANDIDATES_2B + CESS_CANDIDATES_2B
        ) if c in df.columns]

        for col in numeric_cols:
            # ABS ensure we start positive, then flip to negative if Credit
            df[col] = df.apply(lambda r: -abs(parse_amount(r[col])) if r["_NOTE_TYPE"] == "credit" else abs(parse_amount(r[col])), axis=1)

        return df

    # --- START: THE FIX ---
    # We pass a .copy() to apply_signs, ensuring the original df_b2b_raw and df_cdnr_raw are not modified.
    #df_b2b_raw_signed = apply_signs(df_b2b_raw.copy(), note_type_col=note_type_2b)
    #df_cdnr_raw_signed = apply_signs(df_cdnr_raw.copy(), note_type_col=note_type_2b)
    # Signed copy for IMPG (if present) — used for 4A1
    #df_impg_raw_signed = apply_signs(df_impg_raw.copy(), note_type_col=None) if (df_impg_raw is not None and not df_impg_raw.empty) else pd.DataFrame()
    # --- END: THE FIX ---

    gst_pr = match_provided(df_pr_raw, gst_pr_sel) or ensure_col(df_pr_raw, gst_pr_sel, GSTIN_CANDIDATES_PR,
                        penalties=AVOID_RECIPIENT_GSTIN_FOR_PR,
                        value_picker=lambda d: pick_gstin_by_values(d, prefer_supplier=True))
    inv_pr = match_provided(df_pr_raw, inv_pr_sel) or ensure_col(df_pr_raw, inv_pr_sel, INVOICE_CANDIDATES_PR,
                        avoid=AVOID_DOC_LIKE_FOR_PR,
                        penalties=["company", "recipient", "gstin"],
                        value_picker=pick_invoice_by_values)
    date_pr = match_provided(df_pr_raw, date_pr_sel) or ensure_col(df_pr_raw, date_pr_sel, DATE_CANDIDATES_PR)
    cgst_pr = match_provided(df_pr_raw, cgst_pr_sel) or ensure_col(df_pr_raw, cgst_pr_sel, CGST_CANDIDATES_PR)
    sgst_pr = match_provided(df_pr_raw, sgst_pr_sel) or ensure_col(df_pr_raw, sgst_pr_sel, SGST_CANDIDATES_PR)
    igst_pr = match_provided(df_pr_raw, igst_pr_sel) or ensure_col(df_pr_raw, igst_pr_sel, IGST_CANDIDATES_PR)

    # ----------------- Build PR row number lookup -----------------
    # NOTE: This lookup needs gst_pr and inv_pr to be known; they are selected above.
    # Build the PR row lookup so we can show Excel row numbers for matched/almost matched items.
    pr_row_num_lookup = {}
    try:
        pr_header_offset = 2  # PR sheet header is single row -> data starts at Excel row 2 => idx + 2
        if df_pr_raw is not None and not df_pr_raw.empty and gst_pr and inv_pr:
            for idx, row in df_pr_raw.iterrows():
                gst = clean_gstin(row.get(gst_pr, ""))
                inv = inv_basic(row.get(inv_pr, ""))
                if gst and inv:
                    key = (as_text(gst), as_text(inv))
                    excel_row = idx + pr_header_offset
                    pr_row_num_lookup.setdefault(key, []).append(excel_row)
        # convert lists to comma-separated strings (iterate over a copy of keys)
        for key in list(pr_row_num_lookup.keys()):
            pr_row_num_lookup[key] = ", ".join(map(str, sorted(pr_row_num_lookup[key])))
    except Exception:
        pr_row_num_lookup = {}
    # ----------------------------------------------------------------

    def optional_numeric_list(df_):
        res = []
        for pool in [[ "taxable value","taxable amount","assessable value","taxable" ],
                     [ "total tax","total tax amount","tax amount" ],
                     [ "invoice value","total invoice value","value of invoice","invoice total" ],
                     [ "cess","cess amount" ]]:
            col = _find_optional_col(df_, [pool])
            if df_ is not None and not df_.empty and col:
                res.append(col)
        return res

    opt_2b_b2b = optional_numeric_list(df_b2b_raw_signed)
    _append_if_missing(opt_2b_b2b, [ensure_col(df_b2b_raw_signed, "", CGST_CANDIDATES_2B),
                                    ensure_col(df_b2b_raw_signed, "", SGST_CANDIDATES_2B),
                                    ensure_col(df_b2b_raw_signed, "", IGST_CANDIDATES_2B)])

    opt_2b_cdnr = optional_numeric_list(df_cdnr_raw_signed)
    _append_if_missing(opt_2b_cdnr, [ensure_col(df_cdnr_raw_signed, "", CGST_CANDIDATES_2B),
                                     ensure_col(df_cdnr_raw_signed, "", SGST_CANDIDATES_2B),
                                     ensure_col(df_cdnr_raw_signed, "", IGST_CANDIDATES_2B)])

    opt_pr = optional_numeric_list(df_pr_raw)
    _append_if_missing(opt_pr, [cgst_pr, sgst_pr, igst_pr])

    df_2b_b2b = consolidate_by_key(
        df=df_b2b_raw_signed, gstin_col=gst_2b_b2b, inv_col=inv_2b_b2b,
        date_col=date_2b_b2b, numeric_cols=opt_2b_b2b,
        text_cols=[inv_type_2b_b2b]
    ) if (df_b2b_raw is not None and not df_b2b_raw.empty and inv_2b_b2b and gst_2b_b2b) else pd.DataFrame()

    df_2b_cdnr = consolidate_by_key(
        df=df_cdnr_raw_signed, gstin_col=gst_2b_cdnr, inv_col=note_2b_cdnr,
        date_col=notedate_2b_cdnr, numeric_cols=opt_2b_cdnr,
        text_cols=[inv_type_2b_cdnr]
    ) if (df_cdnr_raw is not None and not df_cdnr_raw.empty and note_2b_cdnr and gst_2b_cdnr) else pd.DataFrame()

    if not df_2b_cdnr.empty:
        df_2b_cdnr = _rescue_empty_inv_keys(df_2b_cdnr, inv_col=note_2b_cdnr)

    df_pr = consolidate_by_key(
        df=df_pr_raw, gstin_col=gst_pr, inv_col=inv_pr,
        date_col=date_pr, numeric_cols=opt_pr,
        text_cols=[inv_type_pr]
    )

    def add_display_cols(df_, inv_col, date_col, source_tag):
        if df_.empty:
            return df_
        df = df_.copy()
        df["_INV_DISPLAY"] = df[inv_col] if inv_col in df.columns else ""
        if date_col and date_col in df.columns:
            df["_DATE_DISPLAY"] = df[date_col].apply(format_date_display)
        else:
            df["_DATE_DISPLAY"] = ""
        df["_DISPLAY_SOURCE"] = source_tag
        return df

    df_2b_b2b = add_display_cols(df_2b_b2b, inv_2b_b2b, date_2b_b2b, "B2B")
    df_2b_cdnr = add_display_cols(df_2b_cdnr, note_2b_cdnr, notedate_2b_cdnr, "B2B-CDNR")

    if "_NOTE_TYPE" in df_cdnr_raw_signed.columns and not df_2b_cdnr.empty:
        nt_map = (df_cdnr_raw_signed.assign(_GST_KEY=df_cdnr_raw_signed[gst_2b_cdnr].map(clean_gstin),
                                     _INV_KEY=df_cdnr_raw_signed[note_2b_cdnr].map(inv_basic))
                            .dropna(subset=["_GST_KEY","_INV_KEY"]))
        nt_map = nt_map.groupby(["_GST_KEY","_INV_KEY"])["_NOTE_TYPE"].first().to_dict()
        df_2b_cdnr["_NOTE_TYPE"] = df_2b_cdnr.apply(lambda r: nt_map.get((r["_GST_KEY"], r["_INV_KEY"]), ""), axis=1)

    df_2b = pd.concat([df_2b_b2b, df_2b_cdnr], ignore_index=True) if (not df_2b_b2b.empty or not df_2b_cdnr.empty) else df_2b_b2b

    combined_df, pair_cols = build_pairwise_recon(
        df_pr=df_pr, df_2b=df_2b,
        inv_pr=inv_pr, gst_pr=gst_pr, date_pr=date_pr, cgst_pr=cgst_pr, sgst_pr=sgst_pr, igst_pr=igst_pr,
        inv_type_pr=inv_type_pr,
        inv_2b="_INV_DISPLAY", gst_2b=(gst_2b_b2b or gst_2b_cdnr), date_2b="_DATE_DISPLAY",
        cgst_2b=(cgst_2b_b2b or cgst_2b_cdnr), sgst_2b=(sgst_2b_b2b or sgst_2b_cdnr), igst_2b=(igst_2b_b2b or igst_2b_cdnr),
        inv_type_2b=(inv_type_2b_b2b or inv_type_2b_cdnr)
    )

    state_data = {
        "combined_df": combined_df,
        "pair_cols": pair_cols,
        # Save raw dataframes needed for ITC calc / Comments sheets
        "df_pr_raw": df_pr_raw,
        "df_b2b_raw": df_b2b_raw if 'df_b2b_raw' in locals() else None,
        "df_cdnr_raw": df_cdnr_raw if 'df_cdnr_raw' in locals() else None,
        "df_impg_raw": df_impg_raw if 'df_impg_raw' in locals() else None,
        "df_b2b_raw_signed": df_b2b_raw_signed if 'df_b2b_raw_signed' in locals() else None,
        "df_cdnr_raw_signed": df_cdnr_raw_signed if 'df_cdnr_raw_signed' in locals() else None,
        "df_impg_raw_signed": df_impg_raw_signed if 'df_impg_raw_signed' in locals() else None,
        "df_b2b_imports": df_b2b_imports if 'df_b2b_imports' in locals() else None,

        # Save Lookups
        "row_num_lookup": row_num_lookup,
        "pr_row_num_lookup": pr_row_num_lookup,

        # Save Column Selections needed for ITC logic
        "gst_2b_b2b": gst_2b_b2b, "inv_2b_b2b": inv_2b_b2b, "date_2b_b2b": date_2b_b2b,
        "gst_2b_cdnr": gst_2b_cdnr, "note_2b_cdnr": note_2b_cdnr, "notedate_2b_cdnr": notedate_2b_cdnr,
        "gst_pr": gst_pr, "inv_pr": inv_pr,
        "target_return_period": target_return_period,

        # Save columns for specific checks
        "cgst_pr": cgst_pr, "sgst_pr": sgst_pr, "igst_pr": igst_pr
    }

    return state_data

def _generate_final_outputs(state_data):
    # Unpack variables
    combined_df = state_data["combined_df"]

    # --- FIX 1: Deduplicate columns immediately to prevent crashes ---
    # This keeps the first occurrence of any duplicate column name and drops the rest.
    combined_df = combined_df.loc[:, ~combined_df.columns.duplicated()]
    # -----------------------------------------------------------------

    pair_cols = state_data["pair_cols"]
    df_pr_raw = state_data["df_pr_raw"]
    df_b2b_raw = state_data["df_b2b_raw"]
    df_cdnr_raw = state_data["df_cdnr_raw"]
    df_impg_raw = state_data["df_impg_raw"]
    df_b2b_raw_signed = state_data["df_b2b_raw_signed"]
    df_cdnr_raw_signed = state_data["df_cdnr_raw_signed"]
    df_impg_raw_signed = state_data["df_impg_raw_signed"]
    df_b2b_imports = state_data["df_b2b_imports"]
    row_num_lookup = state_data["row_num_lookup"]
    pr_row_num_lookup = state_data["pr_row_num_lookup"]

    gst_2b_b2b = state_data["gst_2b_b2b"]
    inv_2b_b2b = state_data["inv_2b_b2b"]
    date_2b_b2b = state_data["date_2b_b2b"]
    gst_2b_cdnr = state_data["gst_2b_cdnr"]
    note_2b_cdnr = state_data["note_2b_cdnr"]
    notedate_2b_cdnr = state_data["notedate_2b_cdnr"]
    gst_pr = state_data["gst_pr"]
    inv_pr = state_data["inv_pr"]
    target_return_period = state_data["target_return_period"]

    # Retrieve tax columns if needed for dashboard logic (using safe gets)
    cgst_pr = state_data.get("cgst_pr")
    sgst_pr = state_data.get("sgst_pr")
    igst_pr = state_data.get("igst_pr")

    # ------------------ ITC COMPUTATION BY CODE ------------------

    # 1. Build Lookup Dictionary
    recon_lookup = {}
    try:
        if combined_df is not None and not combined_df.empty:
            for _, r in combined_df.iterrows():
                k = (as_text(r.get("_GST_KEY", "")), as_text(r.get("_INV_KEY", "")))
                recon_lookup[k] = (r.get("Mapping", ""), r.get("Remarks", ""), r.get("Reason", ""))
    except Exception as e:
        app.logger.error(f"Failed to build recon_lookup: {e}")

    # 2. Helper Functions
    def sum_column(df, col):
        if df is None or df.empty or not col: return 0.0
        if col not in df.columns: return 0.0
        try: return float(df[col].astype(str).map(parse_amount).sum())
        except: return 0.0

    def pick_tax_cols_for_sheet(df):
        ig = _find_optional_col(df, [IGST_CANDIDATES_2B]) or (_pick_col_contains(df, r"igst|integrated") if df is not None else None)
        cg = _find_optional_col(df, [CGST_CANDIDATES_2B]) or (_pick_col_contains(df, r"cgst|central") if df is not None else None)
        sg = _find_optional_col(df, [SGST_CANDIDATES_2B]) or (_pick_col_contains(df, r"sgst|state|utgst") if df is not None else None)
        return ig, cg, sg

    def _get_itc_col(df):
        if df is None or df.empty: return None
        # Priority 1: Exact match for "ITC Availability"
        for col in df.columns:
            if "itc availability" in str(col).lower(): return col
        # Priority 2: "ITC Available" (common variation)
        for col in df.columns:
            if "itc available" in str(col).lower(): return col
        return None

    # Identify RCM and ITC columns
    def _get_col_by_keywords(df, keywords):
        if df is None or df.empty: return None
        for col in df.columns:
            c_norm = str(col).lower().strip()
            if any(k in c_norm for k in keywords): return col
        return None

    rc_col_b2b = _get_col_by_keywords(df_b2b_raw, ["reverse charge", "attract reverse charge"])

    # STRICT ITC Column Search
    itc_col_b2b = _get_itc_col(df_b2b_raw)
    itc_col_cdnr = _get_itc_col(df_cdnr_raw)

    # 3. Filtered Summation Logic
    def calc_filtered_sum(df_signed, df_raw, filter_logic="all", mapping_status_check=False):
        if df_signed is None or df_signed.empty: return 0.0, 0.0, 0.0

        ig, cg, sg = pick_tax_cols_for_sheet(df_signed)
        is_b2b = "_SOURCE_SHEET" in df_raw.columns and df_raw["_SOURCE_SHEET"].iloc[0] == "B2B"

        gst_key_col = gst_2b_b2b if is_b2b else gst_2b_cdnr
        inv_key_col = inv_2b_b2b if is_b2b else note_2b_cdnr

        # Use the correct ITC column for the current sheet
        current_itc_col = itc_col_b2b if is_b2b else itc_col_cdnr

        total_i, total_c, total_s = 0.0, 0.0, 0.0

        try:
            # Reset indexes
            df_r = df_raw.reset_index(drop=True)
            df_s = df_signed.reset_index(drop=True)

            for idx, row in df_s.iterrows():
                if idx >= len(df_r): break
                raw_row = df_r.iloc[idx]

                if mapping_status_check:
                    g = clean_gstin(raw_row.get(gst_key_col, ""))
                    i = inv_basic(raw_row.get(inv_key_col, ""))
                    status = str(recon_lookup.get((g, i), ("",))[0]).strip().lower()
                    if status != "not matched": continue

                if filter_logic == "rcm_yes":
                    if not rc_col_b2b: continue
                    val = str(raw_row.get(rc_col_b2b, "")).lower().strip()
                    if val not in ["yes", "y"]: continue

                elif filter_logic == "itc_no":
                    if not current_itc_col: continue
                    val = str(raw_row.get(current_itc_col, "")).lower().strip()
                    # STRICT CHECK: Only "No"
                    if val not in ["no", "n", "ineligible"]: continue

                total_i += parse_amount(row.get(ig, 0))
                total_c += parse_amount(row.get(cg, 0))
                total_s += parse_amount(row.get(sg, 0))
        except: return 0.0, 0.0, 0.0

        return total_i, total_c, total_s

    # --- 4A & 4B Calculations ---

    # 4A1
    impg_ig, _, _ = pick_tax_cols_for_sheet(df_impg_raw)
    val_4a1_flat = sum_column(df_b2b_imports, pick_tax_cols_for_sheet(df_b2b_imports)[0]) if (df_b2b_imports is not None and not df_b2b_imports.empty) else 0.0
    val_4a1_igst = sum_column(df_impg_raw_signed, impg_ig) + val_4a1_flat

    # 4A3 (RCM)
    val_4a3_igst, val_4a3_cgst, val_4a3_sgst = calc_filtered_sum(df_b2b_raw_signed, df_b2b_raw, "rcm_yes")

    # 4A5 (Net ITC)
    b2b_i, b2b_c, b2b_s = calc_filtered_sum(df_b2b_raw_signed, df_b2b_raw, "all")
    cdnr_i, cdnr_c, cdnr_s = calc_filtered_sum(df_cdnr_raw_signed, df_cdnr_raw, "all")

    # Ineligible B2B
    itc_no_i, itc_no_c, itc_no_s = calc_filtered_sum(df_b2b_raw_signed, df_b2b_raw, "itc_no")

    val_4a5_igst = (b2b_i + cdnr_i) - val_4a3_igst - itc_no_i
    val_4a5_cgst = (b2b_c + cdnr_c) - val_4a3_cgst - itc_no_c
    val_4a5_sgst = (b2b_s + cdnr_s) - val_4a3_sgst - itc_no_s

    # 4B2 (Not Matched Logic)
    nm_b2b_i, nm_b2b_c, nm_b2b_s = calc_filtered_sum(df_b2b_raw_signed, df_b2b_raw, "all", True)
    nm_cdnr_i, nm_cdnr_c, nm_cdnr_s = calc_filtered_sum(df_cdnr_raw_signed, df_cdnr_raw, "all", True)
    nm_rcm_i, nm_rcm_c, nm_rcm_s = calc_filtered_sum(df_b2b_raw_signed, df_b2b_raw, "rcm_yes", True)
    nm_itc_no_i, nm_itc_no_c, nm_itc_no_s = calc_filtered_sum(df_b2b_raw_signed, df_b2b_raw, "itc_no", True)

    val_4b2_igst = (nm_b2b_i + nm_cdnr_i) - nm_rcm_i - nm_itc_no_i
    val_4b2_cgst = (nm_b2b_c + nm_cdnr_c) - nm_rcm_c - nm_itc_no_c
    val_4b2_sgst = (nm_b2b_s + nm_cdnr_s) - nm_rcm_s - nm_itc_no_s

    # --- 4D2 (Ineligible ITC: B2B No + CDNR No) ---
    # Calculate CDNR Ineligible (ITC No).
    itc_no_cdnr_i, itc_no_cdnr_c, itc_no_cdnr_s = calc_filtered_sum(df_cdnr_raw_signed, df_cdnr_raw, "itc_no")

    # Sum B2B No + CDNR No
    val_4d2_igst = itc_no_i + itc_no_cdnr_i
    val_4d2_cgst = itc_no_c + itc_no_cdnr_c
    val_4d2_sgst = itc_no_s + itc_no_cdnr_s

    # Construct Rows
    row_4a1 = {"integrated": val_4a1_igst, "central": 0.0, "state": 0.0, "cess": 0.0}
    row_4a2 = {"integrated": 0.0, "central": 0.0, "state": 0.0, "cess": 0.0}
    row_4a3 = {"integrated": val_4a3_igst, "central": val_4a3_cgst, "state": val_4a3_sgst, "cess": 0.0}
    row_4a4 = {"integrated": 0.0, "central": 0.0, "state": 0.0, "cess": 0.0}
    row_4a5 = {"integrated": val_4a5_igst, "central": val_4a5_cgst, "state": val_4a5_sgst, "cess": 0.0}
    row_4b1 = {"integrated": 0.0, "central": 0.0, "state": 0.0, "cess": 0.0}
    row_4b2 = {"integrated": val_4b2_igst, "central": val_4b2_cgst, "state": val_4b2_sgst, "cess": 0.0}
    row_4d2 = {"integrated": val_4d2_igst, "central": val_4d2_cgst, "state": val_4d2_sgst, "cess": 0.0}

    def sum_rows(rows, key): return sum((r.get(key, 0.0) for r in rows), 0.0)

    row_4a = {k: sum_rows([row_4a1, row_4a2, row_4a3, row_4a4, row_4a5], k) for k in ["integrated", "central", "state", "cess"]}
    row_4b = {k: sum_rows([row_4b1, row_4b2], k) for k in ["integrated", "central", "state", "cess"]}
    row_4c = {k: row_4a[k] - row_4b[k] for k in ["integrated", "central", "state", "cess"]}

    # --- 4D1 CALCULATION ---
    val_4d1_i = val_4d1_c = val_4d1_s = 0.0

    if target_return_period and target_return_period.strip():
        try:
            parts = target_return_period.strip().split('-')
            user_dt = datetime(int(parts[0]), int(parts[1]), 1)

            def _loc_filing_col(df):
                for c in df.columns:
                    if "filing" in str(c).lower() and "date" in str(c).lower() and "period" not in str(c).lower(): return c
                return None

            def sum_prev(df_signed, df_raw):
                i_t, c_t, s_t = 0.0, 0.0, 0.0
                if df_signed is None or df_signed.empty: return 0,0,0

                ig, cg, sg = pick_tax_cols_for_sheet(df_signed)
                df_r = df_raw.reset_index(drop=True)
                df_s = df_signed.reset_index(drop=True)

                f_col = _loc_filing_col(df_raw)

                for idx, row in df_s.iterrows():
                    if idx >= len(df_r): break
                    raw_row = df_r.iloc[idx]
                    d = None
                    if f_col: d = parse_date_cell(raw_row.get(f_col))
                    if not d:
                        is_b2b = "_SOURCE_SHEET" in df_raw.columns and df_raw["_SOURCE_SHEET"].iloc[0] == "B2B"
                        dc = date_2b_b2b if is_b2b else notedate_2b_cdnr
                        d = parse_date_cell(raw_row.get(dc))

                    if d:
                        row_tp = datetime(d.year, d.month, 1) if d.day >= 12 else (datetime(d.year, d.month, 1) - timedelta(days=1)).replace(day=1)
                        if row_tp < user_dt:
                            i_t += parse_amount(row.get(ig, 0))
                            c_t += parse_amount(row.get(cg, 0))
                            s_t += parse_amount(row.get(sg, 0))
                return i_t, c_t, s_t

            p_b2b_i, p_b2b_c, p_b2b_s = sum_prev(df_b2b_raw_signed, df_b2b_raw)
            p_cdnr_i, p_cdnr_c, p_cdnr_s = sum_prev(df_cdnr_raw_signed, df_cdnr_raw)

            val_4d1_i = p_b2b_i + p_cdnr_i
            val_4d1_c = p_b2b_c + p_cdnr_c
            val_4d1_s = p_b2b_s + p_cdnr_s

        except Exception as e:
            app.logger.error(f"4D1 Calc Error: {e}")

    row_4d1 = {"integrated": val_4d1_i, "central": val_4d1_c, "state": val_4d1_s, "cess": 0.0}

    itc_values_by_code = {
        "4A":  row_4a,
        "4A1": row_4a1,
        "4A2": row_4a2,
        "4A3": row_4a3,
        "4A4": row_4a4,
        "4A5": row_4a5,
        "4B":  row_4b,
        "4B1": row_4b1,
        "4B2": row_4b2,
        "4C":  row_4c,
        "4D":  {"integrated": 0.0, "central": 0.0, "state": 0.0, "cess": 0.0},
        "4D1": row_4d1,
        "4D2": row_4d2,
    }
    # ------------------ END ITC COMPUTATION ------------------

    # Rebuild Lookup for Comments
    recon_lookup = {}
    for _, row in combined_df.iterrows():
        key = (as_text(row.get("_GST_KEY", "")), as_text(row.get("_INV_KEY", "")))
        recon_lookup[key] = (row.get("Mapping", ""), row.get("Remarks", ""), row.get("Reason", ""))

    pr_comments = df_pr_raw.copy()

    if gst_pr in pr_comments.columns and inv_pr in pr_comments.columns:
        pr_comments["_GST_KEY"] = pr_comments[gst_pr].map(clean_gstin)
        pr_comments["_INV_KEY"] = pr_comments[inv_pr].map(inv_basic)

        def get_recon_status(row):
            key = (row["_GST_KEY"], row["_INV_KEY"])
            mapping, remarks, reason = recon_lookup.get(key, ("", "", ""))
            row_num_2b = ""
            if mapping in ["Matched", "Almost Matched"]:
                row_num_2b = row_num_lookup.get(key, "")
            return pd.Series([mapping, remarks, reason, row_num_2b])

        pr_comments[["Mapping", "Remarks", "Reason", "Row number in 2B"]] = pr_comments.apply(get_recon_status, axis=1)
        pr_comments.drop(columns=["_GST_KEY", "_INV_KEY"], inplace=True)
    else:
        pr_comments["Mapping"] = ""
        pr_comments["Remarks"] = ""
        pr_comments["Reason"] = ""
        pr_comments["Row number in 2B"] = ""

    from collections import Counter

    def _parse_month_year_string(s):
        if s is None: return None
        s = str(s).strip()
        if not s: return None
        s_clean = re.sub(r'[^A-Za-z0-9\s\/\-\:]', ' ', s).strip()
        fmts = ["%b %Y", "%B %Y", "%m/%Y", "%m-%Y", "%Y-%m", "%b-%Y", "%B-%Y", "%m %Y", "%Y %b", "%Y %B"]
        for fmt in fmts:
            try:
                dt = datetime.strptime(s_clean, fmt)
                return (dt.year, dt.month)
            except Exception: pass
        m = re.search(r'(\d{2})\s*[/\-]?\s*(\d{4})$', s)
        if m:
            mo = int(m.group(1)); yr = int(m.group(2))
            if 1 <= mo <= 12: return (yr, mo)
        m2 = re.search(r'(\d{4})\s*[/\-]?\s*(\d{2})$', s)
        if m2:
            yr = int(m2.group(1)); mo = int(m2.group(2))
            if 1 <= mo <= 12: return (yr, mo)
        try:
            dt = pd.to_datetime(s, errors='coerce')
            if not pd.isna(dt): return (int(dt.year), int(dt.month))
        except Exception: pass
        return None

    def _find_exact_filing_date_col(df):
        if df is None or df.empty: return None
        target_keywords = ["filing date", "filingdate", "date of filing"]
        for c in df.columns:
            cn = str(c).lower()
            cn_clean = re.sub(r'[^a-z0-9]', '', cn)
            if "date" in cn_clean:
                if "filing" in cn_clean or "gstr1" in cn_clean or "iff" in cn_clean:
                    if "period" not in cn_clean: return c
        for c in df.columns:
            cn = str(c).lower()
            if "filing" in cn and "date" in cn and "period" not in cn: return c
        return None

    def _tax_period_from_filing_date_cell(v):
        try:
            d = parse_date_cell(v)
            if not d: return ""
            y, m = int(d.year), int(d.month)
            if d.day >= 12: pass
            else:
                if m == 1: y, m = y - 1, 12
                else: y, m = y, m - 1
            return datetime(year=y, month=m, day=1).strftime("%b %Y")
        except Exception: return ""

    # Build gstr2b_comments (B2B)
    try:
        gstr2b_comments = df_b2b_raw.copy() if (df_b2b_raw is not None and not df_b2b_raw.empty) else pd.DataFrame()
    except Exception:
        gstr2b_comments = pd.DataFrame()

    def _get_b2b_row_status(row):
        if not gst_2b_b2b or not inv_2b_b2b: return pd.Series(["", "", "", ""])
        if gst_2b_b2b not in row or inv_2b_b2b not in row: return pd.Series(["", "", "", ""])
        try:
            gst_val = clean_gstin(row[gst_2b_b2b])
            inv_val = inv_basic(row[inv_2b_b2b])
            key = (as_text(gst_val), as_text(inv_val))
            mapping, remarks, reason = recon_lookup.get(key, ("", "", ""))
            pr_row = ""
            if mapping in ["Matched", "Almost Matched"]:
                pr_row = pr_row_num_lookup.get(key, "")
            return pd.Series([mapping, remarks, reason, pr_row])
        except Exception:
            return pd.Series(["", "", "", ""])

    if gstr2b_comments is not None:
        for _col in ["Mapping", "Remarks", "Reason", "Row Number in PR", "Tax Period"]:
            if _col not in gstr2b_comments.columns: gstr2b_comments[_col] = ""

    if not gstr2b_comments.empty:
        try:
            gstr2b_comments[["Mapping", "Remarks", "Reason", "Row Number in PR"]] = gstr2b_comments.apply(_get_b2b_row_status, axis=1)
        except Exception: pass

    # Build gstr2b_cdnr_comments (CDNR)
    try:
        gstr2b_cdnr_comments = df_cdnr_raw.copy() if (df_cdnr_raw is not None and not df_cdnr_raw.empty) else pd.DataFrame()
    except Exception:
        gstr2b_cdnr_comments = pd.DataFrame()

    def _get_cdnr_row_status(row):
        if not gst_2b_cdnr or not note_2b_cdnr: return pd.Series(["", "", "", ""])
        if gst_2b_cdnr not in row or note_2b_cdnr not in row: return pd.Series(["", "", "", ""])
        try:
            gst_val = clean_gstin(row[gst_2b_cdnr])
            note_val = inv_basic(row[note_2b_cdnr])
            key = (as_text(gst_val), as_text(note_val))
            mapping, remarks, reason = recon_lookup.get(key, ("", "", ""))
            pr_row = ""
            if mapping in ["Matched", "Almost Matched"]:
                pr_row = pr_row_num_lookup.get(key, "")
            return pd.Series([mapping, remarks, reason, pr_row])
        except Exception:
            return pd.Series(["", "", "", ""])

    if gstr2b_cdnr_comments is not None:
        for _col in ["Mapping", "Remarks", "Reason", "Row Number in PR", "Tax Period"]:
            if _col not in gstr2b_cdnr_comments.columns: gstr2b_cdnr_comments[_col] = ""

    if not gstr2b_cdnr_comments.empty:
        try:
            gstr2b_cdnr_comments[["Mapping", "Remarks", "Reason", "Row Number in PR"]] = gstr2b_cdnr_comments.apply(_get_cdnr_row_status, axis=1)
        except Exception: pass

    # Tax Period Logic
    filing_col_b2b = _find_exact_filing_date_col(gstr2b_comments)
    if "Tax Period" not in gstr2b_comments.columns: gstr2b_comments["Tax Period"] = ""
    if not gstr2b_comments.empty:
        if filing_col_b2b and filing_col_b2b in gstr2b_comments.columns:
            try: gstr2b_comments["Tax Period"] = gstr2b_comments[filing_col_b2b].map(_tax_period_from_filing_date_cell)
            except Exception: pass
        else:
            if date_2b_b2b and date_2b_b2b in gstr2b_comments.columns:
                try: gstr2b_comments["Tax Period"] = gstr2b_comments[date_2b_b2b].map(_tax_period_from_filing_date_cell)
                except Exception: pass

    filing_col_cdnr = _find_exact_filing_date_col(gstr2b_cdnr_comments)
    if "Tax Period" not in gstr2b_cdnr_comments.columns: gstr2b_cdnr_comments["Tax Period"] = ""
    if not gstr2b_cdnr_comments.empty:
        if filing_col_cdnr and filing_col_cdnr in gstr2b_cdnr_comments.columns:
            try: gstr2b_cdnr_comments["Tax Period"] = gstr2b_cdnr_comments[filing_col_cdnr].map(_tax_period_from_filing_date_cell)
            except Exception: pass
        else:
            if notedate_2b_cdnr and notedate_2b_cdnr in gstr2b_cdnr_comments.columns:
                try: gstr2b_cdnr_comments["Tax Period"] = gstr2b_cdnr_comments[notedate_2b_cdnr].map(_tax_period_from_filing_date_cell)
                except Exception: pass

    # Clean Cols
    cols = list(combined_df.columns)
    if "Invoice Type" in cols:
        idx = cols.index("_INV_KEY") + 1 if "_INV_KEY" in cols else 5
        cols.insert(idx, cols.pop(cols.index("Invoice Type")))
        combined_df = combined_df[cols]

    def _format_and_autosize(ws, df, num_format, text_format):
        numeric_keywords = ["tax", "value", "amount", "cess", "cgst", "sgst", "igst", "rate", "total", "cash", "diff"]
        exclude_cols = ["tax period", "filing period", "gstr-1 period", "return period", "invoice date", "filing date", "note date", "document date", "id", "note number", "note no", "note type", "supply type", "invoice no", "invoice number", "document number"]
        for col_idx, col_name in enumerate(df.columns):
            col_str = str(col_name).lower()
            header_len = len(str(col_name)) + 4
            try:
                sample = df[col_name].head(200).astype(str)
                content_len = int(sample.map(len).max())
            except Exception: content_len = 0
            width = max(12, min(48, max(header_len, content_len + 2)))
            is_numeric_col = any(k in col_str for k in numeric_keywords)
            is_excluded = any(ex in col_str for ex in exclude_cols)
            if is_numeric_col and not is_excluded:
                ws.set_column(col_idx, col_idx, width, num_format)
            else:
                ws.set_column(col_idx, col_idx, width, text_format)

    def _apply_header_style(ws, df, header_fmt):
        for col_idx, col_name in enumerate(df.columns):
            ws.write(0, col_idx, col_name, header_fmt)

    def _fix_datetime_formatting(df):
        if df.empty: return df
        df = df.copy()
        for col in df.columns:
            cn = str(col).lower()
            if ("period" in cn or "month" in cn) and "invoice date" not in cn:
                try:
                    s = pd.to_datetime(df[col], errors='coerce')
                    if s.notna().sum() > 0: df.loc[s.notna(), col] = s[s.notna()].dt.strftime("%b-%Y")
                except: pass
            elif "filing date" in cn:
                try:
                    s = pd.to_datetime(df[col], errors='coerce')
                    if s.notna().sum() > 0: df.loc[s.notna(), col] = s[s.notna()].dt.strftime("%d-%m-%Y")
                except: pass
        return df

    def _ensure_numeric_types(df):
        if df.empty: return df
        df = df.copy()
        numeric_keywords = ["tax", "value", "amount", "cess", "cgst", "sgst", "igst", "rate", "total", "cash", "diff"]
        exclude_cols = ["tax period", "filing period", "gstr-1 period", "return period", "invoice date", "filing date", "note date", "document date", "id", "note number", "note no", "note type", "supply type", "invoice no", "invoice number", "document number"]
        for col in df.columns:
            cn = str(col).lower()
            if any(k in cn for k in numeric_keywords) and not any(ex in cn for ex in exclude_cols):
                try:
                    df[col] = (df[col].astype(str).str.replace(',', '').str.replace('nan', '', case=False).str.strip())
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
                except: pass
        return df

    if pr_comments is not None: pr_comments = _fix_datetime_formatting(pr_comments)
    if gstr2b_comments is not None: gstr2b_comments = _fix_datetime_formatting(gstr2b_comments)
    if gstr2b_cdnr_comments is not None: gstr2b_cdnr_comments = _fix_datetime_formatting(gstr2b_cdnr_comments)
    df_b2b_raw_export = _ensure_numeric_types(df_b2b_raw) if (df_b2b_raw is not None and not df_b2b_raw.empty) else pd.DataFrame()
    df_cdnr_raw_export = _ensure_numeric_types(df_cdnr_raw) if (df_cdnr_raw is not None and not df_cdnr_raw.empty) else pd.DataFrame()
    pr_comments = _ensure_numeric_types(pr_comments)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter", engine_kwargs={"options": {"strings_to_urls": False}}) as writer:
        wb = writer.book
        header_fmt = wb.add_format({"bold": True, "bg_color": "#FCE4D6", "border": 1, "font_name": "Aptos", "font_size": 10, "valign": "vcenter", "align": "left"})
        text_fmt = wb.add_format({"font_name": "Aptos", "font_size": 10, "valign": "vcenter", "align": "left"})
        num_fmt = wb.add_format({"num_format": "#,##0.00", "align": "right", "valign": "vcenter", "font_name": "Aptos", "font_size": 10})
        comment_highlight_fmt = wb.add_format({"bg_color": "#FFF2CC", "align": "left", "valign": "vcenter", "font_name": "Aptos", "font_size": 10})
        dash_title_fmt = wb.add_format({"bold": True, "align": "center", "valign": "vcenter", "font_size": 12, "font_name": "Aptos"})
        dash_table_hdr = wb.add_format({"bold": True, "align": "center", "valign": "vcenter", "border": 1, "bg_color": "#FCE4D6", "font_name": "Aptos", "font_size": 10})
        dash_table_txt = wb.add_format({"border": 1, "align": "left", "valign": "vcenter", "font_name": "Aptos", "font_size": 10})
        dash_table_num = wb.add_format({"num_format": "#,##0.00", "border": 1, "align": "right", "valign": "vcenter", "font_name": "Aptos", "font_size": 10})
        dash_table_lbl = wb.add_format({"bold": True, "border": 1, "align": "left", "valign": "vcenter", "font_name": "Aptos", "font_size": 10})
        note_title_fmt = wb.add_format({"bold": True, "font_size": 11, "valign": "top", "underline": True, "font_name": "Aptos"})
        note_text_fmt = wb.add_format({"text_wrap": True, "valign": "top", "font_size": 10, "font_name": "Aptos"})

        def _highlight_columns(ws, df, col_names, fmt, include_header: bool = True):
            try:
                if df is None: return
                nrows = len(df)
                last_row = nrows
                start_row = 0 if include_header else 1
                for col_name in col_names:
                    if col_name in df.columns:
                        col_idx = int(df.columns.get_loc(col_name))
                        ws.conditional_format(start_row, col_idx, last_row, col_idx, {"type": "no_errors", "format": fmt})
            except Exception: pass

        # DASHBOARD
        ws2 = wb.add_worksheet("Dashboard")
        writer.sheets["Dashboard"] = ws2
        statuses = ["Matched", "Almost Matched", "Not Matched", None]
        rowlabels = ["CGST", "SGST", "IGST", "Total"]

        def _sum_component(status, col_name):
            if not col_name or col_name not in combined_df.columns: return 0.0
            mask = (combined_df["Mapping"] == status) if status else slice(None)

            # --- FIX 2: Handle duplicate column access cleanly ---
            val = combined_df.loc[mask, col_name]
            if isinstance(val, pd.DataFrame):
                val = val.iloc[:, 0]
            # ----------------------------------------------------

            s = val.astype(str).str.replace(',', '').str.replace('nan', '')
            return float(pd.to_numeric(s, errors="coerce").fillna(0).sum())

        def _block_vals(status):
            cg_pr_val = _sum_component(status, pair_cols.get("cgst_pr_col"))
            sg_pr_val = _sum_component(status, pair_cols.get("sgst_pr_col"))
            ig_pr_val = _sum_component(status, pair_cols.get("igst_pr_col"))
            tot_pr = cg_pr_val + sg_pr_val + ig_pr_val
            cg_2b_val = _sum_component(status, pair_cols.get("cgst_2b_col"))
            sg_2b_val = _sum_component(status, pair_cols.get("sgst_2b_col"))
            ig_2b_val = _sum_component(status, pair_cols.get("igst_2b_col"))
            tot_2b = cg_2b_val + sg_2b_val + ig_2b_val
            return ([cg_pr_val, sg_pr_val, ig_pr_val, tot_pr], [cg_2b_val, sg_2b_val, ig_2b_val, tot_2b])

        total_cols = 1 + len(statuses) * 2
        ws2.merge_range(0, 0, 0, total_cols - 1, "Summary - GSTR 2B vs. Purchase Register Reconciliation", dash_title_fmt)
        top_row = 2; sub_row = top_row + 1; data_start = top_row + 2
        ws2.write(top_row, 0, "Status", dash_table_hdr)
        col = 1
        for st in statuses:
            title = "Total" if st is None else st
            ws2.merge_range(top_row, col, top_row, col+1, title, dash_table_hdr)
            col += 2
        ws2.write(sub_row, 0, "Report", dash_table_hdr)
        col = 1
        for _ in statuses:
            ws2.write(sub_row, col, "PR", dash_table_hdr); ws2.write(sub_row, col + 1, "GSTR 2B", dash_table_hdr)
            col += 2
        for r, label in enumerate(rowlabels, start=data_start): ws2.write(r, 0, label, dash_table_lbl)
        col = 1
        for st in statuses:
            pr_vals, b2_vals = _block_vals(st)
            for r, v in enumerate(pr_vals, start=data_start): ws2.write_number(r, col, v, dash_table_num)
            for r, v in enumerate(b2_vals, start=data_start): ws2.write_number(r, col+1, v, dash_table_num)
            col += 2
        note_row_idx = data_start + len(rowlabels)
        ws2.merge_range(note_row_idx, 0, note_row_idx, total_cols - 1, "Note: The above table summary includes B2B (including RCM and POS) and CDNR values from GSTR 2B. Does not include imports or ISD", wb.add_format({"italic": True, "font_color": "red", "align": "left", "font_name": "Aptos", "font_size": 10}))
        ws2.freeze_panes(data_start, 0)
        ws2.set_column(0, 0, 14)
        for c in range(1, total_cols): ws2.set_column(c, c, 15)

        new_table_start = note_row_idx + 3
        ws2.merge_range(new_table_start, 0, new_table_start, 5, "ITC for GSTR 3B (for reference purposes only)", dash_title_fmt)
        itc_headers = ["Details", "Code", "Integrated Tax (₹)", "Central Tax (₹)", "State/UT Tax (₹)", "CESS (₹)"]
        for ci, h in enumerate(itc_headers): ws2.write(new_table_start + 1, ci, h, dash_table_hdr)
        itc_rows = [("(A) ITC Available (whether in full or part)", "4A"), ("(1) Import of goods", "4A1"), ("(2) Import of services", "4A2"), ("(3) Inward supplies liable to reverse charge (other than 1 & 2 above)", "4A3"), ("(4) Inward supplies from ISD", "4A4"), ("(5) All other ITC", "4A5"), ("(B) ITC Reversed", "4B"), ("(1) As per rules 38,42 & 43 of CGST Rules and section 17(5)", "4B1"), ("(2) Others", "4B2"), ("(C) Net ITC Available (A) - (B)", "4C"), ("(D) Other Details", "4D"), ("(1) ITC reclaimed which was reversed under Table 4(B)(2) in earlier tax period", "4D1"), ("(2) Ineligible ITC under section 16(4) & ITC restricted due to PoS rules", "4D2")]
        row_idx = new_table_start + 2
        for details, code in itc_rows:
            is_header_row = code in ["4A", "4B", "4C", "4D"]
            txt_fmt = dash_table_lbl if is_header_row else dash_table_txt
            ws2.write(row_idx, 0, details, txt_fmt)
            ws2.write(row_idx, 1, code, txt_fmt)
            v = itc_values_by_code.get(code, {"integrated": 0.0, "central": 0.0, "state": 0.0, "cess": 0.0})
            ws2.write_number(row_idx, 2, float(v.get("integrated", 0.0)), dash_table_num)
            ws2.write_number(row_idx, 3, float(v.get("central", 0.0)), dash_table_num)
            ws2.write_number(row_idx, 4, float(v.get("state", 0.0)), dash_table_num)
            ws2.write_number(row_idx, 5, float(v.get("cess", 0.0)), dash_table_num)
            row_idx += 1
        ws2.set_column(0, 0, 50); ws2.set_column(1, 5, 16)

        notes_col = 7; notes_start_row = new_table_start + 1
        ws2.write(notes_start_row, notes_col, "Notes:", note_title_fmt)
        itc_notes_list = ["1. 4A1 (Import of goods): Data populated from the IMPG sheet in GSTR 2B.", "2. 4A2 (Import of Services): User to input.", "3. 4A3 (Domestic Reverse Charge): Data populated from the reverse charge column of the B2B sheet in GSTR 2B.", "4. 4A4 (ISD): Data populated from the ISD sheet in GSTR 2B.", "5. 4A5 (Net ITC): Data populated from B2B and B2B-CDNR sheet in GSTR 2B without reverse charge.", "6. 4B1 (Permanent Reversal): User to input.", "7. 4B2 (Temporary Reverse): Data populated from the Not Matched line items.", "8. 4D1 (Past Period ITC): Data populated from the Matched and Almost Matched line items for lines matching with past period GSTR 2B."]
        for i, note in enumerate(itc_notes_list): ws2.merge_range(notes_start_row + 1 + i, notes_col, notes_start_row + 1 + i, notes_col + 10, note, note_text_fmt)

        # RECONCILIATION SHEET
        combined_df.to_excel(writer, index=False, sheet_name="Reconciliation")
        ws = writer.sheets["Reconciliation"]
        ws.freeze_panes(1, 0)
        ws.autofilter(0, 0, len(combined_df), max(0, combined_df.shape[1] - 1))
        _apply_header_style(ws, combined_df, header_fmt)
        _format_and_autosize(ws, combined_df, num_fmt, text_fmt)
        _highlight_columns(ws, combined_df, ["Mapping", "Remarks", "Reason"], comment_highlight_fmt)

        # PR Comments
        pr_comments.to_excel(writer, index=False, sheet_name="PR - Comments")
        ws3 = writer.sheets["PR - Comments"]
        ws3.freeze_panes(1, 0)
        ws3.autofilter(0, 0, len(pr_comments), max(0, pr_comments.shape[1] - 1))
        _apply_header_style(ws3, pr_comments, header_fmt)
        _format_and_autosize(ws3, pr_comments, num_fmt, text_fmt)
        _highlight_columns(ws3, pr_comments, ["Mapping", "Remarks", "Reason"], comment_highlight_fmt)

        # B2B Comments
        try:
            to_write = gstr2b_comments.copy() if not gstr2b_comments.empty else df_b2b_raw_export.copy()
            for _col in ["Mapping", "Remarks", "Reason", "Row Number in PR"]:
                if _col not in to_write.columns: to_write[_col] = ""
            orig_cols = [c for c in to_write.columns if c not in ["Mapping", "Remarks", "Reason", "Row Number in PR"]]
            final_cols = orig_cols + [c for c in ["Mapping", "Remarks", "Reason", "Row Number in PR", "Tax Period"] if c in to_write.columns]
            to_write = to_write.reindex(columns=final_cols)
            to_write.to_excel(writer, index=False, sheet_name="GSTR 2B B2B - Comments")
            ws4 = writer.sheets["GSTR 2B B2B - Comments"]
            ws4.freeze_panes(1, 0)
            ws4.autofilter(0, 0, len(to_write), max(0, to_write.shape[1] - 1))
            _apply_header_style(ws4, to_write, header_fmt)
            _format_and_autosize(ws4, to_write, num_fmt, text_fmt)
            _highlight_columns(ws4, to_write, ["Mapping", "Remarks", "Reason"], comment_highlight_fmt)
        except Exception: app.logger.exception("Could not write B2B sheet to output workbook")

        # CDNR Comments
        try:
            to_write_cd = gstr2b_cdnr_comments.copy() if not gstr2b_cdnr_comments.empty else df_cdnr_raw_export.copy()
            for _col in ["Mapping", "Remarks", "Reason", "Row Number in PR"]:
                if _col not in to_write_cd.columns: to_write_cd[_col] = ""
            orig_cd_cols = [c for c in to_write_cd.columns if c not in ["Mapping", "Remarks", "Reason", "Row Number in PR"]]
            final_cd_cols = orig_cd_cols + [c for c in ["Mapping", "Remarks", "Reason", "Row Number in PR", "Tax Period"] if c in to_write_cd.columns]
            to_write_cd = to_write_cd.reindex(columns=final_cd_cols)
            to_write_cd.to_excel(writer, index=False, sheet_name="GSTR 2B CDNR - Comments")
            ws5 = writer.sheets["GSTR 2B CDNR - Comments"]
            ws5.freeze_panes(1, 0)
            ws5.autofilter(0, 0, len(to_write_cd), max(0, to_write_cd.shape[1] - 1))
            _apply_header_style(ws5, to_write_cd, header_fmt)
            _format_and_autosize(ws5, to_write_cd, num_fmt, text_fmt)
            _highlight_columns(ws5, to_write_cd, ["Mapping", "Remarks", "Reason"], comment_highlight_fmt)
        except Exception: app.logger.exception("Could not write CDNR sheet to output workbook")

    output.seek(0)
    return output.read()

# ------- NEW: Background worker task (called by RQ) -------
from rq import get_current_job
import tempfile, os, time
from concurrent.futures import ThreadPoolExecutor

import pickle
import tempfile
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from rq import get_current_job

# Make sure these helpers are imported or available in main.py:
# download_from_drive, upload_to_drive, _run_reconciliation_pipeline, _generate_final_outputs

def process_reconcile(drive_id_2b: str, drive_id_pr: str, selections: dict, user_id: str = "anon", gstr2b_format: str = "portal", target_return_period: str = "") -> dict:
    def _mark(pct, msg):
        j = get_current_job()
        if j:
            j.meta["progress"] = {"pct": int(pct), "msg": msg}
            j.save_meta()
        print(f"[{time.strftime('%H:%M:%S')}] {pct}% - {msg}", flush=True)

    t0 = time.time()
    _mark(3, "Starting reconciliation...")

    with tempfile.TemporaryDirectory() as td:
        in2b = os.path.join(td, "gstr2b.xlsx")
        inpr = os.path.join(td, "purchase_register.xlsx")

        _mark(10, "Downloading inputs...")
        def _dl(fid, path):
            download_from_drive(fid, path)
            return path

        with ThreadPoolExecutor(max_workers=2) as ex:
            fut2b = ex.submit(_dl, drive_id_2b, in2b)
            futpr = ex.submit(_dl, drive_id_pr, inpr)
            fut2b.result()
            futpr.result()

        _mark(25, "Processing data...")

        # --- Capture Row Counts ---
        row_counts = {"pr": 0, "b2b": 0, "cdnr": 0}
        try:
            df_pr_check = pd.read_excel(inpr, engine="openpyxl")
            row_counts["pr"] = len(df_pr_check)
            with pd.ExcelFile(in2b) as xls:
                offset = 6 if gstr2b_format == "portal" else 1
                if "B2B" in xls.sheet_names:
                    raw = pd.read_excel(xls, "B2B", header=None)
                    row_counts["b2b"] = max(0, len(raw) - offset)
                if "B2B-CDNR" in xls.sheet_names:
                    raw = pd.read_excel(xls, "B2B-CDNR", header=None)
                    row_counts["cdnr"] = max(0, len(raw) - offset)
        except Exception:
            pass
        # --------------------------

        _mark(40, "Running matching algorithms...")

        # 1. Run Pipeline
        state_data = _run_reconciliation_pipeline(
            tmp2b_path=in2b, tmppr_path=inpr,
            gstr2b_format=gstr2b_format,
            target_return_period=target_return_period,
            inv_2b_b2b_sel=selections.get("inv_2b_b2b",""), gst_2b_b2b_sel=selections.get("gst_2b_b2b",""),
            date_2b_b2b_sel=selections.get("date_2b_b2b",""), cgst_2b_b2b_sel=selections.get("cgst_2b_b2b",""),
            sgst_2b_b2b_sel=selections.get("sgst_2b_b2b",""), igst_2b_b2b_sel=selections.get("igst_2b_b2b",""),
            note_2b_cdnr_sel=selections.get("note_2b_cdnr",""), gst_2b_cdnr_sel=selections.get("gst_2b_cdnr",""),
            notedate_2b_cdnr_sel=selections.get("notedate_2b_cdnr",""), cgst_2b_cdnr_sel=selections.get("cgst_2b_cdnr",""),
            sgst_2b_cdnr_sel=selections.get("sgst_2b_cdnr",""), igst_2b_cdnr_sel=selections.get("igst_2b_cdnr",""),
            inv_pr_sel=selections.get("inv_pr",""), gst_pr_sel=selections.get("gst_pr",""),
            date_pr_sel=selections.get("date_pr",""), cgst_pr_sel=selections.get("cgst_pr",""),
            sgst_pr_sel=selections.get("sgst_pr",""), igst_pr_sel=selections.get("igst_pr","")
        )

        # --- Save Row Counts to State Data (Important for Review persistence) ---
        state_data["row_counts"] = row_counts

        # 2. Check for "Almost Matched"
        combined_df = state_data["combined_df"]
        mask_almost = (combined_df["Mapping"] == "Almost Matched")

        if mask_almost.any():
            _mark(50, "Review required...")

            # Prepare data for Review
            review_df = combined_df[mask_almost].copy()
            review_df = review_df.fillna("")

            # --- KEY FIX: Rename Internal Columns for Display ---
            rename_map = {}
            # Handle variations in column naming
            if "_INV_DISPLAY" in review_df.columns:
                rename_map["_INV_DISPLAY"] = "Invoice No (2B)"
            elif "_INV_DISPLAY_2B" in review_df.columns:
                 rename_map["_INV_DISPLAY_2B"] = "Invoice No (2B)"

            if "_DATE_DISPLAY" in review_df.columns:
                rename_map["_DATE_DISPLAY"] = "Invoice Date (2B)"
            elif "_DATE_DISPLAY_2B" in review_df.columns:
                 rename_map["_DATE_DISPLAY_2B"] = "Invoice Date (2B)"

            # Apply renaming
            if rename_map:
                review_df.rename(columns=rename_map, inplace=True)
            # ----------------------------------------------------

            review_df['id'] = review_df.index
            review_rows = review_df.to_dict(orient='records')
            columns = list(review_df.columns)

            # Freeze State
            pickle_filename = f"PendingState_{user_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}.pkl"
            pickle_path = os.path.join(td, pickle_filename)
            with open(pickle_path, "wb") as f:
                pickle.dump(state_data, f)

            pickle_drive_id = upload_to_drive(pickle_path, pickle_filename)

            return {
                "status": "review_needed",
                "review_data": review_rows,
                "columns": columns,
                "pickle_drive_id": pickle_drive_id
            }

        # 3. Finalize immediately if no review needed
        _mark(90, "Finalizing report...")
        blob = _generate_final_outputs(state_data)

        filename = f"Reconciliation_Report_{datetime.now().strftime('%Y%m%d')}.xlsx"
        out_path = os.path.join(td, filename)
        with open(out_path, "wb") as f:
            f.write(blob)

        result_id = upload_to_drive(out_path, filename)

        # --- Generate Dashboard Stats ---
        # Ensure _build_dashboard_stats_dict is defined in main.py
        dashboard_stats = _build_dashboard_stats_dict(state_data)

        return {
            "status": "finished",
            "result_drive_id": result_id,
            "filename": filename,
            "dashboard_stats": dashboard_stats
        }

@app.route("/reconcile_confirm", methods=["POST"])
def reconcile_confirm():
    tmp2b = session.get("tmp2b"); tmppr = session.get("tmppr")
    if not tmp2b or not tmppr or (not os.path.exists(tmp2b)) or (not os.path.exists(tmppr)):
        flash("Upload session expired. Please re-upload the files.")
        return redirect(url_for("index"))

    # ----- AUTO-CLEANUP: remove any previous jobs for this user -----
    try:
        from rq.job import Job
        from rq.registry import StartedJobRegistry, ScheduledJobRegistry, FailedJobRegistry

        # FIX: Get User Identifier correctly for cleanup
        user_identifier = session.get("email") or "anon"

        # 1) If we stored a last_job_id in session, try remove it first
        old_job_id = session.get("last_job_id")
        if old_job_id:
            try:
                old_job = Job.fetch(old_job_id, connection=rconn)
                status = old_job.get_status()
                if status not in ("finished", "failed", "stopped"):
                    try: old_job.cancel(); old_job.delete()
                    except: pass
            except: pass
            session.pop("last_job_id", None)

    except Exception:
        pass # Be defensive if RQ fails
    # -----------------------------------------------------------------

    # Get the format choice and return period
    gstr2b_format = session.get("gstr2b_format", "portal")
    target_return_period = request.form.get("return_period", "").strip()

    sel = {
        # B2B (invoice-based)
        "inv_2b_b2b": (request.form.get("inv_2b_b2b") or "").strip(),
        "gst_2b_b2b": (request.form.get("gst_2b_b2b") or "").strip(),
        "date_2b_b2b": (request.form.get("date_2b_b2b") or "").strip(),
        "cgst_2b_b2b": (request.form.get("cgst_2b_b2b") or "").strip(),
        "sgst_2b_b2b": (request.form.get("sgst_2b_b2b") or "").strip(),
        "igst_2b_b2b": (request.form.get("igst_2b_b2b") or "").strip(),
        # CDNR (note-based)
        "note_2b_cdnr": (request.form.get("note_2b_cdnr") or "").strip(),
        "gst_2b_cdnr": (request.form.get("gst_2b_cdnr") or "").strip(),
        "notedate_2b_cdnr": (request.form.get("notedate_2b_cdnr") or "").strip(),
        "cgst_2b_cdnr": (request.form.get("cgst_2b_cdnr") or "").strip(),
        "sgst_2b_cdnr": (request.form.get("sgst_2b_cdnr") or "").strip(),
        "igst_2b_cdnr": (request.form.get("igst_2b_cdnr") or "").strip(),
        # PR
        "inv_pr": (request.form.get("inv_pr") or "").strip(),
        "gst_pr": (request.form.get("gst_pr") or "").strip(),
        "date_pr": (request.form.get("date_pr") or "").strip(),
        "cgst_pr": (request.form.get("cgst_pr") or "").strip(),
        "sgst_pr": (request.form.get("sgst_pr") or "").strip(),
        "igst_pr": (request.form.get("igst_pr") or "").strip(),
    }

    try:
        drive_id_2b = upload_to_drive(tmp2b, "gstr2b.xlsx")
        drive_id_pr = upload_to_drive(tmppr, "purchase_register.xlsx")
    finally:
        try: os.remove(tmp2b); os.remove(tmppr)
        except: pass
        session.pop("tmp2b", None); session.pop("tmppr", None)
        session.pop("gstr2b_format", None)

    # --- CRITICAL FIX FOR HISTORY: Get User ID from Session ---
    # The form does not contain user_id, so request.form.get("user_id") was returning "anon"
    # This caused the history to be saved under "anon" instead of the actual ID.
    # --- OLD CODE TO REMOVE: ---
    # user_id = session.get("user_id")
    # if not user_id:
    #    user_id = "anon"

    # --- NEW CODE: Use Email Identifier ---
    user_email = session.get("email")
    if user_email:
        # clean email (john.doe@gmail.com -> john.doe)
        user_identifier = re.sub(r'[^a-zA-Z0-9_\-\.]', '', user_email.split('@')[0])
    else:
        user_identifier = str(session.get("user_id", "anon"))

    print(f"DEBUG: Starting Job for User: {user_identifier}")

    # Enqueue Job (Pass user_identifier instead of user_id)
    job = q.enqueue(
        "main.process_reconcile",
        drive_id_2b, drive_id_pr, sel, str(user_identifier),
        gstr2b_format, target_return_period,
        job_timeout=-1,
        result_ttl=86400,
        failure_ttl=86400
    )

    # store job id in session for future auto-clean
    try:
        session["last_job_id"] = job.id
    except Exception:
        pass

    return render_template("progress.html", job_id=job.id)

# --- UPDATE STATUS ROUTE TO PASS RESULT ---
@app.route("/status/<job_id>", methods=["GET"])
def status(job_id):
    job = Job.fetch(job_id, connection=rconn)
    meta = job.meta.get("progress", {"pct": 0, "msg": "queued"})
    state = job.get_status()
    payload = {"state": state, "progress": meta}

    if state == "finished":
        # Pass the full result (including stats) to frontend
        payload["result"] = job.result
    elif state == "failed":
        payload["error"] = (job.exc_info or "")[-1000:]

    return jsonify(payload)
@app.route("/download/<job_id>", methods=["GET"])
def download(job_id):
    job = Job.fetch(job_id, connection=rconn)
    if job.get_status() != "finished":
        return jsonify({"error": "Job not finished"}), 409

    drive_id = job.result.get("result_drive_id")
    # Get the correct filename from the job result
    result_filename = job.result.get("filename", "reconciliation_report.xlsx")
    if not drive_id:
        return jsonify({"error": "No result id"}), 404

    fd, path = tempfile.mkstemp(suffix=".xlsx")
    os.close(fd)
    try:
        download_from_drive(drive_id, path)
    except Exception:
        try:
            os.remove(path)
        except Exception:
            pass
        raise

    @after_this_request
    def _cleanup(response):
        try:
            os.remove(path)
        except Exception:
            pass
        return response

    return send_file(
        path,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name=result_filename,
        conditional=True
    )

# -------------------- Auth Routes --------------------
from werkzeug.security import check_password_hash, generate_password_hash

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        email = request.form['email']
        password = request.form['password']
        user = User.query.filter_by(email=email).first()

        # Verify user and password
        if user and check_password_hash(user.password_hash, password):
            # Create Session Data
            session['logged_in'] = True
            session['email'] = email

            # CRITICAL: This ID is used by the History/My Files feature
            session['user_id'] = user.id

            flash('Logged in successfully.')
            return redirect(url_for('index'))
        else:
            flash('Invalid email or password.')

    return render_template('login.html')

# @app.route('/register', methods=['GET', 'POST'])
# def register():
#     if request.method == 'POST':
#         email = request.form['email']
#         password = request.form['password']
#         if User.query.filter_by(email=email).first():
#             flash('Email already registered.')
#         else:
#             new_user = User(
#                 email=email,
#                 password_hash=generate_password_hash(password)
#             )
#             db.session.add(new_user)
#             db.session.commit()
#             flash('Registration successful. Please log in.')
#             return redirect(url_for('login'))
#     return render_template('register.html')

# main.py - Temporary register route to create an admin

# main.py - Original/Correct Register Route

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        email = request.form['email']
        password = request.form['password']
        if User.query.filter_by(email=email).first():
            flash('Email already registered.')
        else:
            # The User model now correctly defaults is_admin to False
            new_user = User(
                email=email,
                password_hash=generate_password_hash(password)
            )
            db.session.add(new_user)
            db.session.commit()
            flash('Registration successful. Please log in.', 'success')
            return redirect(url_for('login'))
    return render_template('register.html')

@app.route('/forgot_password', methods=['GET', 'POST'])
def forgot_password():
    if request.method == 'POST':
        email = request.form['email']
        user = User.query.filter_by(email=email).first()

        if user:
            # Generate token valid for 1 hour (3600 seconds)
            token = s.dumps(email, salt='email-confirm')

            # Create link
            link = url_for('reset_password', token=token, _external=True)

            # Send Email
            msg = Message('Password Reset Request', recipients=[email])
            msg.body = f'Click the link to reset your password: {link}\n\nLink expires in 1 hour.'

            try:
                mail.send(msg)
                flash('An email has been sent with instructions to reset your password.', 'success')
            except Exception as e:
                flash(f'Error sending email: {str(e)}', 'danger')
        else:
            # Security: Don't reveal if email exists or not, but for now we say sent
            flash('If an account exists with that email, a reset link has been sent.', 'info')

        return redirect(url_for('login'))

    return render_template('forgot_password.html')

@app.route('/reset_password/<token>', methods=['GET', 'POST'])
def reset_password(token):
    try:
        # Verify token (max age 3600 seconds = 1 hour)
        email = s.loads(token, salt='email-confirm', max_age=3600)
    except Exception:
        flash('The reset link is invalid or has expired.', 'danger')
        return redirect(url_for('login'))

    if request.method == 'POST':
        new_password = request.form['password']
        user = User.query.filter_by(email=email).first()

        if user:
            user.password_hash = generate_password_hash(new_password)
            db.session.commit()
            flash('Your password has been updated! You can now log in.', 'success')
            return redirect(url_for('login'))
        else:
            flash('User account not found.', 'danger')
            return redirect(url_for('login'))

    return render_template('reset_password.html', token=token)


@app.route('/logout')
def logout():
    session.clear()
    flash('Logged out.')
    return redirect(url_for('index'))

@app.route('/subscribe', methods=['GET', 'POST'])
def subscribe():
    if not session.get('logged_in'):
        flash('Please log in to access subscription.')
        return redirect(url_for('login'))
    if request.method == 'POST':
        plan = request.form.get('plan')
        if plan in ['half-yearly', 'annual']:
            user = User.query.filter_by(email=session['email']).first()
            if user:
                user.subscribed = True
                db.session.commit()
            flash(f'Subscription activated: {plan.replace("-", " ").title()} plan.')
            return redirect(url_for('index'))
        else:
            flash('Invalid plan selected.')
    return render_template('subscribe.html')

from typing import Any, Dict, Optional

import hashlib
import os
from flask import request, session, url_for
import html
import time
import secrets
import re


from functools import wraps

# --- Admin Functionality ---
def admin_required(f):
    """Decorator to ensure a user is logged in and is an admin."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('logged_in'):
            flash('Please log in to access this page.', 'warning')
            return redirect(url_for('login'))

        user = User.query.filter_by(email=session.get('email')).first()
        if not user or not user.is_admin:
            flash('You do not have permission to access the admin dashboard.', 'danger')
            return redirect(url_for('index'))

        return f(*args, **kwargs)
    return decorated_function

# REPLACEMENT FUNCTION: Fetches all users for the admin dashboard.
@app.route('/admin')
@admin_required
def admin_dashboard():
    """Displays the admin dashboard with a list of all users."""
    all_users = User.query.order_by(User.id).all()
    # Pass today's date to the template to check for expired accounts
    return render_template('admin.html', users=all_users, today=date.today())

@app.route('/admin/user/<int:user_id>/delete', methods=['POST'])
@admin_required
def admin_delete_user(user_id):
    """Deletes a user. Admins cannot delete themselves."""
    admin_user = User.query.filter_by(email=session.get('email')).first()
    if admin_user.id == user_id:
        flash('Admins cannot delete their own account.', 'danger')
        return redirect(url_for('admin_dashboard'))

    user_to_delete = User.query.get_or_404(user_id)
    db.session.delete(user_to_delete)
    db.session.commit()
    flash(f'User {user_to_delete.email} has been deleted.', 'success')
    return redirect(url_for('admin_dashboard'))

@app.route('/admin/user/<int:user_id>/toggle_subscription', methods=['POST'])
@admin_required
def admin_toggle_subscription(user_id):
    """Toggles a user's subscription status."""
    user_to_edit = User.query.get_or_404(user_id)
    # The checkbox sends 'on' when checked, and nothing when unchecked.
    user_to_edit.subscribed = 'subscribed' in request.form
    db.session.commit()
    status = "activated" if user_to_edit.subscribed else "deactivated"
    flash(f'Subscription for {user_to_edit.email} has been {status}.', 'success')
    return redirect(url_for('admin_dashboard'))

@app.route('/admin/user/<int:user_id>/change_password', methods=['POST'])
@admin_required
def admin_change_password(user_id):
    """Changes a user's password."""
    user_to_edit = User.query.get_or_404(user_id)
    new_password = request.form.get('new_password')

    if not new_password or len(new_password) < 6:
        flash('Password must be at least 6 characters long.', 'danger')
        return redirect(url_for('admin_dashboard'))

    user_to_edit.password_hash = generate_password_hash(new_password)
    db.session.commit()
    flash(f'Password for {user_to_edit.email} has been updated.', 'success')
    return redirect(url_for('admin_dashboard'))

# Add this at the top with your other imports
from datetime import date, timedelta

@app.route('/start_trial', methods=['POST'])
def start_trial():
    if not session.get('logged_in'):
        flash('Please log in to start a free trial.', 'warning')
        return redirect(url_for('login'))

    user = User.query.filter_by(email=session['email']).first()
    if not user:
        # defensive: shouldn't happen if step (2) is present, but keep safe
        session.clear()
        flash('Session invalid. Please log in again.', 'warning')
        return redirect(url_for('login'))

    # Prevent users from getting multiple trials: treat any existing expiry as prior subscription/trial.
    if user.subscribed or getattr(user, 'subscription_expiry_date', None):
        flash('A free trial is only available for new users who have not subscribed before.', 'danger')
        return redirect(url_for('subscribe'))

    user.subscribed = True
    user.subscription_expiry_date = date.today() + timedelta(days=30)
    db.session.commit()

    flash('Your 7-day free trial has started! You now have full access.', 'success')
    return redirect(url_for('index'))

@app.route('/admin/edit_user/<int:user_id>', methods=['GET', 'POST'])
@admin_required
def edit_user(user_id):
    """Allows admin to edit a user's subscription and admin status."""
    user_to_edit = User.query.get_or_404(user_id)

    if request.method == 'POST':
        # --- START: Handle Quick Grant Actions ---
        grant_action = request.form.get('grant_access')
        if grant_action:
            user_to_edit.subscribed = True
            today = date.today()

            # Start new subscription period from today
            start_date = today

            if grant_action == '1_month':
                user_to_edit.subscription_expiry_date = start_date + timedelta(days=30)
                flash(f'Granted 1 month access to {user_to_edit.email}.', 'success')
            elif grant_action == '6_months':
                user_to_edit.subscription_expiry_date = start_date + timedelta(days=182)
                flash(f'Granted 6 months access to {user_to_edit.email}.', 'success')
            elif grant_action == '1_year':
                user_to_edit.subscription_expiry_date = start_date + timedelta(days=365)
                flash(f'Granted 1 year access to {user_to_edit.email}.', 'success')

            db.session.commit()
            return redirect(url_for('admin_dashboard'))
        # --- END: Quick Grant Actions ---

        # Prevent removing your own admin privileges (only enforce if we know who is logged in)
        current_user_id = session.get('user_id')
        is_removing_own_admin = (current_user_id is not None and int(current_user_id) == user_to_edit.id and not request.form.get('is_admin'))
        if is_removing_own_admin:
            flash('You cannot remove your own admin privileges.', 'danger')
            return redirect(url_for('edit_user', user_id=user_id))

        # Manual Edit Logic
        user_to_edit.subscribed = request.form.get('subscribed') == 'on'
        user_to_edit.is_admin = request.form.get('is_admin') == 'on'

        expiry_date_str = request.form.get('expiry_date')
        if expiry_date_str:
            try:
                user_to_edit.subscription_expiry_date = datetime.strptime(expiry_date_str, '%Y-%m-%d').date()
            except Exception:
                flash('Invalid expiry date format. Use YYYY-MM-DD.', 'danger')
                return redirect(url_for('edit_user', user_id=user_id))
        else:
            # If not subscribed, clear the date.
            if not user_to_edit.subscribed:
                user_to_edit.subscription_expiry_date = None

        db.session.commit()
        flash(f'User {user_to_edit.email} updated successfully.', 'success')
        return redirect(url_for('admin_dashboard'))

    return render_template('edit_user.html', user=user_to_edit)

# alias route so older templates that call 'delete_user' still work
@app.route('/admin/delete_user/<int:user_id>', methods=['POST'])
@admin_required
def delete_user(user_id):
    # Delegate to the existing admin_delete_user implementation
    return admin_delete_user(user_id)

@app.route('/history')
def history():
    """Displays the list of past reconciliations from Google Drive."""
    if not session.get('logged_in'):
        flash('Please log in to view history.', 'warning')
        return redirect(url_for('login'))

    # 1. Determine Identifier (Same logic as reconcile_confirm)
    user_email = session.get("email")
    if user_email:
        user_identifier = re.sub(r'[^a-zA-Z0-9_\-\.]', '', user_email.split('@')[0])
    else:
        user_identifier = str(session.get("user_id", "anon"))

    reports = []
    print(f"DEBUG History: Fetching for: {user_identifier}")

    try:
        from googleapiclient.discovery import build
        creds = get_credentials()
        service = build('drive', 'v3', credentials=creds)

        # 2. Search for files matching: Recon_{identifier}_
        query = f"name contains 'Recon_{user_identifier}_' and trashed=false"
        print(f"DEBUG History: Query = {query}")

        # 3. CRITICAL FIX: Add supportsAllDrives to find files in all locations
        results = service.files().list(
            q=query,
            fields="files(id, name, createdTime)",
            orderBy="createdTime desc",
            pageSize=50,
            supportsAllDrives=True,         # <--- Fixes "Found 0 files"
            includeItemsFromAllDrives=True  # <--- Fixes "Found 0 files"
        ).execute()

        files = results.get('files', [])
        print(f"DEBUG History: Found {len(files)} files")

        for f in files:
            created_time = f.get('createdTime', '')
            if created_time:
                from dateutil import parser
                dt = parser.parse(created_time)
                formatted_date = dt.strftime('%d-%b-%Y %H:%M')
            else:
                formatted_date = 'N/A'

            reports.append({
                'id': f['id'],
                'drive_file_id': f['id'],
                'report_name': f['name'].replace('.xlsx', ''),
                'generated_at': formatted_date,
            })

    except Exception as e:
        print(f"DEBUG History ERROR: {e}")
        import traceback
        traceback.print_exc()

    return render_template('history.html', reports=reports)

@app.route('/download_history/<report_id>')
def download_history(report_id):
    """Download a report file from Google Drive."""
    if not session.get('logged_in'):
        flash('Please log in to download files.', 'warning')
        return redirect(url_for('login'))

    # Create a temporary file path
    import tempfile
    fd, path = tempfile.mkstemp(suffix=".xlsx")
    os.close(fd)

    try:
        from googleapiclient.discovery import build
        creds = get_credentials()
        service = build('drive', 'v3', credentials=creds)

        # 1. Get file metadata (for the name)
        try:
            file_metadata = service.files().get(
                fileId=report_id,
                fields='name',
                supportsAllDrives=True  # Important: Add this flag
            ).execute()
            file_name = file_metadata.get('name', 'reconciliation_report.xlsx')
        except Exception:
            file_name = 'reconciliation_report.xlsx'

        # 2. Download to the temp path using your existing helper
        download_from_drive(report_id, path)

        # 3. Cleanup after sending
        @after_this_request
        def _cleanup(response):
            try:
                os.remove(path)
            except Exception:
                pass
            return response

        return send_file(
            path,
            as_attachment=True,
            download_name=file_name,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

    except Exception as e:
        print(f"History Download Error: {e}")
        # Clean up if download failed
        if os.path.exists(path):
            try: os.remove(path)
            except: pass

        flash('Failed to download file from history. Please try again.', 'error')
        return redirect(url_for('history'))


@app.route("/download_nm/<job_id>", methods=["GET"])
def download_nm(job_id):
    job = Job.fetch(job_id, connection=rconn)
    if job.get_status() != "finished":
        return jsonify({"error": "Job not finished"}), 409

    # Get the ID for the Not Matched file
    drive_id = job.result.get("nm_drive_id")
    filename = job.result.get("nm_filename", "not_matched.xlsx")

    if not drive_id:
        return "Report not generated or empty", 404

    # Create temp path
    fd, path = tempfile.mkstemp(suffix=".xlsx")
    os.close(fd)

    try:
        download_from_drive(drive_id, path)

        @after_this_request
        def _cleanup(response):
            try: os.remove(path)
            except: pass
            return response

        return send_file(
            path,
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    except Exception:
        return "Error downloading file", 500


# --- NEW ROUTE FOR HISTORY NAVIGATION ---
@app.route("/progress/<job_id>")
def render_progress(job_id):
    """
    Renders the progress page for a specific job ID.
    Used by the History page to view past results.
    """
    return render_template("progress.html", job_id=job_id)

from flask import make_response
@app.route('/consolidate', methods=['POST'])
def consolidate_files():
    # 1. Check Login
    if not session.get('logged_in'):
        flash('Please log in to use this feature.', 'warning')
        return redirect(url_for('login'))

    files = request.files.getlist("files")
    if not files or not files[0].filename:
        flash("No files selected.", "warning")
        return redirect(url_for("index"))

    # 2. DETERMINE RULES
    doc_type = request.form.get('doc_type', 'gstr2b')
    format_option = request.form.get('format_option', 'custom')

    # Rule: Portal 2B = Drop 6 rows. Others = Drop 1 row.
    if doc_type == 'gstr2b' and format_option == 'portal':
        header_height = 6
    else:
        header_height = 1

    try:
        sheets_map = defaultdict(list)
        initialized_sheets = set()

        for file in files:
            if not file.filename.lower().endswith(('.xls', '.xlsx')):
                continue

            try:
                # Read ALL rows (header=None keeps original row indices)
                xls_dict = pd.read_excel(file, sheet_name=None, engine="openpyxl", header=None, dtype=object)
            except Exception as e:
                flash(f"Error reading {file.filename}: {str(e)}", "danger")
                return redirect(url_for("index"))

            for sheet_name, df in xls_dict.items():
                if df.empty:
                    continue

                clean_sheet_name = sheet_name.strip()

                if clean_sheet_name not in initialized_sheets:
                    # File 1: Keep completely intact
                    sheets_map[clean_sheet_name].append(df)
                    initialized_sheets.add(clean_sheet_name)
                else:
                    # File 2+: Remove header rows
                    if len(df) > header_height:
                        sheets_map[clean_sheet_name].append(df.iloc[header_height:])

        # 3. WRITE & FORMAT
        output = io.BytesIO()
        data_written = False

        with pd.ExcelWriter(output, engine="xlsxwriter", datetime_format='dd-mm-yyyy') as writer:
            workbook = writer.book
            num_format = workbook.add_format({'num_format': '#,##0.00', 'align': 'right'})

            for sheet_name, dfs in sheets_map.items():
                if dfs:
                    merged_df = pd.concat(dfs, ignore_index=True)

                    # Convert Numbers
                    for col_idx in range(len(merged_df.columns)):
                        col_name = merged_df.columns[col_idx]
                        merged_df[col_name] = pd.to_numeric(merged_df[col_name], errors='ignore')

                    merged_df.to_excel(writer, sheet_name=sheet_name, header=False, index=False)

                    # Apply Formatting
                    worksheet = writer.sheets[sheet_name]
                    for idx, col in enumerate(merged_df.columns):
                        is_number = pd.api.types.is_numeric_dtype(merged_df[col])
                        is_date = pd.api.types.is_datetime64_any_dtype(merged_df[col])

                        if is_number and not is_date:
                            worksheet.set_column(idx, idx, None, num_format)
                        elif is_date:
                            worksheet.set_column(idx, idx, 15)

                    data_written = True

            if not data_written:
                flash("No data found to consolidate.", "warning")
                return redirect(url_for("index"))

        output.seek(0)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M')
        prefix = "GSTR2B" if doc_type == 'gstr2b' else "PurchaseRegister"
        filename = f"Consolidated_{prefix}_{timestamp}.xlsx"

        # 4. CREATE RESPONSE WITH COOKIE
        # This cookie tells the browser "We are done!"
        response = make_response(send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ))
        response.set_cookie('consolidation_complete', 'true', path='/')
        return response

    except Exception as e:
        app.logger.error(f"Consolidation Error: {e}")
        flash(f"System Error: {str(e)}", "danger")
        return redirect(url_for("index"))

# ==========================================
# NEW ROUTE: Delete History (Database Only)
# ==========================================
@app.route('/delete_history/<report_id>', methods=['POST'])
def delete_history(report_id):
    """
    Removes the record from the database table ONLY.
    Does NOT attempt to connect to Google Drive.
    """
    if not session.get('logged_in'):
        flash('Please log in to perform this action.', 'warning')
        return redirect(url_for('login'))

    try:
        # 1. Find the record in the database using the Drive File ID
        record = ReconciliationReport.query.filter_by(drive_file_id=report_id).first()

        if record:
            # 2. Delete the record from the database
            db.session.delete(record)
            db.session.commit()
            flash('Report removed from history list.', 'success')
        else:
            # Even if not found, we treat it as success so it "disappears" from the user's view
            flash('Record not found (may have already been removed).', 'info')

    except Exception as e:
        app.logger.error(f"Database Removal Error: {e}")
        db.session.rollback() # Important: Reset DB connection if it got stuck
        flash('Could not remove row. Please try again.', 'danger')

    return redirect(url_for('history'))

@app.route("/review_matches", methods=["GET"])
def review_matches():
    job_id = request.args.get("job_id")
    if not job_id: return redirect(url_for("index"))

    try:
        job = Job.fetch(job_id, connection=rconn)
        if job.get_status() == "finished":
            result = job.result
            if result.get("status") == "review_needed":
                rows = result.get("review_data", [])
                columns = result.get("columns", [])
                pickle_id = result.get("pickle_drive_id")
                return render_template("review.html", rows=rows, columns=columns, pickle_id=pickle_id)
    except Exception:
        pass

    flash("Review not required or job expired.")
    return redirect(url_for("index"))

@app.route("/submit_review", methods=["POST"])
def submit_review():
    pickle_id = request.form.get("pickle_id")
    accepted = request.form.getlist("accepted_rows")
    accepted_indices = [int(x) for x in accepted]

    # Get User ID for filename
    user_email = session.get("email")
    if user_email:
        user_identifier = re.sub(r'[^a-zA-Z0-9_\-\.]', '', user_email.split('@')[0])
    else:
        user_identifier = str(session.get("user_id", "anon"))

    # Start the Finalization Job
    job = q.enqueue(
        "main.finalize_reconciliation_job",
        pickle_id, accepted_indices, str(user_identifier),
        job_timeout=-1, result_ttl=86400
    )

    # Show progress page again (it will jump to 100% quickly)
    return render_template("progress.html", job_id=job.id)

def _build_dashboard_stats_dict(state_data):
    """
    Generates the dictionary required by the frontend dashboard.
    """
    combined_df = state_data["combined_df"]
    pair_cols = state_data["pair_cols"]
    row_counts = state_data.get("row_counts", {"pr": 0, "b2b": 0, "cdnr": 0})

    # Helper to safe-sum columns
    def _sum_col(mask, col_name):
        if not col_name or col_name not in combined_df.columns: return 0.0
        s = combined_df.loc[mask, col_name].astype(str).str.replace(',', '').str.replace('nan', '')
        return float(pd.to_numeric(s, errors="coerce").fillna(0).sum())

    stats_table = []
    statuses = ["Matched", "Almost Matched", "Not Matched", None] # None = Total

    for status in statuses:
        if status:
            mask = combined_df["Mapping"] == status
            label = status
        else:
            mask = slice(None) # Select all
            label = "Total"

        # Calculate PR Side
        cg_pr = _sum_col(mask, pair_cols.get("cgst_pr_col"))
        sg_pr = _sum_col(mask, pair_cols.get("sgst_pr_col"))
        ig_pr = _sum_col(mask, pair_cols.get("igst_pr_col"))
        tot_pr = cg_pr + sg_pr + ig_pr

        # Calculate 2B Side
        cg_2b = _sum_col(mask, pair_cols.get("cgst_2b_col"))
        sg_2b = _sum_col(mask, pair_cols.get("sgst_2b_col"))
        ig_2b = _sum_col(mask, pair_cols.get("igst_2b_col"))
        tot_2b = cg_2b + sg_2b + ig_2b

        stats_table.append({
            "status": label,
            "pr": {"cgst": cg_pr, "sgst": sg_pr, "igst": ig_pr, "total": tot_pr},
            "b2b": {"cgst": cg_2b, "sgst": sg_2b, "igst": ig_2b, "total": tot_2b}
        })

    return {
        "counts": row_counts,
        "table": stats_table
    }

def finalize_reconciliation_job(pickle_drive_id, accepted_indices, user_id):
    """
    Resumes the process, updates mappings, and generates final outputs + stats.
    """
    with tempfile.TemporaryDirectory() as td:
        # Download the frozen state
        pickle_path = os.path.join(td, "state.pkl")
        download_from_drive(pickle_drive_id, pickle_path)

        with open(pickle_path, "rb") as f:
            state_data = pickle.load(f)

        df = state_data["combined_df"]

        # Identify "Almost Matched" rows
        almost_mask = df["Mapping"] == "Almost Matched"
        all_almost_indices = df[almost_mask].index.tolist()
        accepted_set = set(accepted_indices)

        # Apply User Decisions
        for idx in all_almost_indices:
            if idx in accepted_set:
                df.at[idx, "Mapping"] = "Matched"
                df.at[idx, "Remarks"] = "Accepted by User"
                df.at[idx, "Reason"] = "User Verified"
            else:
                df.at[idx, "Mapping"] = "Not Matched"
                df.at[idx, "Remarks"] = "Rejected by User"
                orig = str(df.at[idx, "Reason"])
                df.at[idx, "Reason"] = f"{orig} (Rejected)"

        # Update state
        state_data["combined_df"] = df

        # Generate Final Output
        blob = _generate_final_outputs(state_data)

        # Save Final Excel
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"Recon_{user_id}_{timestamp}.xlsx"
        out_path = os.path.join(td, filename)
        with open(out_path, "wb") as f:
            f.write(blob)

        result_id = upload_to_drive(out_path, filename)

        # --- NEW: Calculate Real Dashboard Stats ---
        dashboard_stats = _build_dashboard_stats_dict(state_data)
        # -------------------------------------------

        return {
            "status": "finished",
            "result_drive_id": result_id,
            "filename": filename,
            "dashboard_stats": dashboard_stats
        }

# --- Register Razorpay Blueprint ---
from payments import payment_bp
app. register_blueprint(payment_bp)

if __name__ == "__main__":
    app.run(debug=True)
