# main.py
import io
import re
import os
from dotenv import load_dotenv
load_dotenv(override=False)  # ensures both Flask and RQ worker see .env

import math
import tempfile
from datetime import datetime
from typing import Optional, Dict, List, Tuple
from collections import defaultdict
from flask import Flask, render_template, request, send_file, flash, redirect, url_for, session, jsonify
import pandas as pd
import secrets
# ------- NEW: Queue & Google Drive imports -------
from redis import Redis
from rq import Queue, get_current_job
from rq.job import Job
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

# -------------------- Flask & App Config --------------------
app = Flask(__name__)
# Use SECRET_KEY from environment (Render), fallback for local dev
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-key-for-local-use-only")
# File upload limit
app.config['MAX_CONTENT_LENGTH'] = 25 * 1024 * 1024  # 25 MB

# main.py

# ... (keep other imports) ...

# Database: Use persistent disk on Render, fallback to local file, or use PostgreSQL if specified
database_url = os.environ.get("DATABASE_URL")

if database_url:
    # If DATABASE_URL is set (e.g., for PostgreSQL), use it.
    app.config["SQLALCHEMY_DATABASE_URI"] = database_url.replace("postgres://", "postgresql://")
else:
    # If no DATABASE_URL, use SQLite with logic for Render's persistent disk.
    # Check if we are in the Render environment by looking for the disk mount path.
    render_disk_path = "/mnt/data"
    if os.path.exists(render_disk_path):
        # We are on Render: use the persistent disk for the database.
        db_path = os.path.join(render_disk_path, "users.db")
    else:
        # We are running locally: use a regular file in the project folder.
        db_path = "users.db"
    app.config["SQLALCHEMY_DATABASE_URI"] = f"sqlite:///{db_path}"

app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

# ------- Initialize SQLAlchemy (for auth) -------
from flask_sqlalchemy import SQLAlchemy
db = SQLAlchemy(app)

# User Model (for login/subscribe)
# main.py

class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(120), unique=True, nullable=False)
    password_hash = db.Column(db.String(200), nullable=False)
    subscribed = db.Column(db.Boolean, default=False)
    is_admin = db.Column(db.Boolean, default=False, nullable=False)  # <-- ADD THIS LINE
    def __repr__(self):
        return f'<User {self.email}>'

import threading
_tables_created = False
_tables_lock = threading.Lock()
@app.before_request
def create_tables_once():
    global _tables_created
    if not _tables_created:
        with _tables_lock:
            if not _tables_created:
                db.create_all()
                _tables_created = True

# ------- NEW: Redis Queue (for background jobs) -------
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
rconn = Redis.from_url(REDIS_URL)
q = Queue("reconcile", connection=rconn)

# ------- NEW: Google Drive service (service account) -------
SCOPES = ['https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_FILE = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', 'cred.json')
DRIVE_FOLDER_ID = os.environ.get('DRIVE_FOLDER_ID', None)

def _drive_service():
    path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS') or SERVICE_ACCOUNT_FILE
    if not path or not os.path.exists(path):
        raise RuntimeError(f"GOOGLE_APPLICATION_CREDENTIALS missing or not found: {path}")
    creds = service_account.Credentials.from_service_account_file(path, scopes=SCOPES)
    return build('drive', 'v3', credentials=creds, cache_discovery=False)

def upload_to_drive(local_path: str, filename: str) -> str:
    service = _drive_service()
    metadata = {'name': filename}
    if DRIVE_FOLDER_ID:
        metadata['parents'] = [DRIVE_FOLDER_ID]
    media = MediaFileUpload(local_path, resumable=True)
    file = service.files().create(
        body=metadata,
        media_body=media,
        fields='id',
        supportsAllDrives=True
    ).execute()
    return file.get('id')

def download_from_drive(file_id: str, local_path: str):
    service = _drive_service()
    request = service.files().get_media(
        fileId=file_id,
        supportsAllDrives=True
    )
    with io.FileIO(local_path, 'wb') as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()

# -------------------- Column candidates --------------------
# GSTR-2B (flattened) candidates
INVOICE_CANDIDATES_2B = [
    "invoice details invoice number", "invoice number", "invoice no", "inv no", "inv number",
    "invoice", "invoice details inv no", "invoice details document number",
    "note no", "debit note no", "credit note no", "note number"
]
# Generic GSTIN (B2B)
GSTIN_CANDIDATES_2B = [
    "gstin of supplier", "supplier information gstin of supplier",
    "gstin", "gst no", "gst number", "gstn"
]
# Stronger GSTIN for CDNR (explicit 2B wording first)
GSTIN_CANDIDATES_CDNR = [
    "gstin of supplier 2b", "gstin of supplier", "supplier information gstin of supplier",
    "gstin", "gst no", "gst number", "gstn"
]
DATE_CANDIDATES_2B = [
    "invoice details invoice date", "invoice date", "doc date", "document date", "date"
]
# NEW: explicit note number/date/type candidates for CDNR
NOTE_NO_CANDIDATES_2B = [
    "note no", "note number", "credit note no", "debit note no", "cn no", "dn no", "document number", "document no"
]
NOTE_DATE_CANDIDATES_2B = [
    "note date", "document date", "doc date", "credit note date", "debit note date"
]
NOTE_TYPE_CANDIDATES_2B = [
    "note type", "credit/debit note", "cr/dr", "type of note", "cd note type", "cr/dr note"
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
    "party invoice no", "party inv no", "bill no", "bill number",
    # generic fallbacks
    "invoice number", "invoice no", "inv no", "inv number", "invoice",
    # allow common accounting label
    "doc no"
]
GSTIN_CANDIDATES_PR = [
    # strong preference for supplier/vendor
    "supplier gstin", "vendor gstin", "party gstin",
    # fallbacks
    "gstin", "gst no", "gst number", "gstn"
]
DATE_CANDIDATES_PR = [
    "invoice date", "vendor invoice date", "supplier invoice date", "bill date", "doc date", "document date", "date"
]
CGST_CANDIDATES_PR = ["cgst", "cgst amount", "central tax", "central tax amount"]
SGST_CANDIDATES_PR = ["sgst", "sgst amount", "state tax", "state tax amount", "utgst", "utgst amount"]
IGST_CANDIDATES_PR = ["igst", "igst amount", "integrated tax", "integrated tax amount"]
TAXABLE_CANDIDATES_PR = ["taxable value", "taxable amount", "assessable value", "taxable"]
TOTAL_TAX_CANDIDATES_PR = ["total tax", "total tax amount", "tax amount"]
INVOICE_VALUE_CANDIDATES_PR = ["invoice value", "total invoice value", "value of invoice", "invoice total"]
CESS_CANDIDATES_PR = ["cess", "cess amount"]

# Avoid confusing PR invoice with doc/voucher and PR GSTIN with recipient/company
AVOID_DOC_LIKE_FOR_PR = ["document no", "document number", "doc number", "voucher no", "voucher number"]
AVOID_RECIPIENT_GSTIN_FOR_PR = ["company gstin", "our gstin", "recipient gstin", "customer gstin", "buyer gstin"]

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
    return [str(c).replace("\xa0", " ").strip()]

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
    numeric_cols: List[str]
) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    work = df.copy()
    work["_GST_KEY"] = work[gstin_col].map(clean_gstin)
    work[inv_col] = work[inv_col].map(as_text)
    work["_INV_KEY"] = work[inv_col].map(inv_basic)

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
    agg_dict["_DATE_TMP"] = "min"
    agg_dict[gstin_col] = "first"
    agg_dict[inv_col] = "first"

    protected = set(["_GST_KEY", "_INV_KEY", "_DATE_TMP"] + [gstin_col, inv_col] + numeric_cols)
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

# -------------------- Pairwise combined output --------------------
# main.py

def build_pairwise_recon(
    df_pr: pd.DataFrame, df_2b: pd.DataFrame,
    inv_pr: str, gst_pr: str, date_pr: str, cgst_pr: str, sgst_pr: str, igst_pr: str,
    inv_2b: str, gst_2b: str, date_2b: str, cgst_2b: str, sgst_2b: str, igst_2b: str,
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

    mapping, remarks, reason = [], [], []
    for _, r in merged.iterrows():
        pr_inv_val = as_text(r.get(inv_pr_col, "")) if inv_pr_col in merged.columns else ""
        pr_gst_val = clean_gstin(r.get(gst_pr_col, "")) if gst_pr_col in merged.columns else ""
        b2_inv_val = as_text(r.get(inv_2b_col, "")) if inv_2b_col in merged.columns else ""
        b2_gst_val = clean_gstin(r.get(gst_2b_col, "")) if gst_2b_col in merged.columns else ""

        pr_present = bool(pr_inv_val) or bool(pr_gst_val)
        b2_present = bool(b2_inv_val) or bool(b2_gst_val)
        if pr_present and not b2_present:
            mapping.append("Not Matched"); remarks.append("missing in 2B"); reason.append(""); continue
        if b2_present and not pr_present:
            mapping.append("Not Matched"); remarks.append("missing in PR"); reason.append(""); continue

        mismatches = []
        if date_pr_col and date_2b_col:
            d_pr = parse_date_cell(r.get(date_pr_col, ""))
            d_2b = parse_date_cell(r.get(date_2b_col, ""))
            if (d_pr or d_2b) and (d_pr != d_2b):
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

    # --- FIX: Targeted correction for CDNR GSTINs in the final output ---
    # Find the column that indicates the source sheet (B2B or B2B-CDNR) for 2B data.
    source_sheet_col = "_SOURCE_SHEET_2B"
    if source_sheet_col in out.columns and gst_2b_col in out.columns:
        # Identify rows that came from the B2B-CDNR sheet.
        cdnr_mask = (out[source_sheet_col] == "B2B-CDNR")
        # For those specific rows, overwrite the potentially incorrect GSTIN with the correct one from `_GST_KEY`.
        out.loc[cdnr_mask, gst_2b_col] = out.loc[cdnr_mask, "_GST_KEY"]
    # --- End of FIX ---

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

    keep_extra = [c for c in [gstr1_status_2b] if c and c in out.columns]

    front = ["_GST_KEY", "_INV_KEY", "Mapping", "Remarks", "Reason", "Vendor Name"]
    final_cols = front + pair_cols + keep_extra
    out = out[final_cols]

    two_b_month_series = ""
    if gstr1_status_2b and gstr1_status_2b in out.columns:
        two_b_month_series = out[gstr1_status_2b]
    out["2B month"] = two_b_month_series

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


# -------------------- Routes --------------------
@app.route("/", methods=["GET"])
def index():
    return render_template("index.html")

@app.route("/verify", methods=["POST"])
def verify_columns():
    file_2b = request.files.get("gstr2b")
    file_pr = request.files.get("purchase_register")
    if not file_2b or not file_pr:
        flash("Please upload both files: GSTR-2B and Purchase Register.")
        return redirect(url_for("index"))

    tmp2b = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
    tmppr = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
    file_2b.stream.seek(0); tmp2b.write(file_2b.read()); tmp2b.close()
    file_pr.stream.seek(0); tmppr.write(file_pr.read()); tmppr.close()
    session["tmp2b"] = tmp2b.name
    session["tmppr"] = tmppr.name

    wanted_sheets = ["B2B", "B2B-CDNR"]
    with pd.ExcelFile(tmp2b.name) as xls:
        sheet_names = xls.sheet_names
    present_sheets = [sn for sn in wanted_sheets if sn in sheet_names]

    if "B2B" not in present_sheets:
        flash("Could not find a 'B2B' sheet in the GSTR-2B file.")
        return redirect(url_for("index"))

    df_b2b = None
    df_cdnr = None
    if "B2B" in present_sheets:
        df_b2b = pd.read_excel(tmp2b.name, sheet_name="B2B", header=[4, 5], engine="openpyxl", dtype=str)
        print("LOCAL B2B COLS:", df_b2b.columns.tolist()[:25])
        df_b2b = df_b2b.dropna(how="all")
        df_b2b.columns = flatten_columns(df_b2b.columns)
        df_b2b, _ = normalize_columns(df_b2b)

    if "B2B-CDNR" in present_sheets:
        df_cdnr = pd.read_excel(tmp2b.name, sheet_name="B2B-CDNR", header=[4, 5], engine="openpyxl", dtype=str)
        print("LOCAL CDNR COLS:", df_cdnr.columns.tolist()[:25])
        df_cdnr = df_cdnr.dropna(how="all")
        df_cdnr.columns = flatten_columns(df_cdnr.columns)
        df_cdnr, _ = normalize_columns(df_cdnr)

    df_pr_raw = pd.read_excel(tmppr.name, engine="openpyxl", dtype=str)
    print("LOCAL PR COLS:", df_pr_raw.columns.tolist()[:25])
    df_pr_raw, _ = normalize_columns(df_pr_raw)

    # ---- Detection for B2B (invoice-based) ----
    inv_2b_b2b = _pick_column(df_b2b, INVOICE_CANDIDATES_2B) if df_b2b is not None else None
    gst_2b_b2b = _pick_column(df_b2b, GSTIN_CANDIDATES_2B) if df_b2b is not None else None
    date_2b_b2b = _pick_column(df_b2b, DATE_CANDIDATES_2B) if df_b2b is not None else None
    cgst_2b_b2b = _pick_column(df_b2b, CGST_CANDIDATES_2B) if df_b2b is not None else None
    sgst_2b_b2b = _pick_column(df_b2b, SGST_CANDIDATES_2B) if df_b2b is not None else None
    igst_2b_b2b = _pick_column(df_b2b, IGST_CANDIDATES_2B) if df_b2b is not None else None

    # ---- Detection for CDNR (note-based) ----
    note_2b_cdnr = _pick_column(df_cdnr, NOTE_NO_CANDIDATES_2B) if df_cdnr is not None else None
    notedate_2b_cdnr = _pick_column(df_cdnr, NOTE_DATE_CANDIDATES_2B) if df_cdnr is not None else None
    note_type_2b_cdnr = _pick_column(df_cdnr, NOTE_TYPE_CANDIDATES_2B) if df_cdnr is not None else None
    gst_2b_cdnr = _pick_column(df_cdnr, GSTIN_CANDIDATES_CDNR) if df_cdnr is not None else None
    cgst_2b_cdnr = _pick_column(df_cdnr, CGST_CANDIDATES_2B) if df_cdnr is not None else None
    sgst_2b_cdnr = _pick_column(df_cdnr, SGST_CANDIDATES_2B) if df_cdnr is not None else None
    igst_2b_cdnr = _pick_column(df_cdnr, IGST_CANDIDATES_2B) if df_cdnr is not None else None

    # ---- Detection for PR ----
    inv_pr = _pick_column(df_pr_raw, INVOICE_CANDIDATES_PR, avoid_terms=AVOID_DOC_LIKE_FOR_PR,
                          extra_penalties=["gstin", "company", "recipient"])
    gst_pr = _pick_column(df_pr_raw, GSTIN_CANDIDATES_PR, extra_penalties=AVOID_RECIPIENT_GSTIN_FOR_PR)
    date_pr = _pick_column(df_pr_raw, DATE_CANDIDATES_PR)
    cgst_pr = _pick_column(df_pr_raw, CGST_CANDIDATES_PR)
    sgst_pr = _pick_column(df_pr_raw, SGST_CANDIDATES_PR)
    igst_pr = _pick_column(df_pr_raw, IGST_CANDIDATES_PR)

    if not gst_pr or gst_pr not in df_pr_raw.columns:
        guess = pick_gstin_by_values(df_pr_raw, prefer_supplier=True)
        if guess: gst_pr = guess
    if not inv_pr or inv_pr not in df_pr_raw.columns:
        guess = pick_invoice_by_values(df_pr_raw)
        if guess: inv_pr = guess

    cols_b2b = sorted(df_b2b.columns) if df_b2b is not None else []
    cols_cdnr = sorted(df_cdnr.columns) if df_cdnr is not None else []
    cols_pr = sorted(df_pr_raw.columns)

    return render_template(
        "verify.html",
        # PR
        cols_pr=cols_pr,
        inv_pr=inv_pr, gst_pr=gst_pr, date_pr=date_pr, cgst_pr=cgst_pr, sgst_pr=sgst_pr, igst_pr=igst_pr,
        # B2B
        cols_2b_b2b=cols_b2b,
        inv_2b_b2b=inv_2b_b2b, gst_2b_b2b=gst_2b_b2b, date_2b_b2b=date_2b_b2b,
        cgst_2b_b2b=cgst_2b_b2b, sgst_2b_b2b=sgst_2b_b2b, igst_2b_b2b=igst_2b_b2b,
        # CDNR
        cols_2b_cdnr=cols_cdnr,
        note_2b_cdnr=note_2b_cdnr, notedate_2b_cdnr=notedate_2b_cdnr, gst_2b_cdnr=gst_2b_cdnr,
        cgst_2b_cdnr=cgst_2b_cdnr, sgst_2b_cdnr=sgst_2b_cdnr, igst_2b_cdnr=igst_2b_cdnr,
        note_type_2b_cdnr=note_type_2b_cdnr,
        has_cdnr=("B2B-CDNR" in present_sheets)
    )

# ---------- helpers ----------
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

def _run_reconciliation_pipeline(tmp2b_path: str, tmppr_path: str,
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

    def load_sheet(name):
        df = pd.read_excel(tmp2b_path, sheet_name=name, header=[4, 5])
        df = df.dropna(how="all")
        if df.empty:
            return df
        df.columns = flatten_columns(df.columns)
        df, _ = normalize_columns(df)
        df["_SOURCE_SHEET"] = name
        return df

    df_b2b_raw = load_sheet("B2B") if has_b2b else pd.DataFrame()
    df_cdnr_raw = load_sheet("B2B-CDNR") if has_cdnr else pd.DataFrame()

    df_pr_raw = pd.read_excel(tmppr_path, engine="openpyxl", dtype=str)
    df_pr_raw, pr_norm_map = normalize_columns(df_pr_raw)

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

    def apply_signs(df, note_type_col: Optional[str]) -> pd.DataFrame:
        if df.empty:
            return df
        df = df.copy()

        def normalize_note_type(val: str) -> str:
            s = as_text(val).lower()
            if re.search(r'\bdebit\b|\bdn\b|\bdr\b', s):  return "debit"
            if re.search(r'\bcredit\b|\bcn\b|\bcr\b', s): return "credit"
            return ""

        if note_type_col and note_type_col in df.columns:
            df["_NOTE_TYPE"] = df[note_type_col].map(normalize_note_type)
        else:
            def infer(row):
                note = as_text(row.get(_pick_best_note_col(df, None) or "", ""))
                s = note.lower()
                if "dn" in s or "debit" in s or re.search(r'\bdr\b', s): return "debit"
                if "cn" in s or "credit" in s or re.search(r'\bcr\b', s): return "credit"
                return "credit" if row.get("_SOURCE_SHEET") == "B2B-CDNR" else "invoice"
            df["_NOTE_TYPE"] = df.apply(infer, axis=1)

        numeric_cols = [c for c in (
            CGST_CANDIDATES_2B + SGST_CANDIDATES_2B + IGST_CANDIDATES_2B +
            TAXABLE_CANDIDATES_2B + TOTAL_TAX_CANDIDATES_2B + INVOICE_VALUE_CANDIDATES_2B + CESS_CANDIDATES_2B
        ) if c in df.columns]
        for col in numeric_cols:
            df[col] = df.apply(lambda r: -parse_amount(r[col]) if r["_NOTE_TYPE"] == "credit" else parse_amount(r[col]), axis=1)
        return df

    df_b2b_raw = apply_signs(df_b2b_raw, note_type_col=None)
    df_cdnr_raw = apply_signs(df_cdnr_raw, note_type_col=note_type_2b)

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

    opt_2b_b2b = optional_numeric_list(df_b2b_raw)
    _append_if_missing(opt_2b_b2b, [ensure_col(df_b2b_raw, "", CGST_CANDIDATES_2B),
                                    ensure_col(df_b2b_raw, "", SGST_CANDIDATES_2B),
                                    ensure_col(df_b2b_raw, "", IGST_CANDIDATES_2B)])

    opt_2b_cdnr = optional_numeric_list(df_cdnr_raw)
    _append_if_missing(opt_2b_cdnr, [ensure_col(df_cdnr_raw, "", CGST_CANDIDATES_2B),
                                     ensure_col(df_cdnr_raw, "", SGST_CANDIDATES_2B),
                                     ensure_col(df_cdnr_raw, "", IGST_CANDIDATES_2B)])

    opt_pr = optional_numeric_list(df_pr_raw)
    _append_if_missing(opt_pr, [cgst_pr, sgst_pr, igst_pr])

    df_2b_b2b = consolidate_by_key(
        df=df_b2b_raw, gstin_col=gst_2b_b2b, inv_col=inv_2b_b2b,
        date_col=date_2b_b2b, numeric_cols=opt_2b_b2b
    ) if (df_b2b_raw is not None and not df_b2b_raw.empty and inv_2b_b2b and gst_2b_b2b) else pd.DataFrame()

    df_2b_cdnr = consolidate_by_key(
        df=df_cdnr_raw, gstin_col=gst_2b_cdnr, inv_col=note_2b_cdnr,
        date_col=notedate_2b_cdnr, numeric_cols=opt_2b_cdnr
    ) if (df_cdnr_raw is not None and not df_cdnr_raw.empty and note_2b_cdnr and gst_2b_cdnr) else pd.DataFrame()

    if not df_2b_cdnr.empty:
        df_2b_cdnr = _rescue_empty_inv_keys(df_2b_cdnr, inv_col=note_2b_cdnr)

    df_pr = consolidate_by_key(
        df=df_pr_raw, gstin_col=gst_pr, inv_col=inv_pr,
        date_col=date_pr, numeric_cols=opt_pr
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

    if "_NOTE_TYPE" in df_cdnr_raw.columns and not df_2b_cdnr.empty:
        nt_map = (df_cdnr_raw.assign(_GST_KEY=df_cdnr_raw[gst_2b_cdnr].map(clean_gstin),
                                     _INV_KEY=df_cdnr_raw[note_2b_cdnr].map(inv_basic))
                            .dropna(subset=["_GST_KEY","_INV_KEY"]))
        nt_map = nt_map.groupby(["_GST_KEY","_INV_KEY"])["_NOTE_TYPE"].first().to_dict()
        df_2b_cdnr["_NOTE_TYPE"] = df_2b_cdnr.apply(lambda r: nt_map.get((r["_GST_KEY"], r["_INV_KEY"]), ""), axis=1)

    df_2b = pd.concat([df_2b_b2b, df_2b_cdnr], ignore_index=True) if (not df_2b_b2b.empty or not df_2b_cdnr.empty) else df_2b_b2b

    combined_df, pair_cols = build_pairwise_recon(
        df_pr=df_pr, df_2b=df_2b,
        inv_pr=inv_pr, gst_pr=gst_pr, date_pr=date_pr, cgst_pr=cgst_pr, sgst_pr=sgst_pr, igst_pr=igst_pr,
        inv_2b="_INV_DISPLAY", gst_2b=(gst_2b_b2b or gst_2b_cdnr), date_2b="_DATE_DISPLAY",
        cgst_2b=(cgst_2b_b2b or cgst_2b_cdnr), sgst_2b=(sgst_2b_b2b or sgst_2b_cdnr), igst_2b=(igst_2b_b2b or igst_2b_cdnr)
    )

    # --- START: PR - Comments sheet generation ---
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
            return pd.Series(recon_lookup.get(key, ("", "", "")))

        pr_comments[["Mapping", "Remarks", "Reason"]] = pr_comments.apply(get_recon_status, axis=1)
        pr_comments.drop(columns=["_GST_KEY", "_INV_KEY"], inplace=True)
    else:
        pr_comments["Mapping"] = ""
        pr_comments["Remarks"] = ""
        pr_comments["Reason"] = ""
    # --- END: PR - Comments sheet generation ---

    invoice_type_map = df_2b_b2b.set_index(["_GST_KEY", "_INV_KEY"]).get("_SOURCE_SHEET", pd.Series()).to_dict() \
        if not df_2b_b2b.empty else {}
    note_type_map = df_2b_cdnr.set_index(["_GST_KEY", "_INV_KEY"]).get("_NOTE_TYPE", pd.Series()).to_dict() \
        if not df_2b_cdnr.empty else {}

    combined_df["Invoice Type"] = combined_df.apply(
        lambda r: invoice_type_map.get((r.get("_GST_KEY"), r.get("_INV_KEY")), ""), axis=1)
    combined_df["Note Type"] = combined_df.apply(
        lambda r: {"credit":"Credit Note","debit":"Debit Note"}.get(note_type_map.get((r.get("_GST_KEY"), r.get("_INV_KEY")), "").lower(), ""), axis=1)

    cols = list(combined_df.columns)
    if "Invoice Type" in cols and "Note Type" in cols:
        idx = cols.index("_INV_KEY") + 1
        for c in ["Note Type", "Invoice Type"][::-1]:
            cols.insert(idx, cols.pop(cols.index(c)))
        combined_df = combined_df[cols]

    def _autosize_ws(ws, df, min_w=12, max_w=48):
        sample = df.head(200)
        for col_idx, col_name in enumerate(df.columns):
            header_len = len(str(col_name)) + 4
            try:
                content_len = int(sample[col_name].astype(str).map(len).max())
                if isinstance(content_len, float) and math.isnan(content_len):
                    content_len = 0
            except Exception:
                content_len = 0
            width = max(min_w, min(max_w, max(header_len, content_len + 2)))
            ws.set_column(col_idx, col_idx, width)

    output = io.BytesIO()
    with pd.ExcelWriter(
        output,
        engine="xlsxwriter",
        engine_kwargs={"options": {"strings_to_urls": False}},
    ) as writer:
        wb = writer.book
        header_fmt = wb.add_format({"bold": True})

        combined_df.to_excel(writer, index=False, sheet_name="Reconciliation")
        ws = writer.sheets["Reconciliation"]
        ws.freeze_panes(1, 0)
        ws.autofilter(0, 0, len(combined_df), max(0, combined_df.shape[1] - 1))
        ws.set_row(0, None, header_fmt)
        _autosize_ws(ws, combined_df)

        ws2 = writer.book.add_worksheet("Dashboard")
        writer.sheets["Dashboard"] = ws2
        hdr_bold = header_fmt
        num_fmt  = wb.add_format({"num_format": "#,##0.00"})

        def _sum_component(status, col_name):
            if not col_name or col_name not in combined_df.columns:
                return 0.0
            mask = (combined_df["Mapping"] == status) if status else slice(None)
            return float(pd.to_numeric(combined_df.loc[mask, col_name], errors="coerce").fillna(0).sum())

        def _block_vals(status):
            cg_pr = _sum_component(status, pair_cols.get("cgst_pr_col"))
            sg_pr = _sum_component(status, pair_cols.get("sgst_pr_col"))
            ig_pr = _sum_component(status, pair_cols.get("igst_pr_col"))
            tot_pr = cg_pr + sg_pr + ig_pr

            cg_2b = _sum_component(status, pair_cols.get("cgst_2b_col"))
            sg_2b = _sum_component(status, pair_cols.get("sgst_2b_col"))
            # --- START: THIS IS THE FIX ---
            ig_2b = _sum_component(status, pair_cols.get("igst_2b_col")) # Corrected "igst_b_col" to "igst_2b_col"
            # --- END: THIS IS THE FIX ---
            tot_2b = cg_2b + sg_2b + ig_2b

            return ([cg_pr, sg_pr, ig_pr, tot_pr], [cg_2b, sg_2b, ig_2b, tot_2b])

        statuses = ["Matched", "Almost Matched", "Not Matched", None]
        rowlabels = ["CGST", "SGST", "IGST", "Total"]

        ws2.write(0, 0, "Status", hdr_bold)
        col = 1
        for st in statuses:
            title = "Total" if st is None else st
            ws2.merge_range(0, col, 0, col+1, title, hdr_bold)
            col += 2

        ws2.write(1, 0, "Report", hdr_bold)
        col = 1
        for _ in statuses:
            ws2.write(1, col,     "PR",      hdr_bold)
            ws2.write(1, col + 1, "GSTR 2B", hdr_bold)
            col += 2

        for r, label in enumerate(rowlabels, start=2):
            ws2.write(r, 0, label, hdr_bold)

        col = 1
        for st in statuses:
            pr_vals, b2_vals = _block_vals(st)
            for r, v in enumerate(pr_vals, start=2):
                ws2.write_number(r, col, v, num_fmt)
            for r, v in enumerate(b2_vals, start=2):
                ws2.write_number(r, col+1, v, num_fmt)
            col += 2

        ws2.freeze_panes(2, 0)
        for c in range(0, 1 + len(statuses)*2):
            ws2.set_column(c, c, 14)
        ws2.set_column(0, 0, 12)

        pr_comments.to_excel(writer, index=False, sheet_name="PR - Comments")
        ws3 = writer.sheets["PR - Comments"]
        ws3.freeze_panes(1, 0)
        ws3.autofilter(0, 0, len(pr_comments), max(0, pr_comments.shape[1] - 1))
        ws3.set_row(0, None, header_fmt)
        _autosize_ws(ws3, pr_comments)

    output.seek(0)
    return output.read()

# ------- NEW: Background worker task (called by RQ) -------
from rq import get_current_job
import tempfile, os, time
from concurrent.futures import ThreadPoolExecutor

def process_reconcile(drive_id_2b: str, drive_id_pr: str, selections: dict, user_id: str = "anon") -> dict:
    def _mark(pct, msg):
        j = get_current_job()
        if j:
            j.meta["progress"] = {"pct": int(pct), "msg": msg}
            j.save_meta()
        print(f"[{time.strftime('%H:%M:%S')}] {pct}% - {msg}", flush=True)

    t0 = time.time()
    _mark(3, "Starting")
    with tempfile.TemporaryDirectory() as td:
        in2b = os.path.join(td, "gstr2b.xlsx")
        inpr = os.path.join(td, "purchase_register.xlsx")

        _mark(5, "Downloading inputs from Drive")
        def _dl(fid, path):
            download_from_drive(fid, path)
            return path
        with ThreadPoolExecutor(max_workers=2) as ex:
            fut2b = ex.submit(_dl, drive_id_2b, in2b)
            futpr = ex.submit(_dl, drive_id_pr, inpr)
            fut2b.result(); futpr.result()
        _mark(22, f"Downloads finished in {time.time()-t0:.1f}s")

        _mark(30, "Reconciling")
        x = selections
        blob = _run_reconciliation_pipeline(
            in2b, inpr,
            # B2B
            x.get("inv_2b_b2b",""), x.get("gst_2b_b2b",""), x.get("date_2b_b2b",""), x.get("cgst_2b_b2b",""), x.get("sgst_2b_b2b",""), x.get("igst_2b_b2b",""),
            # CDNR
            x.get("note_2b_cdnr",""), x.get("gst_2b_cdnr",""), x.get("notedate_2b_cdnr",""), x.get("cgst_2b_cdnr",""), x.get("sgst_2b_cdnr",""), x.get("igst_2b_cdnr",""),
            # PR
            x.get("inv_pr",""), x.get("gst_pr",""), x.get("date_pr",""), x.get("cgst_pr",""), x.get("sgst_pr",""), x.get("igst_pr","")
        )
        _mark(82, f"Reconcile+write took {time.time()-t0:.1f}s")

        _mark(85, "Uploading result to Drive")
        out_path = os.path.join(td, f"{user_id}_gstr2b_pr_reconciliation.xlsx")
        with open(out_path, "wb") as f:
            f.write(blob)
        result_id = upload_to_drive(out_path, os.path.basename(out_path))
        _mark(100, f"Done in {time.time()-t0:.1f}s")

    return {"result_drive_id": result_id}

# -------------------- QUEUED confirm route + status + download --------------------
@app.route("/reconcile_confirm", methods=["POST"])
def reconcile_confirm():
    tmp2b = session.get("tmp2b"); tmppr = session.get("tmppr")
    if not tmp2b or not tmppr or (not os.path.exists(tmp2b)) or (not os.path.exists(tmppr)):
        flash("Upload session expired. Please re-upload the files.")
        return redirect(url_for("index"))

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
        try:
            os.remove(tmp2b); os.remove(tmppr)
        except Exception:
            pass
        session.pop("tmp2b", None); session.pop("tmppr", None)

    user_id = request.form.get("user_id", "anon")

    # --- START: REPLACE THIS BLOCK ---
    job = q.enqueue(
        "main.process_reconcile",
        drive_id_2b, drive_id_pr, sel, user_id,
        job_timeout=-1,  # <-- FIX 1: Explicitly disable the timeout for the worker.
        result_ttl=86400,
        failure_ttl=86400
    )
    # --- FIX 2: Ensure this return statement is present right after creating the job. ---
    return render_template("progress.html", job_id=job.id)
    # --- END: REPLACE THIS BLOCK ---

@app.route("/status/<job_id>", methods=["GET"])
def status(job_id):
    job = Job.fetch(job_id, connection=rconn)
    meta = job.meta.get("progress", {"pct": 0, "msg": "queued"})
    state = job.get_status()
    payload = {"state": state, "progress": meta}
    if state == "finished":
        payload["result"] = job.result
    elif state == "failed":
        payload["error"] = (job.exc_info or "")[-1000:]
    return jsonify(payload)

from flask import after_this_request

@app.route("/download/<job_id>", methods=["GET"])
def download(job_id):
    job = Job.fetch(job_id, connection=rconn)
    if job.get_status() != "finished":
        return jsonify({"error": "Job not finished"}), 409

    drive_id = job.result.get("result_drive_id")
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
        download_name="gstr2b_pr_reconciliation.xlsx",
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
        if user and check_password_hash(user.password_hash, password):
            session['logged_in'] = True
            session['email'] = email
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
            flash('Password reset link would be sent (simulated).')
        else:
            flash('Email not found.')
    return render_template('forgot_password.html')

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
import os
import hashlib

def _sanitize_env_value(val: Optional[str]) -> str:
    """Strip whitespace and inline comments from env-var values."""
    if val is None:
        return ""
    s = str(val)
    if "#" in s:
        s = s.split("#", 1)[0]
    return s.strip()


def _format_amount(amount_raw: Any) -> str:
    """Force amount to two-decimal string (e.g. 20000 → 20000.00)."""
    try:
        return "{:.2f}".format(float(amount_raw))
    except (ValueError, TypeError):
        return str(amount_raw).strip()


def build_hash_sequence(params: Dict[str, Any], salt: str) -> str:
    """
    Return the *exact* pipe-separated string that PayU hashes.
    The order is fixed – 18 fields, 6 of them empty after udf5.
    """
    def _trim(x):
        return "" if x is None else str(x).strip()

    # ---- mandatory fields -------------------------------------------------
    key         = _trim(params.get("key", _sanitize_env_value(os.getenv("PAYU_MERCHANT_KEY", ""))))
    txnid       = _trim(params.get("txnid", ""))
    amount      = _format_amount(params.get("amount", "0"))
    productinfo = _trim(params.get("productinfo", ""))
    firstname   = _trim(params.get("firstname", ""))
    email       = _trim(params.get("email", ""))

    # ---- validation -------------------------------------------------------
    if not firstname:
        raise ValueError("firstname is required and cannot be empty")
    if "@" in firstname:
        raise ValueError("firstname cannot be an e-mail address – use the actual name")

    # ---- UDF 1-5 (always present, even if empty) -------------------------
    udf1 = _trim(params.get("udf1", ""))
    udf2 = _trim(params.get("udf2", ""))
    udf3 = _trim(params.get("udf3", ""))
    udf4 = _trim(params.get("udf4", ""))
    udf5 = _trim(params.get("udf5", ""))

    # ---- final parts -------------------------------------------------------
    parts = [
        key, txnid, amount, productinfo, firstname, email,
        udf1, udf2, udf3, udf4, udf5,
        "", "", "", "", "", "",          # **exactly 6** empty fields
        salt
    ]
    return "|".join(parts)


def payu_hash(
    params: Dict[str, Any],
    salt: Optional[str] = None,
    merchant_key: Optional[str] = None,
) -> str:
    """
    Compute PayU SHA-512 hash (lowercase hex).

    * `salt` – explicit salt for local testing; otherwise reads PAYU_SALT env var.
    * `merchant_key` – optional override for the key (adds/replaces params['key']).
    """
    if merchant_key:
        params = dict(params)
        params["key"] = merchant_key

    salt_val = _sanitize_env_value(salt if salt is not None else os.getenv("PAYU_SALT", ""))
    if not salt_val:
        raise ValueError("PayU SALT is missing – set PAYU_SALT env var or pass it explicitly")

    seq = build_hash_sequence(params, salt_val)
    return hashlib.sha512(seq.encode("utf-8")).hexdigest().lower()

# Add this compatibility wrapper right after the `payu_hash` function
def _payu_hash(params, salt=None, merchant_key=None):
    """
    Backwards-compatible wrapper expected by legacy call-sites.
    Delegates to the canonical `payu_hash` implementation.
    """
    return payu_hash(params, salt=salt, merchant_key=merchant_key)

# -------------------------------------------------------------------------
# Helper for safe logging (replaces the real salt with <SALT>)
# -------------------------------------------------------------------------
def masked_sequence(full_seq: str, salt: str) -> str:
    if not salt:
        return full_seq.rsplit("|", 1)[0] + "|<SALT>" if "|" in full_seq else full_seq
    if full_seq.endswith(salt):
        return full_seq[:-len(salt)] + "<SALT>"
    return full_seq.rsplit("|", 1)[0] + "|<SALT>" if "|" in full_seq else full_seq


# -------------------------------------------------------------------------
# Stand-alone test (run with `python payu_hash.py`)
# -------------------------------------------------------------------------
if __name__ == "__main__":
    test_params = {
        "key": "pqBnUd",
        "txnid": "txn_1762602100_343f2d0a76e1",
        "amount": "20000",
        "productinfo": "1-year subscription",
        "firstname": "Deepak Rao",               # ← **REAL NAME**
        "email": "deepak.rao@acertax.com",
        "udf1": "annual",
        # udf2-udf5 are omitted → will be empty strings
    }
    TEST_SALT = "vVZxI7Upbecje3BdcJgjhP6kKmMppX0W"   # **only for local testing**

    seq = build_hash_sequence(test_params, TEST_SALT)
    masked = masked_sequence(seq, TEST_SALT)
    computed = payu_hash(test_params, salt=TEST_SALT)

    print("Masked pipe-sequence:")
    print(masked)
    print("Field count:", len(seq.split("|")), "(expected 18)\n")
    print("Computed hash:")
    print(computed)

    expected = "aa0eead674b8d1e3ea15c0268f003fff7fe66fb8138f71bba5b644f55a30faf316bed6320d56ec9480787ef924c667563aa1d8e210475a18c6a3eb887b118b4a"
    print("\nPayU expected:")
    print(expected)
    print("MATCH?", computed == expected)

def verify_payu_response(posted: dict) -> bool:
    """
    Verify PayU response (compare posted 'hash' or 'sha2' with computed value).
    Response hash formula:
      sha512(salt|status|udf10|udf9|...|udf1|email|firstname|productinfo|amount|txnid|key)
    """
    posted_hash = (posted.get("hash") or posted.get("sha2") or "").strip().lower()
    status = str(posted.get("status", "")).strip()
    txnid = str(posted.get("txnid", "")).strip()
    amount = str(posted.get("amount", "")).strip()
    productinfo = str(posted.get("productinfo", "")).strip()
    firstname = str(posted.get("firstname", "")).strip()
    email = str(posted.get("email", "")).strip()
    key = str(posted.get("key", os.environ.get("PAYU_MERCHANT_KEY", "") or "")).strip()
    salt = os.environ.get("PAYU_SALT", "").strip()

    # collect udf10..udf1 from posted (use empty strings if not present)
    udf_list = []
    for i in range(10, 0, -1):
        udf_list.append(str(posted.get(f"udf{i}", "") or "").strip())

    parts = [salt, status] + udf_list + [email, firstname, productinfo, amount, txnid, key]
    hash_seq = "|".join(parts)
    calc_hash = hashlib.sha512(hash_seq.encode("utf-8")).hexdigest().lower()
    return calc_hash == posted_hash

# Insert/replace the following helper and the `create_payment` route in main.py

import hashlib
import os
from flask import request, session, url_for
import html
import time
import secrets
import re

# ... (keep your other imports and Flask app setup) ...

def payu_request_hash(params: dict) -> str:
    """
    Computes the PayU request hash using the exact formula:
    sha512(key|txnid|amount|productinfo|firstname|email|udf1|udf2|udf3|udf4|udf5||||||SALT)
    This means exactly 5 empty placeholders after udf5.
    """
    payu_salt = os.environ.get("PAYU_SALT", "").strip()

    # The order of fields is absolutely critical.
    hash_parts = [
        str(params.get("key", "")),
        str(params.get("txnid", "")),
        str(params.get("amount", "")),
        str(params.get("productinfo", "")),
        str(params.get("firstname", "")),
        str(params.get("email", "")),
        str(params.get("udf1", "")),
        str(params.get("udf2", "")),
        str(params.get("udf3", "")),
        str(params.get("udf4", "")),
        str(params.get("udf5", "")),
        "",  # Placeholder 1
        "",  # Placeholder 2
        "",  # Placeholder 3
        "",  # Placeholder 4
        "",  # Placeholder 5
    ]

    hash_sequence_str = "|".join(hash_parts) + "|" + payu_salt

    # Compute and return the lowercase hex digest.
    calculated_hash = hashlib.sha512(hash_sequence_str.encode('utf-8')).hexdigest().lower()
    return calculated_hash

@app.route("/create_payment", methods=["POST"])
def create_payment():
    """
    Creates and submits the payment request to PayU with the corrected hash.
    """
    # Read and format inputs
    plan = request.form.get("plan", "half-yearly")
    amount_raw = request.form.get("amount", "0")
    productinfo = request.form.get("description", "Subscription").strip()
    phone = request.form.get("phone", "")

    try:
        amount = "{:.2f}".format(float(amount_raw))
    except (ValueError, TypeError):
        amount = "0.00"

    email = (session.get("email") or request.form.get("email") or "").strip()
    if email and "@" in email:
        firstname = email.split('@', 1)[0].lower()
    else:
        firstname = "guest"

    # Read environment variables and validate
    payu_key = os.environ.get("PAYU_MERCHANT_KEY", "").strip()
    payu_salt = os.environ.get("PAYU_SALT", "").strip()
    payu_url = os.environ.get("PAYU_BASE_URL", "https://test.payu.in/_payment").strip()

    if not payu_key or not payu_salt:
        return "<h2>Configuration Error: PAYU_MERCHANT_KEY or PAYU_SALT is not set.</h2>", 500

    # Assemble all parameters for the form submission
    txnid = f"txn_{int(time.time())}_{secrets.token_hex(6)}"
    payu_params = {
        "key": payu_key, "txnid": txnid, "amount": amount, "productinfo": productinfo,
        "firstname": firstname, "email": email, "phone": phone,
        "surl": url_for("payment_success", _external=True),
        "furl": url_for("payment_fail", _external=True),
        "udf1": plan, "udf2": "", "udf3": "", "udf4": "", "udf5": "",
        "udf6": "", "udf7": "", "udf8": "", "udf9": "", "udf10": "",
        "service_provider": "payu_paisa"
    }

    # Calculate the hash using the new, correct function
    payu_params['hash'] = payu_request_hash(payu_params)

    # Render the auto-submitting form
    return f"""
    <!doctype html><html><head><title>Redirecting to PayU...</title></head>
    <body onload="document.getElementById('payu_form').submit();">
      <h3>Redirecting to PayU...</h3>
      <form id="payu_form" method="POST" action="{html.escape(payu_url)}">
        {''.join([f'<input type="hidden" name="{k}" value="{html.escape(str(v))}">' for k, v in payu_params.items()])}
        <input type="submit" value="Click here if you are not redirected">
      </form>
    </body></html>
    """

from flask import Flask, render_template, render_template_string, request, send_file, flash, redirect, url_for, session, jsonify
@app.route("/compare_payu_seq", methods=["POST"])
def compare_payu_seq():
    our_seq = request.form.get("our_seq", "")
    payu_seq = request.form.get("payu_seq", "")
    def diff_lines(a, b):
        # simple diff: show both and indicate equal/unequal
        same = (a.strip() == b.strip())
        return {"same": same, "our_seq": a, "payu_seq": b}
    res = diff_lines(our_seq, payu_seq)
    # render minimal result
    return render_template_string("""
      <h2>Compare result</h2>
      <p>Sequences identical: <strong>{{same}}</strong></p>
      <h3>Our sequence (masked)</h3><pre>{{our_seq}}</pre>
      <h3>PayU provided sequence</h3><pre>{{payu_seq}}</pre>
      <p><a href="#" onclick="history.back(); return false;">Back</a></p>
    """, **res)

# PayU success/failure endpoints (surl/furl) — adapt to verify and update DB
@app.route("/payment_success", methods=["POST", "GET"])
def payment_success():
    posted = request.form.to_dict() if request.form else request.args.to_dict()
    ok = verify_payu_response(posted)
    if not ok:
        # log suspicious payload and show error
        app.logger.warning("PayU verification failed for payload: %s", {k: posted.get(k) for k in posted.keys() if k != 'hash' and k != 'sha2'})
        return render_template("payment_failed.html", reason="Hash verification failed", data=posted), 400

    # verify txnid/amount match DB record (if you persist txnid earlier)
    # then update Transaction record and mark user's subscription active
    return render_template("payment_success.html", data=posted)

@app.route("/payment_fail", methods=["POST", "GET"])
def payment_fail():
    data = request.form.to_dict() or request.args.to_dict()
    # TODO: log failure and show next steps to user
    return render_template("payment_failed.html", data=data)

# dev-only – paste into main.py while debugging (only when app.debug is True)
@app.route("/_debug_env", methods=["GET"])
def _debug_env():
    if not app.debug:
        return ("Not allowed", 403)
    key = os.environ.get("PAYU_MERCHANT_KEY", "")
    masked = (key[:4] + "..." ) if key else ""
    return f"PAYU key present: {bool(key)}; masked key: {masked}; PAYU_BASE_URL: {os.environ.get('PAYU_BASE_URL')}"

def debug_payu_hash_print(params: dict):
    # prints masked sequence (salt replaced) and computed hash (with actual salt)
    key = str(params.get("key", os.environ.get("PAYU_MERCHANT_KEY", ""))).strip()
    txnid = str(params.get("txnid", "")).strip()
    amount = "{:.2f}".format(float(params.get("amount", "0") or 0))
    productinfo = str(params.get("productinfo","")).strip()
    firstname = str(params.get("firstname","")).strip()
    email = str(params.get("email","")).strip()
    udf = [str(params.get(f"udf{i}", "") or "").strip() for i in range(1, 11)]
    salt = os.environ.get("PAYU_SALT", "").strip()

    parts_display = [key, txnid, amount, productinfo, firstname, email] + udf + ["<SALT>"]
    display_seq = "|".join(parts_display)
    calc = _payu_hash(params)
    print("HASH SEQ (masked):", display_seq)
    print("SHA512(hash_seq):", calc)
    # Do NOT print the raw salt in logs in production..


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

@app.route('/admin')
@admin_required
def admin_dashboard():
    """Displays the admin dashboard with a list of all users."""
    all_users = User.query.order_by(User.id).all()
    return render_template('admin.html', users=all_users)

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

if __name__ == "__main__":
    app.run(debug=True)
