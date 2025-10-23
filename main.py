import io
import re
import os
import math
import tempfile
from datetime import datetime
from typing import Optional, Dict, List, Tuple
from collections import defaultdict

from flask import Flask, render_template, request, send_file, flash, redirect, url_for, session, jsonify

import pandas as pd

# ------- NEW: Queue & Google Drive imports -------
from redis import Redis
from rq import Queue, get_current_job
from rq.job import Job

from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

# -------------------- Flask & App Config --------------------

app = Flask(__name__)
app.secret_key = "change-this-secret"
app.config['MAX_CONTENT_LENGTH'] = 25 * 1024 * 1024  # 25 MB

# ------- NEW: Redis Queue (for background jobs) -------
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
rconn = Redis.from_url(REDIS_URL)
q = Queue("reconcile", connection=rconn)

# ------- NEW: Google Drive service (service account) -------
SCOPES = ['https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_FILE = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', 'cred.json')
DRIVE_FOLDER_ID = os.environ.get('DRIVE_FOLDER_ID', None)

def _drive_service():
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
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
        supportsAllDrives=True     # <-- add this
    ).execute()
    return file.get('id')

def download_from_drive(file_id: str, local_path: str):
    service = _drive_service()
    request = service.files().get_media(
        fileId=file_id,
        supportsAllDrives=True     # <-- add this
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
GSTIN_CANDIDATES_2B = [
    "gstin of supplier", "supplier information gstin of supplier",
    "gstin", "gst no", "gst number", "gstn"
]
DATE_CANDIDATES_2B = [
    "invoice details invoice date", "invoice date", "doc date", "document date", "date"
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
GSTR1_STATUS_2B_CANDIDATES = [
    "gstr-1 filing status", "gstr1 filing status", "filing status", "filing status details",
    "gstr1 status", "gstr-1 status", "status", "tax period", "return period"
]

# -------------------- Column detection helpers --------------------

def _norm(s: str) -> str:
    return re.sub(r'[^a-z0-9]', '', str(s).strip().lower())

def _softnorm(s: str) -> str:
    """Looser normalizer for header matching: replace NBSP, collapse spaces, strip, lowercase."""
    if s is None:
        return ""
    s = str(s).replace("\xa0", " ")
    s = re.sub(r'\s+', ' ', s).strip()
    return s.lower()

def normalize_columns(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, str]]:
    """
    Normalize all column names (trim, collapse spaces, replace NBSP, lowercase for matching),
    but keep and return a map from normalized -> original so we can use the exact original
    names later (for writing back to Excel).
    """
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
            parts = [str(x).strip() for x in tup if (x is not None and str(x).strip().lower() != "nan")]
            out.append(" ".join(parts))
        return out
    return [str(c) for c in cols]

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
        if len(x) < 4 or len(x) > 25: return False
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

def inv_basic(s) -> str:
    v = as_text(s).upper()
    v = re.sub(r'\s+', '', v)
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
    if isinstance(x, (pd.Timestamp, datetime)):
        try:
            return pd.to_datetime(x).date()
        except Exception:
            pass
    if re.fullmatch(r"\d{4,6}", s):
        dt = _parse_excel_serial(s)
        if not pd.isna(dt):
            return dt.date()
    dt = pd.to_datetime(s, dayfirst=True, errors='coerce')
    if not pd.isna(dt):
        return dt.date()
    for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d", "%Y/%m/%d",
                "%d.%m.%Y", "%d-%b-%Y", "%d-%b-%y", "%d %b %Y",
                "%m/%d/%Y", "%m-%d-%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            continue
    return None

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
            mappings.append("not matched"); remarks.append("no GSTIN+Invoice in PR"); reasons.append("missing GSTIN/Invoice in PR"); continue

        key = concat_key(gst_pr, inv_pr)
        cand_idxs = b2_lookup.get(key, [])

        if not cand_idxs:
            mappings.append("not matched"); remarks.append("no GSTIN+Invoice match"); reasons.append(""); continue

        idx2b = cand_idxs[0]
        row2b = df_2b.iloc[idx2b]

        mismatches = []

        if date_col_pr and date_col_2b:
            d_pr = parse_date_cell(row.get(date_col_pr, ""))
            d_2b = parse_date_cell(row2b.get(date_col_2b, ""))
            if (d_pr or d_2b) and (d_pr != d_2b):
                mismatches.append("Invoice Date")

        def eq_amt_round(a, b):
            return round_rupee(a) == round_rupee(b)

        if cgst_col_pr and cgst_col_2b:
            if not eq_amt_round(row.get(cgst_col_pr, 0), row2b.get(cgst_col_2b, 0)):
                mismatches.append("CGST")
        if sgst_col_pr and sgst_col_2b:
            if not eq_amt_round(row.get(sgst_col_pr, 0), row2b.get(sgst_col_2b, 0)):
                mismatches.append("SGST")
        if igst_col_pr and igst_col_2b:
            if not eq_amt_round(row.get(igst_col_pr, 0), row2b.get(igst_col_2b, 0)):
                mismatches.append("IGST")

        if mismatches:
            remarks.append("mismatch")
            extra = []
            if len(cand_idxs) > 1:
                extra.append(f"multiple matches in 2B ({len(cand_idxs)})")
            reasons.append("; ".join(mismatches + extra))
            mappings.append("Almost Match")
        else:
            extra = []
            if len(cand_idxs) > 1:
                extra.append(f"multiple matches in 2B ({len(cand_idxs)})")
            remarks.append("All fields matched" + ("" if not extra else f" ({'; '.join(extra)})"))
            reasons.append("")
            mappings.append("matched")

    out = df_pr.copy()
    out["Mapping"] = mappings
    out["Remarks"] = remarks
    out["Reason"] = reasons
    return out

# -------------------- Pairwise combined output --------------------

def build_pairwise_recon(
    df_pr: pd.DataFrame, df_2b: pd.DataFrame,
    inv_pr: str, gst_pr: str, date_pr: str, cgst_pr: str, sgst_pr: str, igst_pr: str,
    inv_2b: str, gst_2b: str, date_2b: str, cgst_2b: str, sgst_2b: str, igst_2b: str,
):
    # Explicit suffixing
    pr = df_pr.copy()
    b2 = df_2b.copy()
    for c in list(pr.columns):
        if c not in ["_GST_KEY", "_INV_KEY"]:
            pr.rename(columns={c: f"{c}_PR"}, inplace=True)
    for c in list(b2.columns):
        if c not in ["_GST_KEY", "_INV_KEY"]:
            b2.rename(columns={c: f"{c}_2B"}, inplace=True)

    merged = pd.merge(pr, b2, on=["_GST_KEY", "_INV_KEY"], how="outer")

    # -------- GROUP FILL: ensure PR/2B values appear on all rows for the same key --------
    key_grp = merged.groupby(["_GST_KEY", "_INV_KEY"], dropna=False)
    pr_cols_all = [c for c in merged.columns if c.endswith("_PR")]
    b2_cols_all = [c for c in merged.columns if c.endswith("_2B")]

    for col in pr_cols_all:
        merged[col] = key_grp[col].transform(lambda s: s.ffill().bfill())
    for col in b2_cols_all:
        merged[col] = key_grp[col].transform(lambda s: s.ffill().bfill())
    # -------------------------------------------------------------------------------------

    # Column names used later
    inv_pr_col = f"{inv_pr}_PR" if inv_pr else None
    gst_pr_col = f"{gst_pr}_PR" if inv_pr else None and f"{gst_pr}_PR"
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

    # Mapping/remarks/reason (Almost Match when any field differs)
    mapping, remarks, reason = [], [], []
    for _, r in merged.iterrows():
        pr_inv_val = as_text(r.get(inv_pr_col, "")) if inv_pr_col in merged.columns else ""
        pr_gst_val = clean_gstin(r.get(gst_pr_col, "")) if gst_pr_col in merged.columns else ""
        b2_inv_val = as_text(r.get(inv_2b_col, "")) if inv_2b_col in merged.columns else ""
        b2_gst_val = clean_gstin(r.get(gst_2b_col, "")) if gst_2b_col in merged.columns else ""

        pr_present = bool(pr_inv_val) or bool(pr_gst_val)
        b2_present = bool(b2_inv_val) or bool(b2_gst_val)

        if pr_present and not b2_present:
            mapping.append("not matched"); remarks.append("missing in 2B"); reason.append(""); continue
        if b2_present and not pr_present:
            mapping.append("not matched"); remarks.append("missing in PR"); reason.append(""); continue

        mismatches = []
        if date_pr_col and date_2b_col:
            d_pr = parse_date_cell(r.get(date_pr_col, ""))
            d_2b = parse_date_cell(r.get(date_2b_col, ""))
            if (d_pr or d_2b) and (d_pr != d_2b):
                mismatches.append("Invoice Date")

        def neq_round(a, b):
            return round_rupee(a) != round_rupee(b)

        if cgst_pr_col and cgst_2b_col and neq_round(r.get(cgst_pr_col, 0), r.get(cgst_2b_col, 0)):
            mismatches.append("CGST")
        if sgst_pr_col and sgst_2b_col and neq_round(r.get(sgst_pr_col, 0), r.get(sgst_2b_col, 0)):
            mismatches.append("SGST")
        if igst_pr_col and igst_2b_col and neq_round(r.get(igst_pr_col, 0), r.get(igst_2b_col, 0)):
            mismatches.append("IGST")

        if mismatches:
            mapping.append("Almost Match"); remarks.append("mismatch"); reason.append("; ".join(mismatches))
        else:
            mapping.append("matched"); remarks.append("All fields matched"); reason.append("")

    out = merged.copy()
    out["Mapping"] = mapping
    out["Remarks"] = remarks
    out["Reason"] = reason

    # Fix PR invoice display if blank/zero
    if inv_pr_col in out.columns:
        out[inv_pr_col] = out[inv_pr_col].map(as_text)
        mask_fix = (out[inv_pr_col].isin(["", "0"])) | (out[inv_pr_col].isna())
        out.loc[mask_fix, inv_pr_col] = out.loc[mask_fix, "_INV_KEY"]

    # Detect Vendor Name (PR) and GSTR-1 Filing Status (2B)
    def pick_from_list(columns, candidates):
        cnorm = [_norm(c) for c in candidates]
        best = None; best_score = -1
        for col in columns:
            n = _norm(col); score = 0
            if any(n == c for c in cnorm): score += 4
            if any(c in n for c in cnorm): score += 2
            if "name" in n: score += 1
            if score > best_score: best, best_score = col, score
        return best if best_score > 0 else None

    pr_cols_all = [c for c in out.columns if c.endswith("_PR")]
    b2_cols_all = [c for c in out.columns if c.endswith("_2B")]

    vendor_name_pr = pick_from_list(pr_cols_all, [f"{x}_PR" for x in VENDOR_NAME_PR_CANDIDATES])
    gstr1_status_2b = pick_from_list(b2_cols_all, [f"{x}_2B" for x in GSTR1_STATUS_2B_CANDIDATES])

    # Order columns
    pair_cols = [
        gst_pr_col, gst_2b_col,
        inv_pr_col, inv_2b_col,
        date_pr_col, date_2b_col,
        cgst_pr_col, cgst_2b_col,
        sgst_pr_col, sgst_2b_col,
        igst_pr_col, igst_2b_col,
    ]
    pair_cols = [c for c in pair_cols if c and c in out.columns]
    keep_extra = [c for c in [vendor_name_pr, gstr1_status_2b] if c and c in out.columns]

    front = ["_GST_KEY", "_INV_KEY", "Mapping", "Remarks", "Reason"]
    final_cols = front + pair_cols + keep_extra
    return out[final_cols], {
        "cgst_pr_col": cgst_pr_col, "sgst_pr_col": sgst_pr_col, "igst_pr_col": igst_pr_col,
        "cgst_2b_col": cgst_2b_col, "sgst_2b_col": sgst_2b_col, "igst_2b_col": igst_2b_col,
        "gstr1_status_2b_col": gstr1_status_2b  # <-- expose for "2B month"
    }

# -------------------- Dashboard --------------------

def build_dashboard(df_recon: pd.DataFrame, cols: Dict[str, Optional[str]]) -> pd.DataFrame:
    status_col = "Mapping"
    statuses = ["matched", "Almost Match", "not matched"]

    def sum_by(status, colname):
        if not colname or colname not in df_recon.columns:
            return 0.0
        mask = df_recon[status_col].str.lower().eq(status.lower())
        return float(pd.to_numeric(df_recon.loc[mask, colname], errors="coerce").fillna(0).sum())

    def count_by(status):
        return int((df_recon[status_col].str.lower() == status.lower()).sum())

    rows = []
    rows.append(["Count (rows)",
                 count_by("matched"), count_by("Almost Match"), count_by("not matched")])

    cgst_pr = [sum_by(s, cols["cgst_pr_col"]) for s in statuses]
    sgst_pr = [sum_by(s, cols["sgst_pr_col"]) for s in statuses]
    igst_pr = [sum_by(s, cols["igst_pr_col"]) for s in statuses]
    total_pr = [a+b+c for a,b,c in zip(cgst_pr, sgst_pr, igst_pr)]

    rows.append(["CGST (PR)"] + cgst_pr)
    rows.append(["SGST (PR)"] + sgst_pr)
    rows.append(["IGST (PR)"] + igst_pr)
    rows.append(["Total Tax (PR)"] + total_pr)

    cgst_2b = [sum_by(s, cols["cgst_2b_col"]) for s in statuses]
    sgst_2b = [sum_by(s, cols["sgst_2b_col"]) for s in statuses]
    igst_2b = [sum_by(s, cols["igst_2b_col"]) for s in statuses]
    total_2b = [a+b+c for a,b,c in zip(cgst_2b, sgst_2b, igst_2b)]

    rows.append(["CGST (2B)"] + cgst_2b)
    rows.append(["SGST (2B)"] + sgst_2b)
    rows.append(["IGST (2B)"] + igst_2b)
    rows.append(["Total Tax (2B)"] + total_2b)

    return pd.DataFrame(rows, columns=["Metric", "Matched", "Almost Match", "Not Matched"])

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
    xls_2b = pd.read_excel(tmp2b.name, sheet_name=wanted_sheets, header=[4, 5])

    frames = []
    for _, df in xls_2b.items():
        df = df.copy()
        df.columns = flatten_columns(df.columns)
        df = df.dropna(how="all")
        if not df.empty:
            df, _ = normalize_columns(df)  # normalize 2B headers
            frames.append(df)
    df_2b_raw = pd.concat(frames, ignore_index=True)

    df_pr_raw = pd.read_excel(tmppr.name)
    df_pr_raw, _ = normalize_columns(df_pr_raw)  # normalize PR headers

    inv_2b = _pick_column(df_2b_raw, INVOICE_CANDIDATES_2B)
    gst_2b = _pick_column(df_2b_raw, GSTIN_CANDIDATES_2B)
    date_2b = _pick_column(df_2b_raw, DATE_CANDIDATES_2B)
    cgst_2b = _pick_column(df_2b_raw, CGST_CANDIDATES_2B)
    sgst_2b = _pick_column(df_2b_raw, SGST_CANDIDATES_2B)
    igst_2b = _pick_column(df_2b_raw, IGST_CANDIDATES_2B)

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

    cols_2b = sorted(df_2b_raw.columns)
    cols_pr = sorted(df_pr_raw.columns)

    return render_template(
        "verify.html",
        cols_2b=cols_2b, cols_pr=cols_pr,
        inv_2b=inv_2b, gst_2b=gst_2b, date_2b=date_2b, cgst_2b=cgst_2b, sgst_2b=sgst_2b, igst_2b=igst_2b,
        inv_pr=inv_pr, gst_pr=gst_pr, date_pr=date_pr, cgst_pr=cgst_pr, sgst_pr=sgst_pr, igst_pr=igst_pr
    )

# ------- Extracted heavy logic into a helper so the worker can call it -------
def _run_reconciliation_pipeline(tmp2b_path: str, tmppr_path: str,
                                 inv_2b_sel: str, gst_2b_sel: str, date_2b_sel: str, cgst_2b_sel: str, sgst_2b_sel: str, igst_2b_sel: str,
                                 inv_pr_sel: str, gst_pr_sel: str, date_pr_sel: str, cgst_pr_sel: str, sgst_pr_sel: str, igst_pr_sel: str) -> bytes:
    # Reload sheets
    wanted_sheets = ["B2B", "B2B-CDNR"]
    xls_2b = pd.read_excel(tmp2b_path, sheet_name=wanted_sheets, header=[4, 5])
    frames = []
    for _, df in xls_2b.items():
        df = df.copy()
        df.columns = flatten_columns(df.columns)
        df = df.dropna(how="all")
        if not df.empty:
            df, _ = normalize_columns(df)  # normalize 2B headers
            frames.append(df)
    df_2b_raw = pd.concat(frames, ignore_index=True)

    df_pr_raw = pd.read_excel(tmppr_path)
    df_pr_raw, pr_norm_map = normalize_columns(df_pr_raw)  # normalize PR headers, keep map

    # ---- Robust fallback: normalized selection must match ANY header ----
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

    inv_2b = ensure_col(df_2b_raw, inv_2b_sel, INVOICE_CANDIDATES_2B)
    gst_2b = ensure_col(df_2b_raw, gst_2b_sel, GSTIN_CANDIDATES_2B)
    date_2b = ensure_col(df_2b_raw, date_2b_sel, DATE_CANDIDATES_2B)
    cgst_2b = ensure_col(df_2b_raw, cgst_2b_sel, CGST_CANDIDATES_2B)
    sgst_2b = ensure_col(df_2b_raw, sgst_2b_sel, SGST_CANDIDATES_2B)
    igst_2b = ensure_col(df_2b_raw, igst_2b_sel, IGST_CANDIDATES_2B)

    gst_pr = ensure_col(df_pr_raw, gst_pr_sel, GSTIN_CANDIDATES_PR,
                        penalties=AVOID_RECIPIENT_GSTIN_FOR_PR,
                        value_picker=lambda d: pick_gstin_by_values(d, prefer_supplier=True))
    inv_pr = ensure_col(df_pr_raw, inv_pr_sel, INVOICE_CANDIDATES_PR,
                        avoid=AVOID_DOC_LIKE_FOR_PR,
                        penalties=["company", "recipient", "gstin"],
                        value_picker=pick_invoice_by_values)
    date_pr = ensure_col(df_pr_raw, date_pr_sel, DATE_CANDIDATES_PR)
    cgst_pr = ensure_col(df_pr_raw, cgst_pr_sel, CGST_CANDIDATES_PR)
    sgst_pr = ensure_col(df_pr_raw, sgst_pr_sel, SGST_CANDIDATES_PR)
    igst_pr = ensure_col(df_pr_raw, igst_pr_sel, IGST_CANDIDATES_PR)

    optional_numeric_2b = []
    for pool in [[ "taxable value","taxable amount","assessable value","taxable" ],
                 [ "total tax","total tax amount","tax amount" ],
                 [ "invoice value","total invoice value","value of invoice","invoice total" ],
                 [ "cess","cess amount" ]]:
        col = _find_optional_col(df_2b_raw, [pool]);  optional_numeric_2b.append(col) if col else None
    for must in [cgst_2b, sgst_2b, igst_2b]:
        if must and must not in optional_numeric_2b: optional_numeric_2b.append(must)

    optional_numeric_pr = []
    for pool in [[ "taxable value","taxable amount","assessable value","taxable" ],
                 [ "total tax","total tax amount","tax amount" ],
                 [ "invoice value","total invoice value","value of invoice","invoice total" ],
                 [ "cess","cess amount" ]]:
        col = _find_optional_col(df_pr_raw, [pool]);  optional_numeric_pr.append(col) if col else None
    for must in [cgst_pr, sgst_pr, igst_pr]:
        if must and must not in optional_numeric_pr: optional_numeric_pr.append(must)

    # Consolidated by key for recon
    df_2b = consolidate_by_key(df=df_2b_raw, gstin_col=gst_2b, inv_col=inv_2b, date_col=date_2b, numeric_cols=optional_numeric_2b)
    df_pr = consolidate_by_key(df=df_pr_raw, gstin_col=gst_pr, inv_col=inv_pr, date_col=date_pr, numeric_cols=optional_numeric_pr)

    combined_df, pair_cols = build_pairwise_recon(
        df_pr=df_pr, df_2b=df_2b,
        inv_pr=inv_pr, gst_pr=gst_pr, date_pr=date_pr, cgst_pr=cgst_pr, sgst_pr=sgst_pr, igst_pr=igst_pr,
        inv_2b=inv_2b, gst_2b=gst_2b, date_2b=date_2b, cgst_2b=cgst_2b, sgst_2b=sgst_2b, igst_2b=igst_2b
    )

    # ---------------- NEW: PR - Comments sheet data ----------------
    recon_lookup = {}
    for _, row in combined_df.iterrows():
        key = (as_text(row.get("_GST_KEY", "")), as_text(row.get("_INV_KEY", "")))
        recon_lookup[key] = (row.get("Mapping", ""), row.get("Remarks", ""), row.get("Reason", ""))

    pr_comments = df_pr_raw.copy()
    if gst_pr in pr_comments.columns and inv_pr in pr_comments.columns:
        pr_comments["_GST_KEY"] = pr_comments[gst_pr].map(clean_gstin)
        pr_comments["_INV_KEY"] = pr_comments[inv_pr].map(inv_basic)
        pr_comments["Mapping"] = pr_comments.apply(lambda r: recon_lookup.get((r["_GST_KEY"], r["_INV_KEY"]), ("", "", ""))[0], axis=1)
        pr_comments["Remarks"] = pr_comments.apply(lambda r: recon_lookup.get((r["_GST_KEY"], r["_INV_KEY"]), ("", "", ""))[1], axis=1)
        pr_comments["Reason"] = pr_comments.apply(lambda r: recon_lookup.get((r["_GST_KEY"], r["_INV_KEY"]), ("", "", ""))[2], axis=1)
        pr_comments = pr_comments[[c for c in df_pr_raw.columns] + ["Mapping", "Remarks", "Reason"]]
    else:
        pr_comments["Mapping"] = ""
        pr_comments["Remarks"] = ""
        pr_comments["Reason"] = ""

    # ---------------- Prepare Dashboard (transposed) ----------------
    dashboard_df = build_dashboard(combined_df, pair_cols)
    dashboard_tx = dashboard_df.set_index("Metric").T.reset_index()
    dashboard_tx.rename(columns={"index": "Status"}, inplace=True)

    # ---------------- Add extra columns to Reconciliation ----------------
    gstr1_col = pair_cols.get("gstr1_status_2b_col", None)
    two_b_month_series = combined_df[gstr1_col] if gstr1_col and gstr1_col in combined_df.columns else ""
    combined_df["2B month"] = two_b_month_series
    combined_df["Eligibility"] = ""
    combined_df["User Remarks"] = ""

    # --------- FAST XLSX WRITER (XlsxWriter) ---------

    def _autosize_ws(ws, df, min_w=12, max_w=48):
        # Sample first 200 rows for width estimate to avoid full column scans
        sample = df.head(200)
        for col_idx, col_name in enumerate(df.columns):
            header_len = len(str(col_name)) + 4
            try:
                content_len = int(sample[col_name].astype(str).map(len).max())
                if math.isnan(content_len):
                    content_len = 0
            except Exception:
                content_len = 0
            width = max(min_w, min(max_w, max(header_len, content_len + 2)))
            ws.set_column(col_idx, col_idx, width)

    output = io.BytesIO()
    with pd.ExcelWriter(
        output,
        engine="xlsxwriter",
        engine_kwargs={"options": {"constant_memory": True, "strings_to_urls": False}},
    ) as writer:
        wb = writer.book
        header_fmt = wb.add_format({"bold": True})

        # ---------------- Reconciliation ----------------
        combined_df.to_excel(writer, index=False, sheet_name="Reconciliation")
        ws = writer.sheets["Reconciliation"]
        ws.freeze_panes(1, 0)
        ws.autofilter(0, 0, len(combined_df), max(0, combined_df.shape[1] - 1))
        ws.set_row(0, None, header_fmt)
        _autosize_ws(ws, combined_df)

        # ---------------- Dashboard ----------------
        dashboard_tx.to_excel(writer, index=False, sheet_name="Dashboard")
        ws2 = writer.sheets["Dashboard"]
        ws2.freeze_panes(1, 0)
        ws2.autofilter(0, 0, len(dashboard_tx), max(0, dashboard_tx.shape[1] - 1))
        ws2.set_row(0, None, header_fmt)
        _autosize_ws(ws2, dashboard_tx, min_w=14)

        # ---------------- PR - Comments ----------------
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
    """
    Faster version:
      • Parallel downloads (2B + PR)
      • Clear progress updates
      • Timing logs in worker output
    """
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

        # ---- Download both files concurrently (overlap network I/O) ----
        _mark(5, "Downloading inputs from Drive")
        def _dl(fid, path):
            download_from_drive(fid, path)
            return path

        with ThreadPoolExecutor(max_workers=2) as ex:
            fut2b = ex.submit(_dl, drive_id_2b, in2b)
            futpr = ex.submit(_dl, drive_id_pr, inpr)
            # Wait for both
            fut2b.result()
            futpr.result()

        _mark(22, f"Downloads finished in {time.time()-t0:.1f}s")

        # ---- Reconcile + build XLSX (XlsxWriter used inside pipeline) ----
        _mark(30, "Reconciling")
        x = selections
        blob = _run_reconciliation_pipeline(
            in2b, inpr,
            x.get("inv_2b",""), x.get("gst_2b",""), x.get("date_2b",""), x.get("cgst_2b",""), x.get("sgst_2b",""), x.get("igst_2b",""),
            x.get("inv_pr",""), x.get("gst_pr",""), x.get("date_pr",""), x.get("cgst_pr",""), x.get("sgst_pr",""), x.get("igst_pr","")
        )
        _mark(82, f"Reconcile+write took {time.time()-t0:.1f}s")

        # ---- Upload result to Drive (keep behavior) ----
        _mark(85, "Uploading result to Drive")
        out_path = os.path.join(td, f"{user_id}_gstr2b_pr_reconciliation.xlsx")
        with open(out_path, "wb") as f:
            f.write(blob)

        result_id = upload_to_drive(out_path, os.path.basename(out_path))
        _mark(100, f"Done in {time.time()-t0:.1f}s")

        # Temp files are auto-removed with the TemporaryDirectory

    return {"result_drive_id": result_id}

# -------------------- QUEUED confirm route + status + download --------------------

@app.route("/reconcile_confirm", methods=["POST"])
def reconcile_confirm():
    # Ensure temp paths from prior /verify
    tmp2b = session.get("tmp2b"); tmppr = session.get("tmppr")
    if not tmp2b or not tmppr or (not os.path.exists(tmp2b)) or (not os.path.exists(tmppr)):
        flash("Upload session expired. Please re-upload the files.")
        return redirect(url_for("index"))

    # Read user selections
    sel = {
        "inv_2b": (request.form.get("inv_2b") or "").strip(),
        "gst_2b": (request.form.get("gst_2b") or "").strip(),
        "date_2b": (request.form.get("date_2b") or "").strip(),
        "cgst_2b": (request.form.get("cgst_2b") or "").strip(),
        "sgst_2b": (request.form.get("sgst_2b") or "").strip(),
        "igst_2b": (request.form.get("igst_2b") or "").strip(),
        "inv_pr": (request.form.get("inv_pr") or "").strip(),
        "gst_pr": (request.form.get("gst_pr") or "").strip(),
        "date_pr": (request.form.get("date_pr") or "").strip(),
        "cgst_pr": (request.form.get("cgst_pr") or "").strip(),
        "sgst_pr": (request.form.get("sgst_pr") or "").strip(),
        "igst_pr": (request.form.get("igst_pr") or "").strip(),
    }

    # Upload both temp files to Google Drive and enqueue job
    try:
        drive_id_2b = upload_to_drive(tmp2b, "gstr2b.xlsx")
        drive_id_pr = upload_to_drive(tmppr, "purchase_register.xlsx")
    finally:
        # Clean local temp & session
        try:
            os.remove(tmp2b); os.remove(tmppr)
        except Exception:
            pass
        session.pop("tmp2b", None); session.pop("tmppr", None)

    user_id = request.form.get("user_id", "anon")

    job = q.enqueue(
        "main.process_reconcile",  # module.function path
        drive_id_2b, drive_id_pr, sel, user_id,
        job_timeout=900, result_ttl=86400, failure_ttl=86400
    )

    # Return a tiny progress page that polls /status and triggers /download when ready
    return render_template("progress.html", job_id=job.id)

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

@app.route("/download/<job_id>", methods=["GET"])
def download(job_id):
    job = Job.fetch(job_id, connection=rconn)
    if job.get_status() != "finished":
        return jsonify({"error": "Job not finished"}), 409
    drive_id = job.result.get("result_drive_id")
    if not drive_id:
        return jsonify({"error": "No result id"}), 404

    # Stream the Drive file to the client (no public sharing required)
    with tempfile.TemporaryDirectory() as td:
        path = os.path.join(td, "gstr2b_pr_reconciliation.xlsx")
        download_from_drive(drive_id, path)
        return send_file(
            path,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            as_attachment=True,
            download_name="gstr2b_pr_reconciliation.xlsx"
        )

if __name__ == "__main__":
    app.run(debug=True)
