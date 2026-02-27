# C:\BusyWin\AI BOT\BUSY_RECEIPT_BOT_STRUCTURE\final.py
from flask import Flask, request, jsonify, send_from_directory, abort, g
from flask_cors import CORS
import os, re, datetime, json, pandas as pd, subprocess, requests, pyodbc, threading, sys, hashlib, shutil
from PIL import Image
from flask import send_file
from PyPDF2 import PdfMerger
from apscheduler.schedulers.background import BackgroundScheduler
from deep_translator import GoogleTranslator

# ===== WhatsApp sender imports =====
import time, uuid, logging, queue
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv
from werkzeug.utils import secure_filename
# at the top with other imports
try:
    from deep_translator import GoogleTranslator
except ImportError:
    GoogleTranslator = None
IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
from concurrent.futures import ThreadPoolExecutor
UPLOAD_EXEC = ThreadPoolExecutor(max_workers=40)

# -------------------------------------------------------
# Load .env (WhatsApp API keys, logging switches, etc.)
# -------------------------------------------------------
load_dotenv()

# -------------------------------------------------------
# Flask & CORS
# -------------------------------------------------------
app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": [
    "http://192.168.2.37",
    "http://192.168.193.172",
    "http://localhost:5000",
    "http://127.0.0.1:5500",
    "http://yourdomain.com"
]}})
import os, re
from flask import request, jsonify, send_from_directory, abort
from werkzeug.utils import secure_filename
# =========================
# BCN MEDIA FOLDERS (FIX)
# =========================
BCN_MEDIA_DIR  = r"C:\BusyWin\IMAGES\BCN"          # where full images/videos are saved
BCN_THUMBS_DIR = r"C:\BusyWin\IMAGES\BCN\_thumbs" # thumbs folder

# Backward compatibility (your old code uses BCN_DIR)
BCN_DIR = BCN_MEDIA_DIR

os.makedirs(BCN_MEDIA_DIR, exist_ok=True)
os.makedirs(BCN_THUMBS_DIR, exist_ok=True)

os.makedirs(BCN_MEDIA_DIR, exist_ok=True)
os.makedirs(BCN_THUMBS_DIR, exist_ok=True)

ALLOWED_EXT = {".jpg", ".jpeg", ".png", ".webp", ".mp4", ".mov", ".avi", ".mkv", ".pdf"}  # adjust if needed
import re

def _clean_barcode(s: str) -> str:
    """
    Keep only safe characters for filenames and searching.
    Allows letters, digits, underscore, dash.
    """
    s = (s or "").strip()
    s = re.sub(r"[^A-Za-z0-9_-]", "", s)
    return s

from PIL import Image
import os, re
from flask import request, jsonify, abort, send_from_directory
from werkzeug.utils import secure_filename

def _media_type_from_ext(ext: str) -> str:
    ext = (ext or "").lower()
    if ext in [".jpg", ".jpeg", ".png", ".webp"]:
        return "image"
    if ext in [".mp4", ".mov", ".avi", ".mkv"]:
        return "video"
    if ext == ".pdf":
        return "pdf"
    return "file"

def _thumb_name(original_filename: str) -> str:
    base, _ = os.path.splitext(original_filename)
    return f"{base}.jpg"   # thumbs always jpg
def _file_url(fname: str) -> str:
    return request.host_url.rstrip("/") + "/bcn_image/" + fname

def _thumb_url(fname: str) -> str:
    return request.host_url.rstrip("/") + "/bcn_thumb/" + fname

def _ensure_image_thumb(original_path: str, original_filename: str) -> str | None:
    """
    Creates thumbnail in BCN_THUMBS_DIR and returns thumb filename.
    Only for image files.
    """
    try:
        thumb_fn = _thumb_name(original_filename)
        thumb_path = os.path.join(BCN_THUMBS_DIR, thumb_fn)

        # If thumb already exists and is newer than original, reuse it
        if os.path.exists(thumb_path):
            if os.path.getmtime(thumb_path) >= os.path.getmtime(original_path):
                return thumb_fn

        with Image.open(original_path) as im:
            # Convert to RGB for JPG
            if im.mode in ("RGBA", "P"):
                im = im.convert("RGB")

            # Make a small thumbnail (card size)
            im.thumbnail((480, 480))  # you can change size like (360, 360)
            im.save(thumb_path, "JPEG", quality=75, optimize=True)

        return thumb_fn
    except Exception as e:
        print("[thumb] failed:", original_filename, e)
        return None

def _compress_image_inplace(path: str):
    """
    Reduce uploaded image size (max 1600px) + jpg quality.
    This keeps file smaller and faster on mobile.
    """
    try:
        with Image.open(path) as im:
            # normalize orientation if available
            try:
                exif = im.getexif()
                orientation = exif.get(274)
                if orientation == 3:
                    im = im.rotate(180, expand=True)
                elif orientation == 6:
                    im = im.rotate(270, expand=True)
                elif orientation == 8:
                    im = im.rotate(90, expand=True)
            except Exception:
                pass

            if im.mode in ("RGBA", "P"):
                im = im.convert("RGB")

            # resize to max 1600px
            max_side = 1600
            w, h = im.size
            scale = max(w, h) / float(max_side)
            if scale > 1:
                im = im.resize((int(w/scale), int(h/scale)))

            # save as JPG (even if png) to reduce size
            # if you want to keep original extension, tell me; JPG is best for size.
            im.save(path, "JPEG", quality=80, optimize=True)
    except Exception as e:
        print("[compress] skip:", path, e)

def _next_available_filename(barcode: str, ext: str) -> str:
    """
    BARCODE.ext
    BARCODE_1.ext
    BARCODE_2.ext ...
    """
    base = barcode
    candidate = f"{base}{ext}"
    if not os.path.exists(os.path.join(BCN_MEDIA_DIR, candidate)):
        return candidate

    i = 1
    while True:
        candidate = f"{base}_{i}{ext}"
        if not os.path.exists(os.path.join(BCN_MEDIA_DIR, candidate)):
            return candidate
        i += 1

# -------------------------------------------------------
# Paths & files (existing app)
# -------------------------------------------------------
QUEUE_DIR = r"C:\\BusyWin\\AI BOT\\BUSY_RECEIPT_BOT_STRUCTURE\\queue"
EXCEL_FILE = r"C:\\BusyWin\\AI BOT\\BUSY_RECEIPT_BOT_STRUCTURE\\customer_dues.xlsx"
SALESMAN_FILE = r"C:\\BusyWin\\AI BOT\\BUSY_RECEIPT_BOT_STRUCTURE\\salesman_list.xlsx"
STOCK_FILE = r"C:\\BusyWin\\AI BOT\\BUSY_RECEIPT_BOT_STRUCTURE\\SSH_BCNwiseStockDetails.xlsx"
AHK_SCRIPT_PATH = r"C:\\Path\\To\\Your\\FinalBot.ahk"  # UPDATE your AHK path
FACES_FOLDER = r"C:\BusyWin\AI BOT\BUSY_RECEIPT_BOT_STRUCTURE\faces"
STAFF_FILE = r"C:\BusyWin\AI BOT\BUSY_RECEIPT_BOT_STRUCTURE\staff.xlsx"
ATTENDANCE_FILE = r"C:\BusyWin\AI BOT\BUSY_RECEIPT_BOT_STRUCTURE\attendance_log.xlsx"
# put this near your other globals
PTS_EXPIRY_VIDEO = r"C:\BusyWin\IMAGES\CUP\ptsexpiry.mp4"

# Fixed local VIDEO files for specific coupon templates
# (change the paths if your file names are different)
COUPON_VIDEO_MAP = {
    "2sshnncup":   r"C:\BusyWin\IMAGES\CUP\2sshnncup.mp4",
    "1sshmaincup": r"C:\BusyWin\IMAGES\CUP\1sshmaincup.mp4",
    "3sshncup":    r"C:\BusyWin\IMAGES\CUP\3sshncup.mp4",
    "groupcup":    r"C:\BusyWin\IMAGES\CUP\groupcup.mp4",
}
THUMBS_DIR = r"C:\BusyWin\IMAGES\BCN\_thumbs"
ALLOWED_IMG_EXT = (".jpg", ".jpeg", ".png", ".webp")
def _find_exact_thumb_filename(barcode: str) -> str:
    """
    Returns exact filename like '801801.jpg' ONLY if it exists in _thumbs.
    No partial match.
    """
    b = _clean_barcode(barcode)
    for ext in ALLOWED_IMG_EXT:
        fn = b + ext
        if os.path.exists(os.path.join(THUMBS_DIR, fn)):
            return fn
    return ""
@app.get("/media/bcn_thumbs/<path:filename>")
def media_bcn_thumbs(filename):
    return send_from_directory(THUMBS_DIR, filename, as_attachment=False)

import re  # keep only once at the top of the file
# 👉 Salesman/Helper commission PDF engine
from salesman_commission_report import fetch_salesman_rows, generate_pdf
from daybook import generate_daybook
from brand_sales_register_pdf import generate_brand_sales_pdf
from brand_sales_register_pdf import build_brand_sale_pdf
# ==============================
# CREDITORS PURCHASE ANALYSIS - API + SEND TO GROUP (QUEUE: LOCAL PDF)
# ✅ Uses SAME working flow as Daybook/Brand Sale:
#    enqueue_send_job(pdf_path=local_pdf)
#    BSP tries uploadMedia -> if fails -> FALLBACK sends
# ==============================

from flask import request, jsonify
import os

import party_purchase_bcn_stock_pdf as pp

CREDITORS_GROUP_ID = "120363406067995796@g.us"
CREDITOR_TEMPLATE_NAME = "creditor_purchase_pdf"   # must exist in templates_registry


def _pick(v, default=""):
    v = "" if v is None else str(v).strip()
    return v if v else default


def send_creditor_purchase_analysis_on_whatsapp(party: str, from_date: str, to_date: str, group_id: str) -> dict:
    try:
        # 1) Generate PDF (LOCAL)
        pdf_path = pp.build_party_purchase_bcn_stock_pdf(
            party=party,
            from_date=from_date,
            to_date=to_date,
            media_root=pp.MEDIA_ROOT,
            out_dir=pp.OUTPUT_FOLDER,
            open_pdf=False,
        )

        if not pdf_path or not os.path.exists(pdf_path):
            app.logger.error("[creditor] ❌ PDF not generated: %s", pdf_path)
            return {"ok": False, "error": "pdf_not_generated", "pdf_path": pdf_path}

        # 2) receiver (IMPORTANT: group id should NOT go through normalize_mobile)
        to_id = resolve_receiver(group_id) or group_id

        # 3) template + language
        tpl = (CREDITOR_TEMPLATE_NAME or "").strip().lower()
        tinfo = _template_registry.get(tpl, {})
        language_code = (tinfo.get("language") or "en").strip()

        # 4) caption (vars=1)
        caption = (
            "SUBHASH SAREE HOUSE CREDITORS PURCHASE ANALYSIS\n"
            f"Party: {party}\n"
            f"From: {from_date}  To: {to_date}"
        )
        values = [caption]

        # 5) Queue job (LOCAL PDF) ✅ SAME AS daybook/brand-sale
        job_id = enqueue_send_job(
            to=to_id,
            template_name=tpl,
            language_code=language_code,
            values=values,
            pdf_link=None,
            image_link=None,
            video_link=None,
            pdf_path=pdf_path,       # ✅ local pdf ONLY
            raw_text=caption,
            delay_seconds=0
        )

        app.logger.info("[creditor] ✅ queued job_id=%s to=%s pdf=%s", job_id, to_id, pdf_path)
        return {"ok": True, "job_id": job_id, "pdf": os.path.basename(pdf_path)}

    except Exception as e:
        app.logger.exception("[creditor] ❌ ERROR: %s", e)
        return {"ok": False, "error": str(e)}


@app.route("/api/creditor_purchase_analysis", methods=["GET", "POST"])
def api_creditor_purchase_analysis():
    try:
        payload = request.get_json(silent=True) or {} if request.method == "POST" else {}

        party = _pick(payload.get("party"), _pick(request.args.get("party"), pp.DEFAULT_PARTY_NAME))
        from_date = _pick(payload.get("from"), _pick(request.args.get("from"), pp.DEFAULT_FROM_DATE))
        to_date = _pick(payload.get("to"), _pick(request.args.get("to"), pp.DEFAULT_TO_DATE))
        group_id = _pick(payload.get("group_id"), _pick(request.args.get("group_id"), CREDITORS_GROUP_ID))

        send_res = send_creditor_purchase_analysis_on_whatsapp(party, from_date, to_date, group_id)

        return jsonify({
            "ok": True,
            "party": party,
            "from": from_date,
            "to": to_date,
            "group_id": group_id,
            "send_result": send_res,
        })

    except Exception as e:
        app.logger.exception("[creditor] api_creditor_purchase_analysis failed")
        return jsonify({"ok": False, "error": str(e)}), 500
# ===========================
# PARTY SEARCH API (for APK dropdown)
# GET /api/party_search?q=fashion
# ===========================

import pyodbc
from flask import request, jsonify

PARTY_SEARCH_SQL = r"""
DECLARE @q NVARCHAR(200) = ?;

SELECT TOP (30)
    Code,
    Name,
    PrintName = ISNULL(NULLIF(LTRIM(RTRIM(PrintName)), ''), Name)
FROM Master1
WHERE
    (
        Name LIKE '%' + @q + '%'
        OR PrintName LIKE '%' + @q + '%'
    )
ORDER BY
    CASE
        WHEN Name LIKE @q + '%'
          OR PrintName LIKE @q + '%'
        THEN 0
        ELSE 1
    END,
    Name;
"""

@app.get("/api/party_search")
def api_party_search():
    q = (request.args.get("q") or "").strip()

    # small guard: avoid returning junk when user typed nothing
    if not q or len(q) < 2:
        return jsonify({"ok": True, "q": q, "items": []})

    try:
        conn = pyodbc.connect(SQL_CONN_STR)
        try:
            cur = conn.cursor()
            cur.execute(PARTY_SEARCH_SQL, (q,))
            rows = cur.fetchall()

            items = []
            for r in rows:
                # r = (Code, Name, PrintName)
                items.append({
                    "code": int(r[0]) if r[0] is not None else None,
                    "name": (r[1] or "").strip(),
                    "printName": (r[2] or "").strip(),
                })

            return jsonify({"ok": True, "q": q, "count": len(items), "items": items})
        finally:
            conn.close()

    except Exception as e:
        app.logger.exception("[party_search] error")
        return jsonify({"ok": False, "error": str(e)}), 500

def normalize_name_with_ji(name: str) -> str:
    """
    Ensures the name ends with exactly ONE 'JI'.
    """
    if not name:
        return ""

    s = str(name).strip()
    s = re.sub(r'(?:\bji\b\s*)+$', '', s, flags=re.IGNORECASE).strip()

    if not s:
        return "JI"

    return f"{s} JI"


def ensure_single_ji(name: str) -> str:
    """
    Backward-compatible helper — older code calls ensure_single_ji(),
    internally we use normalize_name_with_ji().
    """
    return normalize_name_with_ji(name)

def is_coupon_redeemed(mobile_no: str, coupon_code: str) -> bool:
    """
    Return True if this coupon has already been redeemed for this mobile number
    based on dbo.TmpRedeemCupponCode (MobileNo, CupponCode).
    """
    try:
        if not mobile_no or not coupon_code:
            return False

        sql = """
        SELECT TOP 1 1
        FROM dbo.TmpRedeemCupponCode
        WHERE LTRIM(RTRIM(MobileNo)) = ?
          AND LTRIM(RTRIM(CONVERT(varchar(50), CupponCode))) = ?
        """

        with pyodbc.connect(SQL_CONN_STR) as conn:
            cur = conn.cursor()
            cur.execute(sql, (mobile_no.strip(), coupon_code.strip()))
            row = cur.fetchone()
            return row is not None
    except Exception as e:
        app.logger.exception(
            f"[coupon] redeem-check failed for {mobile_no}/{coupon_code}: {e}"
        )
        # If check fails, better to treat as NOT redeemed than silently skip
        return False


os.makedirs(QUEUE_DIR, exist_ok=True)
os.makedirs(FACES_FOLDER, exist_ok=True)

ALLOWED_EXTENSIONS = {'pdf', 'jpg', 'jpeg'}
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Main upload folder for voucher documents
UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploaded_documents")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Sub-folders for classification
PROCESSED_DOC_DIR = os.path.join(UPLOAD_FOLDER, "processed")
UNPROCESSED_DOC_DIR = os.path.join(UPLOAD_FOLDER, "unprocessed")
os.makedirs(PROCESSED_DOC_DIR, exist_ok=True)
os.makedirs(UNPROCESSED_DOC_DIR, exist_ok=True)

app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
import re

def normalize_receiver_id(raw: str) -> str | None:
    """
    Accepts:
      - mobile numbers (10 digits / 91xxxxxxxxxx / +91xxxxxxxxxx) => returns 91xxxxxxxxxx
      - group ids (numeric > 12 digits OR already contains @g.us) => returns ...@g.us
    """
    if not raw:
        return None

    s = str(raw).strip()

    # If already a WhatsApp group JID
    if s.endswith("@g.us"):
        return s

    # Keep only digits for length checks (handles spaces, +, etc.)
    digits = re.sub(r"\D", "", s)

    # ✅ Treat long numeric as group id
    if len(digits) > 12:
        return digits + "@g.us"

    # ✅ Otherwise treat as normal mobile
    # (your existing normalize_mobile should return like 91xxxxxxxxxx)
    return normalize_mobile(s)

# -------------------------------------------------------
# SQL Server connection for your existing endpoints
# -------------------------------------------------------
SQL_CONN_STR = (
    "DRIVER={SQL Server};"
    "SERVER=localhost\\SQL2022;"
    "DATABASE=BusyComp0001_db12025;"
    "UID=SA;"
    "PWD=busy123"
)
# -------------------------------------------------------
# OTP AUTH DB (tmpDurgaOTPConfig is inside BusyComp0001_db)
# -------------------------------------------------------
SQL_CONN_STR_AUTH = (
    "DRIVER={SQL Server};"
    "SERVER=localhost\\SQL2022;"
    "DATABASE=BusyComp0001_db;"
    "UID=SA;"
    "PWD=busy123"
)

def auth_db_conn_factory():
    return pyodbc.connect(SQL_CONN_STR_AUTH)

app.config["AUTH_DB_CONN_FACTORY"] = auth_db_conn_factory

# OTP tuning
app.config["OTP_EXPIRES_SEC"] = 300            # 5 min
app.config["AUTH_TOKEN_TTL_SEC"] = 24 * 3600   # 24 hours
app.config["OTP_DEBUG_RETURN_OTP"] = "0"       # set "1" only for testing
def fetch_barcode_party_summary_sql(barcode: str) -> dict:
    """
    Returns:
      {
        "purchase_party_alias": "POOJA SAREE HOUSE",
        "sold_parties": ["PARWATI PAIKRA JI 9174...", "PREETI RAJWADE JI 9139..."],
        "total_purchase_qty": 2,
        "total_sold_qty": 2
      }

    NOTE:
    - You MUST adjust table/column names below if your schema differs.
    - This is written to match Busy "Query on Barcode No." movement style.
    """
    out = {
        "purchase_party_alias": "",
        "sold_parties": [],
        "total_purchase_qty": 0,
        "total_sold_qty": 0
    }

    try:
        with pyodbc.connect(SQL_CONN_STR) as conn:
            cur = conn.cursor()

            # ✅ IMPORTANT:
            # Replace dbo.BarcodeQuery with your actual movement table/view if different.
            # Many Busy DBs store this via Tran2 + ItemDesc joins; if you already have a view, use it here.
            sql = """
            SELECT
                VchType,
                Particulars,
                ISNULL(TRY_CONVERT(float, [Qty In]), 0)  AS QtyIn,
                ISNULL(TRY_CONVERT(float, [Qty Out]), 0) AS QtyOut
            FROM dbo.BarcodeQuery
            WHERE LTRIM(RTRIM(Barcode)) = ?
            ORDER BY [Date] ASC
            """

            rows = cur.execute(sql, barcode).fetchall()

            sold = []
            total_in = 0.0
            total_out = 0.0
            purchase_party = ""

            for r in rows:
                vtype = (str(r[0]) if r[0] is not None else "").strip().lower()
                party = (str(r[1]) if r[1] is not None else "").strip()
                qin = float(r[2] or 0)
                qout = float(r[3] or 0)

                if "pur" in vtype:  # Purc
                    total_in += qin
                    if not purchase_party and party:
                        purchase_party = party

                if "sal" in vtype:  # Sale
                    total_out += qout
                    if party:
                        sold.append(party)

            # unique keep order
            seen = set()
            sold_unique = []
            for s in sold:
                if s not in seen:
                    seen.add(s)
                    sold_unique.append(s)

            out["purchase_party_alias"] = purchase_party
            out["sold_parties"] = sold_unique
            out["total_purchase_qty"] = int(total_in) if abs(total_in - int(total_in)) < 1e-9 else total_in
            out["total_sold_qty"] = int(total_out) if abs(total_out - int(total_out)) < 1e-9 else total_out

            return out

    except Exception:
        # If your DB doesn't have dbo.BarcodeQuery, out remains blank (Android will show "-"/0)
        return out


# -------------------------------------------------------
# Google Sheet (existing app)
# -------------------------------------------------------
GOOGLE_SHEET_ID = "14re5UoHFvRO20raMjw2IKZA31RsA224u0NWkIefPO04"
SHEET_URL = f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/export?format=xlsx"
TEMP_EXCEL = os.path.join(QUEUE_DIR, "form_data.xlsx")

# -------------------------------------------------------
# Balance Confirmation config (existing app)
# -------------------------------------------------------
BALCONF_BRANCHES = {
    "MAIN": ["MAIN"],
    "SSHN": ["SSHN"],
    "SSHR": ["SSHR"],
}
THUMB_MAX = 320       # thumbnail size max width/height
THUMB_QUALITY = 70    # thumbnail jpg quality

# -------------------------------------------------------
# WhatsApp sender ENV + constants
# -------------------------------------------------------
API_URL = os.getenv("WHATSAPP_API_URL", "").strip()
API_KEY = os.getenv("WHATSAPP_KEY", "").strip()
WABA    = os.getenv("WHATSAPP_WABA", "").strip()

KEY_HEADER_NAME = (os.getenv("WHATSAPP_KEY_HEADER", "Key") or "Key").strip()
AUTH_BEARER     = (os.getenv("WHATSAPP_AUTH_BEARER", "0").strip().lower() in ("1","true","yes"))

# Base + Logging directories
BASE_DIR = os.path.dirname(__file__)
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# Invoices / PDF directory
PDF_DIR = os.getenv(
    "PDF_DIR",
    r"C:\BusyWin\AI BOT\BUSY_RECEIPT_BOT_STRUCTURE\invoices"
).strip()
os.makedirs(PDF_DIR, exist_ok=True)

PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "http://127.0.0.1:5000").strip()

# ---- Coupon expiry reminder folders & media ----
COUPON_DIR       = os.path.join(BASE_DIR, "coupon")
COUPON_GEN_DIR   = os.path.join(COUPON_DIR, "generated")
COUPON_PROC_DIR  = os.path.join(COUPON_DIR, "processed")
os.makedirs(COUPON_GEN_DIR, exist_ok=True)
os.makedirs(COUPON_PROC_DIR, exist_ok=True)
import pyodbc

# ---------- Influencer / Model form constants ----------
INFLUENCER_SHEET_URL = (
    "https://docs.google.com/spreadsheets/d/"
    "1I-aSk_wzLuHqqzHXeXZhcU6DjuGyRZdzRj0UmNRbUG8/edit?usp=sharing"
)

INFLUENCER_TEMP_EXCEL = os.path.join(BASE_DIR, "influencer_form_latest.xlsx")

# 🔥 This now works correctly because LOG_DIR is defined above
INFLUENCER_PROCESSED_LOG = os.path.join(LOG_DIR, "influencer_form_processed.txt")

# Admin number to receive the alert
INFLUENCER_ALERT_MSISDN = "120363421302677422@g.us"

# Template name from templates_registry.xlsx
INFLUENCER_TEMPLATE_NAME = "influencer_model"

# Coupon expiry video (used elsewhere)
EXPIRY_COUPON_VIDEO = r"C:\BusyWin\IMAGES\expiry coupon.mp4"

TEMPLATE_DEFAULT = os.getenv("TEMPLATE_DEFAULT", "invoice1").strip()
DRY_RUN = os.getenv("DRY_RUN", "0").strip().lower() in ("1", "true", "yes")
BUSY_TOKEN = os.getenv("BUSY_TOKEN", "").strip()

S3_ENABLE = os.getenv("S3_ENABLE", "0").strip().lower() in ("1","true","yes")  # unused

NUM_WORKERS = int(os.getenv("NUM_WORKERS", "50"))
RETRY_MAX = int(os.getenv("RETRY_MAX", "1"))
RETRY_MAX_SLEEP = int(os.getenv("RETRY_MAX_SLEEP", "60"))

REGISTRY_XLSX = os.path.join(BASE_DIR, "templates_registry.xlsx")

# ===== Missing mobiles capture (Excel you can fill) =====
MISSING_MOBILES_FILE = os.path.join(BASE_DIR, "uploadsalesmanmobileno.xlsx")
MISSING_MOBILES_COLUMNS = ["role", "name", "mobile", "source", "last_seen"]

# ---- Google Sheet webhook (Apps Script) ----
GS_WEBHOOK = os.getenv("GS_WEBHOOK", "").strip()
GS_TOKEN   = os.getenv("GS_TOKEN", "").strip()

def _register_to_sheet(wamid, to, text, mediaurl=None, local_url=None):
    """
    Tell the Apps Script 'register' endpoint that we sent a message.
    This creates a row in the Google Sheet so the Google webhook can match statuses
    and (if needed) trigger fallback.
    """
    try:
        if not GS_WEBHOOK or not GS_TOKEN:
            return
        import urllib.parse as up
        params = {
            "mode": "register",
            "token": GS_TOKEN,
            "wamid": wamid or "",
            "to": to or "",
            "text": text or "",
            "mediaurl": mediaurl or "",
            "local_url": local_url or ""
        }
        url = GS_WEBHOOK.rstrip("/") + "?" + up.urlencode(params, safe=":/")
        r = requests.get(url, timeout=20)
        app.logger.info(f"[GS] register→ {r.status_code}")
    except Exception:
        app.logger.exception("[GS] register failed")

# -------------------------------------------------------
# Logging (clean & concise)
# -------------------------------------------------------
def _env_bool(name: str, default: bool) -> bool:
    return (os.getenv(name, "1" if default else "0").strip().lower() in ("1","true","yes","y","on"))

LOG_SIMPLE       = _env_bool("LOG_SIMPLE", True)
LOG_HTTP_ACCESS  = _env_bool("LOG_HTTP_ACCESS", False)
LOG_LEVEL        = os.getenv("LOG_LEVEL", "INFO").upper()

fmt_simple  = "%(asctime)s | %(levelname)s | %(message)s"
fmt_verbose = "%(asctime)s [%(levelname)s] %(name)s - %(message)s"
formatter   = logging.Formatter(fmt_simple if LOG_SIMPLE else fmt_verbose)

console_handler = logging.StreamHandler()
console_handler.setLevel(LOG_LEVEL)
console_handler.setFormatter(formatter)

server_log_path = os.path.join(LOG_DIR, "server.log")
file_handler = RotatingFileHandler(server_log_path, maxBytes=5*1024*1024, backupCount=5, encoding="utf-8")
file_handler.setLevel(LOG_LEVEL)
file_handler.setFormatter(formatter)

app.logger.setLevel(LOG_LEVEL)
app.logger.handlers.clear()
app.logger.addHandler(console_handler)
app.logger.addHandler(file_handler)
# === CSV logging helpers (thread-safe) ===
from threading import Lock
LOG_WRITE_LOCK = Lock()

def _append_row_csv(path, row_dict):
    """
    Append one row to a CSV safely, even with many threads.
    Creates the file with a header on first write.
    Uses a temp file + os.replace to avoid partial writes on Windows.
    """
    import csv, os, tempfile

    os.makedirs(os.path.dirname(path), exist_ok=True)

    with LOG_WRITE_LOCK:
        file_exists = os.path.exists(path)
        fd, tmp_path = tempfile.mkstemp(prefix=os.path.basename(path)+".", suffix=".tmp",
                                        dir=os.path.dirname(path))
        try:
            with os.fdopen(fd, "w", newline="", encoding="utf-8") as ftmp:
                writer = csv.DictWriter(ftmp, fieldnames=list(row_dict.keys()))
                if not file_exists:
                    writer.writeheader()
                writer.writerow(row_dict)

            if file_exists:
                with open(path, "a", newline="", encoding="utf-8") as fout, \
                     open(tmp_path, "r", encoding="utf-8") as fin:
                    first_line = fin.readline()
                    if fout.tell() == 0:
                        fout.write(first_line)  # header
                    for line in fin:
                        fout.write(line)
                os.remove(tmp_path)
            else:
                os.replace(tmp_path, path)
        except Exception:
            try: os.remove(tmp_path)
            except Exception: pass
            raise
# ==== Media ID cache (reuse uploadMedia id) ====
MEDIA_CACHE_FILE = os.path.join(BASE_DIR, "media_cache.json")
MEDIA_TTL_SECONDS = 2 * 60 * 60  # 2 hours
MEDIA_CACHE_LOCK = Lock()


def _file_md5(path: str, chunk_size: int = 8192) -> str:
    """Return md5 hash of a file (to detect same file even if renamed)."""
    h = hashlib.md5()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _load_media_cache() -> dict:
    """Load media cache JSON from disk."""
    if not os.path.exists(MEDIA_CACHE_FILE):
        return {}
    try:
        with open(MEDIA_CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_media_cache(cache: dict) -> None:
    """Save media cache JSON to disk (thread-safe)."""
    os.makedirs(os.path.dirname(MEDIA_CACHE_FILE), exist_ok=True)
    with open(MEDIA_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


def get_cached_media_id(file_path: str) -> str | None:
    """
    Return a cached media_id for this file if:
      - entry exists
      - not older than MEDIA_TTL_SECONDS
      - file size matches
    """
    try:
        if not os.path.exists(file_path):
            return None

        key = _file_md5(file_path)
        size_now = os.path.getsize(file_path)
        now = time.time()

        with MEDIA_CACHE_LOCK:
            cache = _load_media_cache()
            entry = cache.get(key)
            if not entry:
                return None

            ts = entry.get("timestamp", 0)
            size_cached = entry.get("size", 0)
            media_id = entry.get("media_id")

            # expired or size changed → drop
            if (now - ts) > MEDIA_TTL_SECONDS or size_cached != size_now or not media_id:
                cache.pop(key, None)
                _save_media_cache(cache)
                return None

            return media_id
    except Exception:
        return None


def store_media_id(file_path: str, media_id: str) -> None:
    """Store/refresh media_id for given file in cache."""
    try:
        if not os.path.exists(file_path) or not media_id:
            return

        key = _file_md5(file_path)
        size_now = os.path.getsize(file_path)
        now = time.time()

        with MEDIA_CACHE_LOCK:
            cache = _load_media_cache()
            cache[key] = {
                "media_id": media_id,
                "timestamp": now,
                "size": size_now,
                "path": file_path,
            }
            _save_media_cache(cache)
    except Exception:
        # cache failures should never break sending
        app.logger.exception("[upload/cache] store_media_id failed")

werk = logging.getLogger("werkzeug")
werk.handlers.clear()
if LOG_HTTP_ACCESS:
    werk.setLevel(LOG_LEVEL)
    werk.addHandler(console_handler)
    werk.addHandler(file_handler)
else:
    werk.setLevel(logging.WARNING)
    werk.addHandler(console_handler)
    werk.addHandler(file_handler)

@app.before_request
def _log_request():
    g.request_id = str(uuid.uuid4())
    g._start_ts = time.time()
    app.logger.info(f"[rid={g.request_id}] ➡ {request.method} {request.path}")

@app.after_request
def _log_response(response):
    dur_ms = int((time.time() - getattr(g, "_start_ts", time.time())) * 1000)
    app.logger.info(f"[rid={getattr(g,'request_id','-')}] ✅ {response.status_code} in {dur_ms}ms")
    return response

# -------------------------------------------------------
# WhatsApp Template Registry (optional)
# -------------------------------------------------------
_template_registry = {}
def load_template_registry():
    """Load templates_registry.xlsx into memory (if present)."""
    global _template_registry
    try:
        if os.path.exists(REGISTRY_XLSX):
            df = pd.read_excel(REGISTRY_XLSX).fillna("")
            cols = {c.lower().strip(): c for c in df.columns}

            tn = cols.get("template_name") or "template_name"
            lc = cols.get("language_code") or "language_code"
            ht = cols.get("header_type") or "header_type"
            vc = cols.get("body_var_count") or "body_var_count"
            dl = cols.get("default_media_link") or "default_media_link"
            ft = cols.get("fallback_text") or "fallback_text"        # NEW
            dm = cols.get("delay_min") or "delay_min"                # NEW

            reg = {}
            for _, r in df.iterrows():
                name = str(r.get(tn, "")).strip()
                if not name:
                    continue
                try:
                    delay_val = int(str(r.get(dm, "")).strip() or "0")
                except Exception:
                    delay_val = 0
                reg[name.lower()] = {
                    "language": (str(r.get(lc, "")).strip() or "en"),
                    "header":   (str(r.get(ht, "none")).strip().lower() or "none"),
                    "vars":     int(r.get(vc) or 0),
                    "default_media_link": str(r.get(dl, "")).strip(),
                    "fallback_text": str(r.get(ft, "")).strip(),      # NEW
                    "delay_min": delay_val                             # NEW
                }
            _template_registry = reg
            app.logger.info(f"[registry] loaded {len(_template_registry)} template(s)")
        else:
            _template_registry = {}
            app.logger.info("[registry] no templates_registry.xlsx; registry empty")
    except Exception as e:
        _template_registry = {}
        app.logger.exception(f"[registry] load failed: {e}")
    return _template_registry

load_template_registry()
# ---- Template header helpers (needed by /busy_send) ----
def get_template_header_type(tpl: str) -> str:
    """
    Reads header type from templates_registry.xlsx (loaded into _template_registry).
    Returns one of: 'document', 'image', 'video', 'none'.
    Falls back to 'none' if unknown.
    """
    info = _template_registry.get((tpl or "").lower().strip(), {}) if isinstance(_template_registry, dict) else {}
    h = (info.get("header") or "none").lower().strip()
    return h if h in ("document", "image", "video", "none") else "none"

def get_template_default_media_link(tpl: str) -> str | None:
    """
    Returns default media link (if any) for the template from the registry,
    otherwise None.
    """
    info = _template_registry.get((tpl or "").lower().strip(), {}) if isinstance(_template_registry, dict) else {}
    d = (info.get("default_media_link") or "").strip()
    return d or None

# Quick helpers
def _find_first_sale_template_default() -> str:
    """Pick a reasonable default template name for sale-messages."""
    for key in ("salesshn","salesshr","salemain","saletall"):
        if key in _template_registry:
            return key
    # fallback to any registry template
    if _template_registry:
        return list(_template_registry.keys())[0]
    return (TEMPLATE_DEFAULT or "invoice1").lower()

# -------------------------------------------------------
# WhatsApp helpers
# -------------------------------------------------------
# ===== Fallback (BotMasterSender) config =====
FALLBACK_ENABLE = (os.getenv("FALLBACK_ENABLE", "1").strip().lower() in ("1","true","yes"))
FALLBACK_API_URL = os.getenv("FALLBACK_API_URL", "").strip()
FALLBACK_SENDER_ID = os.getenv("FALLBACK_SENDER_ID", "").strip()
FALLBACK_AUTH_TOKEN = os.getenv("FALLBACK_AUTH_TOKEN", "").strip()

def _fallback_can_send() -> bool:
    return bool(FALLBACK_ENABLE and FALLBACK_API_URL and FALLBACK_SENDER_ID and FALLBACK_AUTH_TOKEN)
    
# 🔁 GLOBAL TOGGLE: force *all* messages via fallback (BotMaster)
FORCE_FALLBACK_ALL_DEFAULT = _env_bool("FORCE_FALLBACK_ALL", False)
GLOBAL_FORCE_FALLBACK_ALL = FORCE_FALLBACK_ALL_DEFAULT

def _build_public_media_url(pdf_path: str | None) -> str | None:
    """If we have a saved PDF path and PUBLIC_BASE_URL is set, return a public URL."""
    if not pdf_path:
        return None
    try:
        fname = os.path.basename(pdf_path)
        return f"{PUBLIC_BASE_URL.rstrip('/')}/files/{fname}"
    except Exception:
        return None
def _to_static_url(abs_path: str) -> str:
    """
    Converts a local absolute file path into a URL served by /static.

    Example:
      C:\BusyWin\IMAGES\BCN\_thumbs\801801.jpg
    -> http://<SERVER>:5000/static/BCN/_thumbs/801801.jpg

    ✅ Works for Windows paths
    ✅ Uses current request host so Android gets correct full URL
    """
    if not abs_path:
        return ""

    try:
        static_root = os.path.abspath(app.static_folder)  # ...\static
        p = os.path.abspath(abs_path)

        # Must be inside static folder
        if not p.lower().startswith(static_root.lower()):
            return ""

        rel = os.path.relpath(p, static_root)  # BCN\_thumbs\801801.jpg
        rel = rel.replace("\\", "/")           # BCN/_thumbs/801801.jpg

        return request.host_url.rstrip("/") + "/static/" + rel
    except Exception:
        return ""

def _values_to_text(template_name: str, values: list[str]) -> str:
    """Convert template + values to a compact plain text line for fallback."""
    vals = [v for v in (values or []) if str(v).strip()]
    parts = [f"[{template_name}]"] if template_name else []
    if vals:
        parts.append(" | ".join(vals))
    parts.append("— Subhash Saree House")
    return " ".join(parts)
def _render_with_placeholders(text: str, values: list[str]) -> str:
    """Replace {1}..{n} placeholders with values[0..n-1]."""
    if not text:
        return ""
    vals = [""] + [str(v or "").strip() for v in (values or [])]
    import re
    def repl(m):
        try:
            idx = int(m.group(1))
            return vals[idx] if idx < len(vals) else ""
        except Exception:
            return ""
    return re.sub(r"\{(\d+)\}", repl, text)

def registry_fallback_text(template_name: str, values: list[str]) -> str | None:
    """Return formatted fallback text from registry, or None."""
    info = _template_registry.get((template_name or "").lower().strip(), {})
    ft = (info.get("fallback_text") or "").strip()
    if not ft:
        return None
    return _render_with_placeholders(ft, values or [])

def delay_minutes_for_template(template_name: str) -> int:
    """Return delay in minutes (Excel override, else 10 min for coupon templates)."""
    t = (template_name or "").lower().strip()
    info = _template_registry.get(t, {})
    dm = int(info.get("delay_min") or 0)
    if dm <= 0 and "cup" in t:
        dm = 10
    return max(0, dm)

def send_fallback_botmaster(receiver_id: str, text: str, media_url: str | None = None, local_path: str | None = None) -> tuple[int, dict]:
    """
    Send via BotMasterSender. Prefers direct file upload if local_path is provided,
    otherwise falls back to mediaurl, otherwise plain text.
    """
    if DRY_RUN:
        app.logger.info(f"[FALLBACK] (dry-run) to={receiver_id} text~={text[:80]} media={bool(media_url or local_path)}")
        return 200, {"dry_run": True}

    # Base payload
    payload = {
        "senderId": str(FALLBACK_SENDER_ID),
        "receiverId": str(receiver_id),
        "messageText": str(text),
        "authToken": str(FALLBACK_AUTH_TOKEN),
    }

    try:
        if local_path and os.path.exists(local_path):
            # 🔼 Direct file upload
            files = {"uploadFile": open(local_path, "rb")}
            r = requests.post(FALLBACK_API_URL, data=payload, files=files, timeout=90)
        else:
            # 🔗 Fallback to mediaurl or text
            if media_url:
                payload["mediaurl"] = media_url
            r = requests.post(FALLBACK_API_URL, json=payload, timeout=60)

        try:
            jr = r.json()
        except Exception:
            jr = {"raw": r.text}

        app.logger.info(f"[FALLBACK] ↩ {r.status_code} body~={str(jr)[:160]}")
        return r.status_code, jr
    except Exception as e:
        app.logger.exception("[FALLBACK] request failed")
        return 500, {"error": {"message": str(e)}}

def append_fallback_log(to, text, media_url, status_code, body):
    try:
        row = {
            "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "to": to,
            "text": text,
            "media_url": media_url or "",
            "status_code": status_code,
            "response": json.dumps(body, ensure_ascii=False)
        }
        _append_row_csv(os.path.join(LOG_DIR, "fallback.csv"), row)
    except Exception:
        app.logger.exception("[FALLBACK] log write failed")

# --- Extra attachments when sending SALES templates via fallback ---

# CHANGE THESE PATHS to your two real files
SALE_EXTRA_ATTACHMENTS = [
    r"C:\BusyWin\IMAGES\QR.png",   # e.g. catalogue
    r"C:\BusyWin\IMAGES\Aabhash Agrawal.vcf",   # e.g. offer poster
]

def _is_sales_template(tpl: str | None) -> bool:
    """True only for your 'sales...' templates (salesshn, salesshr, salesmain, salestall, salespall, etc.)."""
    t = (tpl or "").strip().lower()
    return t.startswith("sales")   # covers all your sales templates from Excel

def send_fallback_with_sales_extras(
    to: str,
    text: str,
    * ,
    media_url: str | None = None,
    local_path: str | None = None,
    template_name: str | None = None,
):
    """
    Wrapper around send_fallback_botmaster:
      - Sends the normal fallback message.
      - If template is a SALES template, also sends 2 extra attachments (if files exist).
    """
    # 1) main fallback send (with full bill text)
    sc, body = send_fallback_botmaster(to, text, media_url=media_url, local_path=local_path)
    append_fallback_log(to, text, media_url, sc, body)

    # 2) extra attachments only for SALES templates and only if first send was OK
    if 200 <= sc < 300 and _is_sales_template(template_name):
        for extra_path in SALE_EXTRA_ATTACHMENTS:
            try:
                if not extra_path or not os.path.exists(extra_path):
                    continue

                # ❗ For extra attachments: send NO TEXT / CAPTION
                extra_text = ""  # if this ever causes error, change to " "

                sc2, body2 = send_fallback_botmaster(
                    to,
                    extra_text,
                    media_url=None,
                    local_path=extra_path
                )
                append_fallback_log(to, extra_text, None, sc2, body2)
                app.logger.info(f"[FALLBACK] extra sales attachment sent (no text): {extra_path} to={to} status={sc2}")
            except Exception:
                app.logger.exception("[FALLBACK] extra sales attachment failed")

    return sc, body


@app.get("/logs/peek")
def logs_peek():
    """Show last N lines of all_attempts.csv as plain text."""
    import itertools
    n = int(request.args.get("n", 200))
    p = os.path.join(LOG_DIR, "all_attempts.csv")
    if not os.path.exists(p):
        return "no CSV yet", 404
    with open(p, "r", encoding="utf-8", errors="ignore") as f:
        lines = f.readlines()
    tail = "".join(lines[-n:])
    return "<pre>" + tail.replace("<","&lt;").replace(">","&gt;") + "</pre>"


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in {'pdf','jpg','jpeg'}
# ---- media kind helper ----
HEADER_DOC = "document"
HEADER_IMG = "image"
HEADER_VID = "video"
HEADER_NONE = "none"

def detect_local_media_kind(path: str | None) -> str:
    if not path:
        return HEADER_NONE
    ext = os.path.splitext(path)[1].lower()
    if ext in (".pdf",):
        return HEADER_DOC
    if ext in (".jpg", ".jpeg", ".png", ".webp"):
        return HEADER_IMG
    if ext in (".mp4", ".mov", ".m4v", ".avi", ".3gp", ".mkv"):
        return HEADER_VID
    return HEADER_NONE

def _make_bsp_headers():
    headers = {"Content-Type": "application/json"}
    if WABA:
        headers["wabaNumber"] = WABA
    if AUTH_BEARER:
        headers["Authorization"] = f"Bearer {API_KEY}"
    else:
        headers[KEY_HEADER_NAME] = API_KEY
    return headers

# --- plain text sender (for OTP etc.) ---
def send_plain_text(to: str, body: str):
    """
    Send OTP/text via BSP.
    IMPORTANT: Log full response so we can see if BSP is actually accepting it.
    If BSP text fails (or returns no message id), auto-send via fallback.
    """
    to = resolve_receiver(to) or str(to)

    if DRY_RUN:
        app.logger.info(f"[BSP/TEXT] (dry-run) to={to} body={body[:120]}")
        return 200, {"dry_run": True, "to": to, "body": body}

    # If API_URL is missing → direct fallback
    if not API_URL:
        app.logger.error("[BSP/TEXT] API_URL missing, using fallback")
        if _fallback_can_send():
            sc, fb = send_fallback_botmaster(to, body)
            return sc, {"fallback_used": True, "fallback": fb}
        return 500, {"error": {"message": "API_URL missing and fallback not configured"}}

    payload = {
        "messaging_product": "whatsapp",
        "to": str(to),
        "type": "text",
        "text": {"body": str(body)}
    }

    try:
        r = requests.post(API_URL, headers=_make_bsp_headers(), json=payload, timeout=60)

        # Always capture body
        try:
            jr = r.json()
        except Exception:
            jr = {"raw": r.text}

        # ✅ Log full response (THIS is what you need right now)
        app.logger.info(f"[BSP/TEXT] ↩ {r.status_code} body={str(jr)[:800]}")

        # Try to extract message id (Meta cloud style)
        msg_id = None
        if isinstance(jr, dict):
            msgs = jr.get("messages")
            if isinstance(msgs, list) and msgs and isinstance(msgs[0], dict):
                msg_id = msgs[0].get("id")

        # If not a clean success → fallback
        if not (200 <= r.status_code < 300) or not msg_id:
            app.logger.warning(f"[BSP/TEXT] no msg_id / not accepted → trying fallback to={to}")
            if _fallback_can_send():
                sc, fb = send_fallback_botmaster(to, body)
                return sc, {
                    "bsp_status": r.status_code,
                    "bsp_response": jr,
                    "fallback_used": True,
                    "fallback": fb
                }

        return r.status_code, jr

    except Exception as e:
        app.logger.exception("[BSP/TEXT] request failed, trying fallback")
        if _fallback_can_send():
            sc, fb = send_fallback_botmaster(to, body)
            return sc, {"error": str(e), "fallback_used": True, "fallback": fb}
        return 500, {"error": {"message": str(e)}}
@app.get("/test/send_text")
def test_send_text():
    to = request.args.get("to", "").strip()
    msg = request.args.get("msg", "Test message from SSH server").strip()
    to = resolve_receiver(to) or to
    sc, jr = send_plain_text(to, msg)
    return jsonify({"ok": 200 <= sc < 300, "status": sc, "resp": jr})
# ✅ Make send_plain_text available to auth_otp.py (blueprint)
app.config["AUTH_SEND_TEXT"] = send_plain_text
# --- media upload for template header doc/image ---
# --- media upload for template header doc/image (with 2-hour cache) ---
def upload_media_get_id(local_path: str) -> str | None:
    """
    Upload a local file via BSP /uploadMedia and return media_id.
    If the same file (same content) was uploaded in last 2 hours,
    reuse the cached media_id instead of uploading again.
    """
    try:
        if not local_path or not os.path.exists(local_path):
            app.logger.error(f"[upload] local file missing: {local_path}")
            return None

        # 1) Try cache first
        cached_id = get_cached_media_id(local_path)
        if cached_id:
            app.logger.info(f"[upload/cache] reusing media_id={cached_id} for {os.path.basename(local_path)}")
            return cached_id

        # 2) No valid cache → do real upload
        if not API_URL:
            app.logger.error("[upload] API_URL missing")
            return None

        if API_URL.endswith("/message"):
            url = API_URL.rsplit("/", 1)[0] + "/uploadMedia"
        else:
            url = API_URL.rstrip("/") + "/uploadMedia"

        # Minimal mime map
        ext = os.path.splitext(local_path)[1].lower()
        if ext == ".pdf":
            mime = "application/pdf"
        elif ext in (".jpg", ".jpeg"):
            mime = "image/jpeg"
        elif ext == ".png":
            mime = "image/png"
        elif ext == ".webp":
            mime = "image/webp"
        elif ext in (".mp4", ".m4v"):
            mime = "video/mp4"
        elif ext in (".mov", ".avi", ".3gp", ".mkv"):
            # many BSPs only accept MP4; still try with mp4 mime
            mime = "video/mp4"
        else:
            mime = "application/octet-stream"

        headers = {"wabaNumber": WABA, KEY_HEADER_NAME: API_KEY}
        app.logger.info(f"[upload] → {os.path.basename(local_path)} ({mime})")
        with open(local_path, "rb") as f:
            files = {"file": (os.path.basename(local_path), f, mime)}
            r = requests.post(url, headers=headers, files=files, timeout=90)

        try:
            jr = r.json()
        except Exception:
            jr = {}

        if r.status_code in (200, 201) and isinstance(jr, dict) and jr.get("id"):
            media_id = str(jr["id"])
            app.logger.info(f"[upload] ↩ 200 id={media_id}")
            # 3) Store in cache for next 2 hours
            store_media_id(local_path, media_id)
            return media_id

        app.logger.info(f"[upload] ↩ {r.status_code} body~={str(jr)[:160]}")
        return None
    except Exception as e:
        app.logger.exception(f"[upload] error: {e}")
        return None

from auth_otp import bp_auth_otp
app.register_blueprint(bp_auth_otp)
# --- template payload builder (FIXED: lowercase component types) ---
def build_payload(
    to,
    template_name,
    language_code="en",
    pdf_link=None,
    pdf_id=None,
    image_link=None,
    image_id=None,
    video_link=None,
    video_id=None,
    body_params=None
):
    components = []

    # Choose exactly one header, precedence: document > image > video
    if pdf_id or pdf_link:
        doc_obj = {"id": pdf_id} if pdf_id else {"link": pdf_link}
        doc_obj["filename"] = "SUBHASH SAREE HOUSE.pdf"
        components.append({
            "type": "header",                                    # <— lowercase
            "parameters": [{"type": "document", "document": doc_obj}]
        })
    elif image_id or image_link:
        img_obj = {"id": image_id} if image_id else {"link": image_link}
        components.append({
            "type": "header",
            "parameters": [{"type": "image", "image": img_obj}]
        })
    elif video_id or video_link:
        vid_obj = {"id": video_id} if video_id else {"link": video_link}
        components.append({
            "type": "header",
            "parameters": [{"type": "video", "video": vid_obj}]
        })

    # Body parameters (OTP, etc.)
    if body_params:
        components.append({
            "type": "body",                                      # <— lowercase
            "parameters": [{"type": "text", "text": str(v)} for v in body_params]
        })

    return {
        "messaging_product": "whatsapp",
        "to": str(to),
        "type": "template",
        "template": {
            "name": template_name,
            "language": {"code": language_code, "policy": "deterministic"},
            "components": components
        }
    }

def send_bsp(payload: dict):
    headers = _make_bsp_headers()
    if DRY_RUN:
        app.logger.info("[BSP] (dry-run) send template")
        return 200, {"dry_run": True, "payload": {"template": payload.get('template',{}), "to": payload.get('to')}, "wamid": None}
    start = time.time()
    try:
        r = requests.post(API_URL, headers=headers, json=payload, timeout=60)
        try:
            jr = r.json()
        except Exception:
            jr = {"raw": r.text}
    except Exception as e:
        app.logger.error(f"[BSP] ❌ request failed: {e}")
        return 500, {"error": {"message": str(e)}, "wamid": None}
    elapsed = int((time.time() - start) * 1000)

    # Try to pull WhatsApp/BSP message id in common shapes
    wamid = None
    try:
        if isinstance(jr, dict):
            # Typical Cloud API: {"messages":[{"id":"wamid.HBgM..."}]}
            msgs = jr.get("messages")
            if isinstance(msgs, list) and msgs and isinstance(msgs[0], dict):
                wamid = msgs[0].get("id") or wamid
            # Some vendors wrap under "bsp_response"
            if not wamid and isinstance(jr.get("bsp_response"), dict):
                msgs2 = jr["bsp_response"].get("messages")
                if isinstance(msgs2, list) and msgs2 and isinstance(msgs2[0], dict):
                    wamid = msgs2[0].get("id") or wamid
    except Exception:
        pass

    status = r.status_code
    code = "-"
    if isinstance(jr, dict):
        err = jr.get("error") or {}
        code = err.get("code") or err.get("error_code") or "-"

    app.logger.info(f"[BSP] ↩ {status} ({code}) in {elapsed}ms wamid={wamid}")
    if isinstance(jr, dict):
        jr["wamid"] = wamid
    return status, jr

def classify_response(resp_json: dict) -> str:
    try:
        bsp = resp_json.get("bsp_response", {})
        status = resp_json.get("status_code", 0)
        msgs = bsp.get("messages") if isinstance(bsp, dict) else None
        if isinstance(msgs, list) and msgs:
            msg = msgs[0] if isinstance(msgs[0], dict) else {}
            if msg.get("id") and msg.get("message_status", "").lower() in ("accepted", "queued", "sent", ""):
                return "success"
        err = {}
        if isinstance(bsp, dict):
            err = bsp.get("error") or {}
        code = (err.get("code") or err.get("error_code") or 0) if isinstance(err, dict) else 0
        msg_text = (err.get("message") or "").lower() if isinstance(err, dict) else ""
        if code in (1051, 401, 403) or "invalidkey" in msg_text or "invalid key" in msg_text:
            return "auth_error"
        if code in (131026, 1012) or "not a whatsapp user" in msg_text or "not on whatsapp" in msg_text:
            return "not_on_whatsapp"
        if status == 200 and not msgs and not err:
            return "failed"
        return "failed"
    except Exception:
        return "failed"

def append_logs(to, template_name, language_code, header_sent, values, result, attempts=1):
    try:
        status_class = classify_response(result)
        row = {
            "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "to": to,
            "template_name": template_name,
            "language_code": language_code,
            "header_sent": header_sent,
            "values": json.dumps(values or [], ensure_ascii=False),
            "status": status_class,
            "attempts": attempts,
            "status_code": result.get("status_code"),
            "bsp_response": json.dumps(result.get("bsp_response", {}), ensure_ascii=False)
        }
        base = LOG_DIR
        _append_row_csv(os.path.join(base, "all_attempts.csv"), row)
        if status_class == "success":
            _append_row_csv(os.path.join(base, "success.csv"), row)
        elif status_class == "not_on_whatsapp":
            _append_row_csv(os.path.join(base, "not_on_whatsapp.csv"), row)
        elif status_class == "auth_error":
            _append_row_csv(os.path.join(base, "auth_error.csv"), row)
        else:
            _append_row_csv(os.path.join(base, "failed.csv"), row)

        app.logger.info(f"[LOG] {row['status']} | to={to} | tpl={template_name} | lang={language_code} | attempts={attempts}")
    except Exception as e:
        app.logger.exception(f"[log] write failed: {e}")


# -------------------------------------------------------
# WhatsApp async queue (worker + retries)
# -------------------------------------------------------
JobQueue: "queue.Queue[dict]" = queue.Queue()
JOBS = {}  # job_id -> {"status": "...", "attempts": n, "last_result": {...}}
# --- Auto-fallback by timeout (safety net) ---
WA_PENDING: dict[str, dict] = {}     # id -> {"ctx":..., "sent_at": float, "fallback_done": bool}
FALLBACK_TIMEOUT_MIN = int(os.getenv("FALLBACK_TIMEOUT_MIN", "8"))  # override via .env (we set 6)
# Map BSP/WhatsApp message id -> context so we can fallback later if delivery fails
WA_MSG_MAP: dict[str, dict] = {}
# Prevent duplicate fallback for the same WAMID
WA_FALLBACK_DONE: set[str] = set()


def _exponential_backoff(attempt: int) -> int:
    return min(RETRY_MAX_SLEEP, 2 ** (attempt - 1))

def _send_once(job_payload: dict):
    track_id = None  # ensure defined even if something fails early

    payload = build_payload(
        to=job_payload["to"],
        template_name=job_payload["template_name"],
        language_code=job_payload["language_code"],
        pdf_link=job_payload.get("pdf_link"),
        pdf_id=job_payload.get("media_id"),       # document id
        image_link=job_payload.get("image_link"),
        image_id=job_payload.get("image_id"),     # image id
        video_link=job_payload.get("video_link"),
        video_id=job_payload.get("video_id"),     # video id
        body_params=job_payload.get("values")
    )

    status, resp = send_bsp(payload)

    # --- Register message to Google Sheet for webhook fallback + track for timeout ---
    try:
        wamid = resp.get("wamid") if isinstance(resp, dict) else None

        fb_text = job_payload.get("fallback_text") or _values_to_text(
            job_payload["template_name"], job_payload.get("values") or []
        )

        mediaurl = job_payload.get("pdf_link") or job_payload.get("image_link") or job_payload.get("video_link")

        local_public = None
        if not mediaurl and job_payload.get("pdf_path"):
            local_public = _build_public_media_url(job_payload.get("pdf_path"))

        _register_to_sheet(wamid or f"noid-{uuid.uuid4()}", job_payload["to"], fb_text, mediaurl, local_public)

        track_id = (wamid if isinstance(wamid, str) and wamid else f"noid-{uuid.uuid4()}")
        WA_PENDING[track_id] = {
            "ctx": {
                "to": job_payload["to"],
                "template_name": job_payload["template_name"],
                "values": job_payload.get("values") or [],
                "raw_text": fb_text,
                "media_url": mediaurl or local_public,
                "local_path": job_payload.get("pdf_path")
            },
            "sent_at": time.time(),
            "fallback_done": False
        }
    except Exception:
        app.logger.exception("[GS/TIMEOUT] post-send register/track error")

    # store wamid->context for webhook-based fallback later
    try:
        wamid = resp.get("wamid") if isinstance(resp, dict) else None
        if wamid:
            WA_MSG_MAP[wamid] = {
                "to": job_payload["to"],
                "template_name": job_payload["template_name"],
                "values": job_payload.get("values") or [],
                "media_url": job_payload.get("pdf_link") or job_payload.get("image_link") or job_payload.get("video_link"),
                "local_path": job_payload.get("pdf_path"),
                "raw_text": job_payload.get("fallback_text")
                            or registry_fallback_text(job_payload["template_name"], job_payload.get("values") or [])
                            or _values_to_text(job_payload["template_name"], job_payload.get("values") or [])
            }
    except Exception:
        app.logger.exception("wamid mapping failed")

    header_sent = "none"
    if job_payload.get("media_id") or job_payload.get("pdf_link"):
        header_sent = "document"
    elif job_payload.get("image_id") or job_payload.get("image_link"):
        header_sent = "image"
    elif job_payload.get("video_id") or job_payload.get("video_link"):
        header_sent = "video"

    return status, resp, header_sent, track_id

def _worker():
    with app.app_context():
        while True:
            job = JobQueue.get()
            job_id = job["job_id"]
            JOBS[job_id]["status"] = "sending"
            attempts = 0
            last_result = None
            header_sent = "none"
            track_id = None   # 👈 add this line
            app.logger.info(f"[job={job_id}] 🚚 sending to={job['to']} tpl={job['template_name']}")

            # Upload media (doc/img/video) if needed
            media_id = None
            image_id = None
            video_id = None
            
            pdf_link   = job.get("pdf_link") or None
            image_link = job.get("image_link") or None
            video_link = job.get("video_link") or None
            local_path = job.get("pdf_path") or None  # reusing this field name to avoid schema changes
            
            if (not pdf_link and not image_link and not video_link) and local_path:
                kind = detect_local_media_kind(local_path)
                try:
                    up_id = upload_media_get_id(local_path)
                except Exception:
                    up_id = None
                if up_id:
                    if kind == HEADER_DOC:
                        media_id = up_id
                    elif kind == HEADER_IMG:
                        image_id = up_id
                    elif kind == HEADER_VID:
                        video_id = up_id
            
            job["media_id"] = media_id
            job["image_id"] = image_id
            job["video_id"] = video_id



            # Retry loop
            while attempts < RETRY_MAX:
                attempts += 1
                try:
                    status, resp, header_sent, track_id = _send_once(job)
                except Exception as e:
                    status, resp = 500, {"bsp_response": {"error": {"message": str(e)}}}

                result = {"status_code": status, "bsp_response": resp}
                last_result = result
                JOBS[job_id]["attempts"] = attempts
                JOBS[job_id]["last_result"] = result
                cls = classify_response(result)

                if cls == "success":
                    append_logs(job["to"], job["template_name"], job["language_code"], header_sent,
                                job.get("values"), result, attempts=attempts)
                    JOBS[job_id]["status"] = "success"
                    # cancel timeout/fallback
                    if track_id:
                        _mark_delivered_by_track_id(track_id)
                    wamid_success = resp.get("wamid") if isinstance(resp, dict) else None
                    if wamid_success:
                        _mark_delivered_by_wamid(wamid_success)
                    app.logger.info(f"[job={job_id}] ✅ delivered in {attempts} attempt(s)")
                    break

                if cls == "auth_error":
                    append_logs(job["to"], job["template_name"], job["language_code"], header_sent,
                                job.get("values"), result, attempts=attempts)
                    JOBS[job_id]["status"] = "auth_error"
                    app.logger.error(f"[job={job_id}] ❌ auth_error after {attempts} attempt(s)")
                    break

                if cls == "not_on_whatsapp":
                    append_logs(job["to"], job["template_name"], job["language_code"], header_sent,
                                job.get("values"), result, attempts=attempts)
                    JOBS[job_id]["status"] = "not_on_whatsapp"
                    app.logger.error(f"[job={job_id}] ❌ not_on_whatsapp after {attempts} attempt(s)")
                    break

                sleep_s = _exponential_backoff(attempts)
                app.logger.warning(f"[job={job_id}] retry {attempts}/{RETRY_MAX} in {sleep_s}s")
                time.sleep(sleep_s)

            # If still failed → log + fallback
            if JOBS[job_id]["status"] in ("sending", "failed") or (
                attempts >= RETRY_MAX and JOBS[job_id]["status"] not in ("success", "auth_error", "not_on_whatsapp")
            ):
                append_logs(job["to"], job["template_name"], job["language_code"], header_sent,
                            job.get("values"), last_result or {"status_code": 0, "bsp_response": {}}, attempts=attempts)
                JOBS[job_id]["status"] = "failed"
                app.logger.error(f"[job={job_id}] ❌ failed after {attempts} attempt(s)")

                # 🔁 FINAL CHANCE → Fallback API
                if _fallback_can_send():
                    try:
                        fb_text = job.get("fallback_text") or registry_fallback_text(job.get("template_name",""), job.get("values", [])) or _values_to_text(job.get("template_name",""), job.get("values", []))
                        fb_media = job.get("fallback_media_url")

                        sc, body = send_fallback_with_sales_extras(
                            job["to"],
                            fb_text,
                            media_url=fb_media,
                            local_path=job.get("pdf_path"),
                            template_name=job.get("template_name"),
                        )

                        if 200 <= sc < 300:
                            # ✅ success via fallback → cancel any pending timeout/fallback
                            if track_id:
                                _mark_delivered_by_track_id(track_id)

                            app.logger.info(f"[job={job_id}] ✅ delivered via FALLBACK")
                            JOBS[job_id]["status"] = "success_fallback"
                            JOBS[job_id]["last_result"] = {"status_code": sc, "fallback_response": body}
                    except Exception:
                        app.logger.exception(f"[job={job_id}] fallback threw an exception")



            JobQueue.task_done()

_worker_threads = []
for i in range(max(1, NUM_WORKERS)):
    t = threading.Thread(target=_worker, name=f"sender-worker-{i+1}", daemon=True)
    t.start()
    _worker_threads.append(t)

def enqueue_send_job(to, template_name, language_code="en", values=None,
                     pdf_link=None, image_link=None, pdf_path=None, raw_text=None, video_link=None,
                     fallback_text: str | None = None,
                     delay_seconds: int = 0):
    """Queue (or schedule-delayed) a send job, honoring registry fallback_text and delay.

    If GLOBAL_FORCE_FALLBACK_ALL is True, we SKIP BSP entirely and send via BotMasterSender
    (fallback) instead, still respecting delay_seconds.
    """
    global GLOBAL_FORCE_FALLBACK_ALL

    job_id = str(uuid.uuid4())

    # Prefer Excel fallback text first
    pretty_fb = registry_fallback_text(template_name, values or [])
    fb_text = pretty_fb or fallback_text or raw_text or _values_to_text(template_name, values or [])

    # Decide media for fallback
    public_media_url = None
    local_path_for_fb = None

    if pdf_link:
        public_media_url = pdf_link
    elif image_link:
        public_media_url = image_link
    elif video_link:
        public_media_url = video_link
    elif pdf_path:
        # We will upload the local file directly to BotMaster
        local_path_for_fb = pdf_path

    JOBS[job_id] = {"status": "queued", "attempts": 0, "last_result": None}

    def _put_to_bsp_queue():
        JobQueue.put({
            "job_id": job_id,
            "to": str(to),
            "template_name": template_name,
            "language_code": language_code,
            "values": (values or []),
            "pdf_link": (pdf_link or None),
            "image_link": (image_link or None),
            "video_link": (video_link or None),
            "pdf_path": (pdf_path or None),
            "fallback_text": fb_text,
            "fallback_media_url": public_media_url,
            "image_id": None,
            "video_id": None,
        })

    def _send_via_fallback():
        try:
            if not _fallback_can_send():
                app.logger.error("[queue] FORCE_FALLBACK_ALL is ON but fallback is not configured!")
                JOBS[job_id]["status"] = "failed_fallback_not_configured"
                return

            app.logger.info(f"[queue→fallback] job_id={job_id} to={to} tpl={template_name}")

            # 🔁 use wrapper that also sends extra sales attachments
            sc, body = send_fallback_with_sales_extras(
                str(to),
                fb_text,
                media_url=public_media_url,
                local_path=local_path_for_fb,
                template_name=template_name,
            )

            JOBS[job_id]["attempts"] = 1
            if 200 <= sc < 300:
                JOBS[job_id]["status"] = "success_fallback"
                JOBS[job_id]["last_result"] = {"status_code": sc, "fallback_response": body}
                app.logger.info(f"[queue→fallback] ✅ delivered via fallback for job_id={job_id}")
            else:
                JOBS[job_id]["status"] = "failed_fallback"
                JOBS[job_id]["last_result"] = {"status_code": sc, "fallback_response": body}
                app.logger.error(f"[queue→fallback] ❌ fallback failed ({sc}) for job_id={job_id}")
        except Exception:
            JOBS[job_id]["status"] = "failed_fallback_exception"
            app.logger.exception(f"[queue→fallback] exception for job_id={job_id}")

    # 🔁 Decide: BSP queue (normal) vs. direct fallback (forced)
    if GLOBAL_FORCE_FALLBACK_ALL and _fallback_can_send():
        if delay_seconds and delay_seconds > 0:
            threading.Timer(delay_seconds, _send_via_fallback).start()
            app.logger.info(f"[queue→fallback] job scheduled id={job_id} delay={delay_seconds}s tpl={template_name}")
        else:
            _send_via_fallback()
            app.logger.info(f"[queue→fallback] job executed immediately id={job_id} tpl={template_name}")
    else:
        if delay_seconds and delay_seconds > 0:
            threading.Timer(delay_seconds, _put_to_bsp_queue).start()
            app.logger.info(f"[queue] job scheduled id={job_id} delay={delay_seconds}s tpl={template_name}")
        else:
            _put_to_bsp_queue()
            app.logger.info(f"[queue] job queued id={job_id} tpl={template_name}")

    return job_id
# ---------------- DAYBOOK CONFIG ----------------
DAYBOOK_DEFAULT_TO = "120363367991085282@g.us"   # your group id
DAYBOOK_OUTPUT_FOLDER = r"C:\BusyWin\AI BOT\BUSY_RECEIPT_BOT_STRUCTURE\OUTPUTS"
DAYBOOK_TEMPLATE_NAME = "daybook_pdf"  # must exist in templates_registry.xlsx

def send_daybook_bsp(date_from: str | None = None, date_to: str | None = None, to: str | None = None) -> dict:
    """
    Generate Daybook PDF (local) and send via BSP template with DOCUMENT header.
    """
    # default today
    if not date_from or not date_to:
        today = datetime.date.today().strftime("%Y-%m-%d")
        date_from = date_from or today
        date_to   = date_to   or today

    to = to or DAYBOOK_DEFAULT_TO
    to = resolve_receiver(to) or to  # keeps ...@g.us as-is

    # ✅ Use the function you imported from daybook.py
    # It MUST return the PDF path.
    pdf_path = generate_daybook(date_from, date_to, DAYBOOK_OUTPUT_FOLDER)  # <-- FIXED

    if not pdf_path or not os.path.exists(pdf_path):
        app.logger.error("[daybook] PDF not generated: %s", pdf_path)
        return {"ok": False, "error": "pdf_not_generated"}

    template_name = DAYBOOK_TEMPLATE_NAME
    tinfo = _template_registry.get(template_name.lower(), {})
    language_code = (tinfo.get("language") or "en").strip()

    caption = f"DAYBOOK {date_from} to {date_to}"
    values = [caption]  # {1} in template body

    job_id = enqueue_send_job(
        to=to,
        template_name=template_name,
        language_code=language_code,
        values=values,
        pdf_path=pdf_path,      # ✅ local PDF -> uploadMedia -> document header
        raw_text=caption,
        delay_seconds=0,
    )

    app.logger.info("[daybook] queued to=%s job_id=%s pdf=%s", to, job_id, pdf_path)
    return {"ok": True, "job_id": job_id, "pdf": os.path.basename(pdf_path)}

# -------------------------------------------------------
# EXISTING FUNCTIONS (face/dues/etc.)
# -------------------------------------------------------
def recognize_face(input_image_path):
    try:
        from deepface import DeepFace
    except ImportError:
        print("DeepFace not installed")
        return None
    for mobile in os.listdir(FACES_FOLDER):
        folder = os.path.join(FACES_FOLDER, mobile)
        face_img = os.path.join(folder, "face.jpg")
        if os.path.exists(face_img):
            try:
                result = DeepFace.verify(img1_path=face_img, img2_path=input_image_path, enforce_detection=False)
                if result.get("verified"):
                    return mobile
            except Exception:
                continue
    return None

def create_coupon_json_later(mobile, delay=60):
    def task():
        try:
            timestamp = datetime.datetime.now().strftime('%d-%m-%Y_%H%M%S')
            filename = f"register_customer_coupon_{mobile}_{timestamp}.json"
            filepath = os.path.join(QUEUE_DIR, filename)
            with open(filepath, 'w') as f:
                json.dump({"type": "register_customer_coupon", "mobile": mobile}, f)
            print(f"🎁 Coupon JSON created after delay: {filename}")
        except Exception as e:
            print("❌ Failed to create coupon JSON:", e)
    threading.Timer(delay, task).start()

def load_customer_dues():
    try:
        if not os.path.exists(EXCEL_FILE):
            return {}
        df = pd.read_excel(EXCEL_FILE, dtype=str).fillna("")
        dues = {
            str(row["mobile"])[-10:]: {"name": row.get("name", ""), "amount": row.get("amount", "0")}
            for _, row in df.iterrows()
        }
        return dues
    except Exception as e:
        print(f"Error loading Excel: {e}")
        return {}

def lookup_salesman(name):
    """Return the mobile (digits only) for an exact name match, else None."""
    try:
        if not os.path.exists(SALESMAN_FILE):
            return None
        df = pd.read_excel(SALESMAN_FILE, dtype=str).fillna("")
        target = (name or "").strip().casefold()
        for _, row in df.iterrows():
            nm = str(row.get("name", "")).strip().casefold()
            if nm == target:
                mob = re.sub(r"\D", "", str(row.get("mobile", "")))
                return mob if mob else None
        return None
    except Exception as e:
        print(f"Salesman lookup error: {e}")
        return None
def normalize_mobile(number: str) -> str:
    """Return normalized mobile with 91 prefix if it's a 10-digit number."""
    s = re.sub(r"\D", "", str(number or "").strip())  # keep only digits
    if len(s) == 10:
        return "91" + s
    return s
# --- Receiver normalization / staff-code mapping ---
# Map short staff codes (like "007") to real MSISDN with country code.
STAFF_CODE_MAP = {
    # "007": "91XXXXXXXXXX",  # e.g., Bunty
    # "BUNTY": "91XXXXXXXXXX",
    # Add more if you use short codes internally.
}

def resolve_receiver(raw_to: str) -> str | None:
    """
    Turn anything like '007', '7000229060', '+91 7000 229060' into a sendable MSISDN.
    ALSO supports WhatsApp Group IDs like '1203...@g.us' (returned as-is).
    """
    t = str(raw_to or "").strip()
    if not t:
        return None

    # ✅ 1) WhatsApp GROUP: keep full JID, do NOT touch
    if "@g.us" in t.lower():
        return t

    # 2) short staff codes
    if t in STAFF_CODE_MAP:
        return STAFF_CODE_MAP[t]

    # 3) normal mobile numbers
    s = re.sub(r"\D", "", t)  # keep only digits
    if len(s) == 10:
        return "91" + s
    if len(s) >= 11:
        return s
    return None

def slugify_filename(s: str) -> str:
    # Replace invalid filename chars and whitespace with underscores
    s = re.sub(r'[\\/:*?"<>|\s]+', '_', str(s).strip())
    # Collapse multiples and trim ends
    return re.sub(r'_+', '_', s).strip('_')
import pyodbc
import datetime

VCH_PURCHASE = 2
VCH_STTF     = 5
VCH_SALE     = 9

def _safe_float(x, default=0.0):
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default

def fetch_purchase_sale_summary_by_barcode(barcode: str) -> dict:
    """
    Returns exactly what Android needs:
      {
        purchase_party_alias: "MOTILAL VIJAY KUMAR",
        sold_parties: ["RANJU SINGH JI", ...],
        total_purchase_qty: 1,
        total_sold_qty: 1
      }

    ✅ Scans ALL BusyComp0001* databases (ONLINE)
    ✅ SQL Server 2012 safe
    ✅ FIXED PartyName logic:
        - PURCHASE (VchType=2): ALWAYS Party1 (MasterCode1)
        - SALE (VchType=9): Prefer Party2 if not internal, else Party1
    """
    if not barcode:
        return {
            "purchase_party_alias": "",
            "sold_parties": [],
            "total_purchase_qty": 0,
            "total_sold_qty": 0
        }

    rows = []

    with pyodbc.connect(SQL_CONN_STR) as conn:
        cur = conn.cursor()

        # 1) find BusyComp0001* dbs
        dbs = []
        for r in cur.execute("""
            SELECT name
            FROM sys.databases
            WHERE name LIKE 'BusyComp0001%'
              AND state_desc='ONLINE'
        """).fetchall():
            dbs.append(str(r[0]))

        # 2) scan each db if ItemParamDet exists
        for db in dbs:
            try:
                # check ItemParamDet exists
                chk = cur.execute(f"""
                    SELECT 1
                    FROM {db}.sys.tables
                    WHERE name='ItemParamDet'
                """).fetchone()
                if not chk:
                    continue

                sql = f"""
                SELECT
                    N'{db}' AS DbName,
                    ipd.[Date] AS VchDate,
                    ipd.VchType,
                    ipd.VchCode,
                    ipd.VchNo,
                    ipd.ItemCode,
                    ipd.MCCode,
                    ipd.C1 AS DesignNo,
                    ipd.C2 AS Size,
                    CAST(ipd.Value1 AS FLOAT) AS QtySign,
                    ABS(CAST(ipd.Value1 AS FLOAT)) AS QtyAbs,
                    CAST(ipd.D1 AS FLOAT) AS Rate,
                    CAST(ipd.D4 AS FLOAT) AS SalePriceAtPurchase,
                    CAST(ipd.D5 AS FLOAT) AS LineAmount,

                    -- ✅ FIXED PARTY PICK
                    CASE
                        WHEN ipd.VchType = 2 THEN party1.PrintName  -- PURCHASE: always supplier (Party1)

                        WHEN ipd.VchType = 9 THEN                   -- SALE: prefer Party2 if not internal
                            CASE
                                WHEN party2.PrintName IS NOT NULL
                                     AND party2.PrintName NOT LIKE '%WS%'
                                     AND party2.PrintName NOT LIKE 'SSHN%'
                                     AND party2.PrintName NOT LIKE 'SSHR%'
                                     AND party2.PrintName NOT LIKE 'MAIN%'
                                THEN party2.PrintName
                                ELSE party1.PrintName
                            END

                        ELSE party1.PrintName
                    END AS PartyName

                FROM {db}.dbo.ItemParamDet ipd
                LEFT JOIN {db}.dbo.Tran1 t1
                    ON t1.VchCode = ipd.VchCode AND t1.VchType = ipd.VchType
                LEFT JOIN {db}.dbo.Master1 party2
                    ON party2.Code = t1.MasterCode2
                LEFT JOIN {db}.dbo.Master1 party1
                    ON party1.Code = t1.MasterCode1
                WHERE ipd.BCN = ?
                  AND ISNULL(ipd.VchCode,0) <> 0
                """

                for rr in cur.execute(sql, barcode).fetchall():
                    rows.append({
                        "DbName": rr.DbName,
                        "VchDate": rr.VchDate,
                        "VchType": int(rr.VchType or 0),
                        "VchCode": int(rr.VchCode or 0),
                        "VchNo": str(rr.VchNo or ""),
                        "ItemCode": int(rr.ItemCode or 0),
                        "MCCode": int(rr.MCCode or 0),
                        "DesignNo": str(rr.DesignNo or ""),
                        "Size": str(rr.Size or ""),
                        "QtySign": _safe_float(rr.QtySign),
                        "QtyAbs": _safe_float(rr.QtyAbs),
                        "Rate": _safe_float(rr.Rate),
                        "SalePriceAtPurchase": _safe_float(rr.SalePriceAtPurchase),
                        "LineAmount": _safe_float(rr.LineAmount),
                        "PartyName": str(rr.PartyName or "").strip(),
                    })

            except Exception as e:
                app.logger.warning("[party-scan] skip db %s due to %s", db, e)
                continue

    if not rows:
        return {
            "purchase_party_alias": "",
            "sold_parties": [],
            "total_purchase_qty": 0,
            "total_sold_qty": 0
        }

    # 3) deduplicate similar to your SQL
    seen = set()
    deduped = []
    for r in sorted(rows, key=lambda x: (x["DbName"], x["VchDate"], x["VchCode"])):
        k = (
            r["VchType"], r["VchCode"], r["VchNo"],
            r["ItemCode"], r["MCCode"], r["VchDate"],
            round(r["QtySign"], 6), round(r["Rate"], 6), round(r["SalePriceAtPurchase"], 6)
        )
        if k in seen:
            continue
        seen.add(k)
        deduped.append(r)

    purch = [r for r in deduped if r["VchType"] == VCH_PURCHASE and r["QtySign"] > 0]
    sale  = [r for r in deduped if r["VchType"] == VCH_SALE and r["QtySign"] < 0]

    total_purchase_qty = sum(r["QtyAbs"] for r in purch)
    total_sold_qty     = sum(r["QtyAbs"] for r in sale)

    # latest purchase party (PurchasedFrom)
    purchase_party_alias = ""
    if purch:
        purch_sorted = sorted(purch, key=lambda r: (r["VchDate"], r["DbName"], r["VchCode"]), reverse=True)
        purchase_party_alias = (purch_sorted[0]["PartyName"] or "").strip()

    # sold parties list (unique, newest first)
    sold_parties = []
    if sale:
        sale_sorted = sorted(sale, key=lambda r: (r["VchDate"], r["DbName"], r["VchCode"]), reverse=True)
        seenp = set()
        for r in sale_sorted:
            nm = (r["PartyName"] or "").strip()
            if not nm:
                continue
            if nm in seenp:
                continue
            seenp.add(nm)
            sold_parties.append(nm)

    return {
        "purchase_party_alias": purchase_party_alias,
        "sold_parties": sold_parties,
        "total_purchase_qty": total_purchase_qty,
        "total_sold_qty": total_sold_qty
    }

# ---------- Sales window recipients (Salesman + Helper) ----------
def fetch_party_summary_itemparamdet_all_dbs(barcode: str) -> dict:
    """
    Uses ItemParamDet (BCN) across all BusyComp0001* databases to get:
      - purchase_party_alias (latest purchase party)
      - sold_parties (distinct sale parties)
      - total_purchase_qty (sum qty where VchType=2 and QtySign>0)
      - total_sold_qty (sum abs qty where VchType=9 and QtySign<0)

    SQL Server 2012 compatible.
    """
    VCH_PURCHASE = 2
    VCH_SALE = 9

    party = {
        "purchase_party_alias": "",
        "sold_parties": [],
        "total_purchase_qty": 0.0,
        "total_sold_qty": 0.0
    }

    try:
        with pyodbc.connect(SQL_CONN_STR) as conn:
            cur = conn.cursor()

            sql = r"""
            DECLARE @BCN NVARCHAR(100) = ?;
            DECLARE @VCH_PURCHASE INT = 2;
            DECLARE @VCH_SALE INT = 9;

            IF OBJECT_ID('tempdb..#JAll') IS NOT NULL DROP TABLE #JAll;

            CREATE TABLE #JAll (
                DbName SYSNAME,
                VchDate DATETIME,
                VchType INT,
                VchCode INT,
                VchNo NVARCHAR(50),
                QtySign FLOAT,
                QtyAbs FLOAT,
                PartyName NVARCHAR(255)
            );

            DECLARE @db SYSNAME, @sql2 NVARCHAR(MAX);

            DECLARE dbcur CURSOR FAST_FORWARD FOR
            SELECT name
            FROM sys.databases
            WHERE name LIKE 'BusyComp0001%'
              AND state_desc='ONLINE';

            OPEN dbcur;
            FETCH NEXT FROM dbcur INTO @db;

            WHILE @@FETCH_STATUS = 0
            BEGIN
                SET @sql2 = N'
                IF EXISTS (
                    SELECT 1 FROM ' + QUOTENAME(@db) + N'.sys.tables WHERE name = ''ItemParamDet''
                )
                BEGIN
                    INSERT INTO #JAll (DbName, VchDate, VchType, VchCode, VchNo, QtySign, QtyAbs, PartyName)
                    SELECT
                        N''' + REPLACE(@db,'''','''''') + N''' AS DbName,
                        ipd.[Date] AS VchDate,
                        ipd.VchType,
                        ipd.VchCode,
                        ipd.VchNo,
                        CAST(ipd.Value1 AS FLOAT) AS QtySign,
                        ABS(CAST(ipd.Value1 AS FLOAT)) AS QtyAbs,

                        CASE
                            WHEN party2.PrintName IS NOT NULL
                                 AND party2.PrintName NOT LIKE ''%WS%''
                                 AND party2.PrintName NOT LIKE ''SSHN%''
                                 AND party2.PrintName NOT LIKE ''MAIN%''
                            THEN party2.PrintName
                            ELSE party1.PrintName
                        END AS PartyName

                    FROM ' + QUOTENAME(@db) + N'.dbo.ItemParamDet ipd
                    LEFT JOIN ' + QUOTENAME(@db) + N'.dbo.Tran1 t1
                        ON t1.VchCode = ipd.VchCode AND t1.VchType = ipd.VchType
                    LEFT JOIN ' + QUOTENAME(@db) + N'.dbo.Master1 party2
                        ON party2.Code = t1.MasterCode2
                    LEFT JOIN ' + QUOTENAME(@db) + N'.dbo.Master1 party1
                        ON party1.Code = t1.MasterCode1

                    WHERE ipd.BCN = @BCN
                      AND ISNULL(ipd.VchCode,0) <> 0;
                END;';

                EXEC sp_executesql @sql2, N'@BCN NVARCHAR(100)', @BCN=@BCN;
                FETCH NEXT FROM dbcur INTO @db;
            END

            CLOSE dbcur;
            DEALLOCATE dbcur;

            -- total purchase qty
            SELECT COALESCE(SUM(QtyAbs),0) AS TotalPurchaseQty
            FROM #JAll
            WHERE VchType=@VCH_PURCHASE AND QtySign>0;

            -- total sold qty
            SELECT COALESCE(SUM(QtyAbs),0) AS TotalSoldQty
            FROM #JAll
            WHERE VchType=@VCH_SALE AND QtySign<0;

            -- latest purchase party alias
            SELECT TOP 1 COALESCE(LTRIM(RTRIM(PartyName)),'') AS PurchasePartyAlias
            FROM #JAll
            WHERE VchType=@VCH_PURCHASE AND QtySign>0
            ORDER BY VchDate DESC, DbName DESC, VchCode DESC;

            -- sold parties (distinct)
            SELECT DISTINCT COALESCE(LTRIM(RTRIM(PartyName)),'') AS SoldParty
            FROM #JAll
            WHERE VchType=@VCH_SALE AND QtySign<0
              AND COALESCE(LTRIM(RTRIM(PartyName)),'') <> ''
            ORDER BY SoldParty;
            """

            # ✅ pyodbc gives multiple resultsets - fetch in order
            cur.execute(sql, barcode)

            # RS1: TotalPurchaseQty
            r1 = cur.fetchone()
            total_purchase = float(r1[0] or 0) if r1 else 0.0

            # next resultset
            cur.nextset()
            r2 = cur.fetchone()
            total_sold = float(r2[0] or 0) if r2 else 0.0

            # next resultset
            cur.nextset()
            r3 = cur.fetchone()
            purchase_party_alias = (r3[0] or "").strip() if r3 else ""

            # next resultset
            cur.nextset()
            sold_parties = []
            rows = cur.fetchall() or []
            for rr in rows:
                nm = (rr[0] or "").strip()
                if nm:
                    sold_parties.append(nm)

            party["purchase_party_alias"] = purchase_party_alias
            party["sold_parties"] = sold_parties
            party["total_purchase_qty"] = total_purchase
            party["total_sold_qty"] = total_sold

            return party

    except Exception as e:
        app.logger.warning("[bcn_v2] fetch_party_summary_itemparamdet_all_dbs failed for %s: %s", barcode, e)
        return party

# --- Small date & json helpers ---
def _parse_any_date(s: str) -> datetime.date | None:
    s = (s or "").strip()
    for fmt in ("%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            return datetime.datetime.strptime(s, fmt).date()
        except Exception:
            pass
    return None

def _date_yyyy_mm_dd(d: datetime.date) -> str:
    return d.strftime("%Y-%m-%d")

def _safe_json_read(path: str) -> dict | None:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        app.logger.exception(f"[coupon] read json failed: {path}")
        return None

def _safe_json_write(path: str, payload: dict):
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception:
        app.logger.exception(f"[coupon] write json failed: {path}")

def _coupon_json_name(code: str, msisdn: str, expires_on: datetime.date) -> str:
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"coupon_{slugify_filename(code)}_{msisdn}_{expires_on.strftime('%Y%m%d')}_{ts}.json"

def _move_coupon_to_processed(src_path: str):
    try:
        base = os.path.basename(src_path)
        dst = os.path.join(COUPON_PROC_DIR, base)
        os.replace(src_path, dst)
        app.logger.info(f"[coupon] moved to processed: {base}")
    except Exception:
        app.logger.exception("[coupon] move failed")

# --- Record a coupon at issue-time (so we can resend later) ---
def _record_coupon_send(to_msisdn: str,
                        coupon_code: str,
                        expires_on: str,
                        template_name: str,
                        language_code: str,
                        values: list[str],
                        media_local: str | None = None,
                        meta: dict | None = None) -> bool:
    """
    Save a JSON in coupon/generated capturing all details to resend the same way.
    - expires_on can be 'dd-mm-YYYY' or 'YYYY-mm-dd'
    - media_local: pass EXPIRY_COUPON_VIDEO to force that video in reminders
    """
    try:
        d = _parse_any_date(expires_on)
        if not d:
            app.logger.error(f"[coupon] invalid expiry date: {expires_on}")
            return False
        msisdn = resolve_receiver(to_msisdn)
        if not msisdn:
            app.logger.error(f"[coupon] invalid MSISDN: {to_msisdn}")
            return False

        payload = {
            "type": "coupon",
            "to": msisdn,
            "code": str(coupon_code).strip(),
            "expires_on": _date_yyyy_mm_dd(d),
            "sent_on": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "reminders_sent": [],

            # to reconstruct the same BSP send:
            "template_name": (template_name or "").strip().lower(),
            "language_code": (language_code or "en").strip(),
            "values": list(values or []),

            # always use your reminder video for scheduled sends
            "media_local": media_local or None,

            "meta": meta or {}
        }
        fname = _coupon_json_name(coupon_code, msisdn, d)
        _safe_json_write(os.path.join(COUPON_GEN_DIR, fname), payload)
        app.logger.info(f"[coupon] recorded → {fname}")
        return True
    except Exception:
        app.logger.exception("[coupon] record failed")
        return False

# --- Enqueue a reminder using the SAME BSP template path ---
def _enqueue_coupon_reminder(rec: dict) -> bool:
    """
    Enqueue a reminder via the same BSP flow.
    Attaches header video based on template_name:
    - <template>.mp4
    - <template>.MP4
    Fallback -> EXPIRY_COUPON_VIDEO
    """
    try:
        to   = rec.get("to")
        tpl  = (rec.get("template_name") or "").strip().lower()
        lang = (rec.get("language_code") or "en").strip()
        vals = rec.get("values") or []

        CUP_VIDEO_DIR = r"C:\BusyWin\IMAGES\CUP"

        # 1) try lowercase .mp4
        video_local = os.path.join(CUP_VIDEO_DIR, f"{tpl}.mp4")

        # 2) try uppercase .MP4
        if not os.path.exists(video_local):
            video_local_alt = os.path.join(CUP_VIDEO_DIR, f"{tpl}.MP4")
            if os.path.exists(video_local_alt):
                video_local = video_local_alt

        # 3) fallback
        if not os.path.exists(video_local):
            app.logger.warning(
                f"[coupon] video not found for tpl={tpl}. "
                f"Tried {tpl}.mp4 / {tpl}.MP4, using default EXPIRY_COUPON_VIDEO"
            )
            video_local = EXPIRY_COUPON_VIDEO

        delay_sec = delay_minutes_for_template(tpl) * 60

        job_id = enqueue_send_job(
            to=to,
            template_name=tpl,
            language_code=lang,
            values=vals,
            pdf_link=None,
            image_link=None,
            video_link=None,
            delay_seconds=delay_sec,
            pdf_path=video_local   # ✅ correct video goes here
        )

        app.logger.info(
            f"[coupon] reminder queued | job_id={job_id} | to={to} | tpl={tpl} | video={video_local}"
        )
        return True

    except Exception:
        app.logger.exception("[coupon] enqueue reminder failed")
        return False

def _date_ymd(dstr: str) -> str:
    # Accept dd-mm-YYYY and return YYYY-mm-dd for SQL "date" parameters
    return datetime.datetime.strptime(dstr, "%d-%m-%Y").strftime("%Y-%m-%d")
def _load_missing_mobiles_df() -> pd.DataFrame:
    try:
        if os.path.exists(MISSING_MOBILES_FILE):
            df = pd.read_excel(MISSING_MOBILES_FILE, dtype=str).fillna("")
            # make sure required columns exist
            for col in MISSING_MOBILES_COLUMNS:
                if col not in df.columns:
                    df[col] = ""
            return df[MISSING_MOBILES_COLUMNS].copy()
    except Exception:
        app.logger.exception("failed reading uploadsalesmanmobileno.xlsx")
    return pd.DataFrame(columns=MISSING_MOBILES_COLUMNS)

def _save_missing_mobiles_df(df: pd.DataFrame):
    try:
        out = df[MISSING_MOBILES_COLUMNS].copy()
        out.to_excel(MISSING_MOBILES_FILE, index=False)
        app.logger.info(f"[mobiles] wrote {len(out)} rows → {MISSING_MOBILES_FILE}")
    except Exception:
        app.logger.exception("failed writing uploadsalesmanmobileno.xlsx")

def _upsert_missing_mobiles(rows: list[dict]):
    """
    rows items look like:
       {"role":"salesman"|"helper", "name":"...", "mobile":"", "source":"sales_window", "last_seen":"YYYY-MM-DD"}
    If (role,name) exists, update last_seen/source and mobile if provided.
    """
    if not rows:
        return
    df = _load_missing_mobiles_df()

    # index by (role, name) normalized
    idx = {(str(r).strip().lower(), str(n).strip().upper()): i
           for i, (r, n) in enumerate(zip(df["role"].fillna(""), df["name"].fillna("")))}

    updates = 0
    for r in rows:
        role = str(r.get("role","")).strip().lower()
        name = str(r.get("name","")).strip().upper()
        mobile = re.sub(r"\D", "", str(r.get("mobile","") or ""))
        source = str(r.get("source","sales_window"))
        last_seen = str(r.get("last_seen",""))

        key = (role, name)
        if key in idx:
            i = idx[key]
            if mobile:
                df.at[i, "mobile"] = mobile
            if source:
                df.at[i, "source"] = source
            if last_seen:
                df.at[i, "last_seen"] = last_seen
        else:
            df.loc[len(df)] = {
                "role": role,
                "name": name,
                "mobile": mobile,
                "source": source or "sales_window",
                "last_seen": last_seen or datetime.datetime.now().strftime("%Y-%m-%d")
            }
        updates += 1

    _save_missing_mobiles_df(df)
    app.logger.info(f"[mobiles] upserted {updates} row(s)")

def _lookup_mobile_from_upload_sheet(role: str, name: str) -> str | None:
    """Use mobile you typed into uploadsalesmanmobileno.xlsx if SQL has NULL."""
    try:
        df = _load_missing_mobiles_df()
        hit = df[(df["role"].str.lower()==str(role).lower()) &
                 (df["name"].str.upper()==str(name).upper())]
        if not hit.empty:
            m = re.sub(r"\D","", str(hit.iloc[0]["mobile"] or ""))
            if len(m) == 10:
                return "91" + m
            if len(m) >= 11:
                return m
    except Exception:
        app.logger.exception("lookup from uploadsalesmanmobileno failed")
    return None
def _normalize_name(n: str) -> str:
    n = (n or "").strip()
    # keep original casing for readability but collapse spaces
    n = re.sub(r"\s+", " ", n)
    return n

def _normalize_mobile_db(m: str) -> str:
    # digits only; if 10-digit, add 91; else leave as-is (some DBs store with 91 already)
    s = re.sub(r"\D", "", str(m or ""))
    if len(s) == 10:
        return "91" + s
    return s

def rebuild_salesman_file(window_days: int = 45, vch_type: int = 9):
    """
    Build C:\BusyWin\AI BOT\BUSY_RECEIPT_BOT_STRUCTURE\salesman_list.xlsx
    by scanning the last `window_days` of SALES vouchers and extracting
    Salesman (from BrokerCode) names + mobiles from DB.
    - If mobile missing in DB, try your upload sheet (uploadsalesmanmobileno.xlsx).
    - Deduplicate by NAME (keep the first non-empty mobile).
    - Only two columns are written: name, mobile (as your lookup expects).
    """
    try:
        # Date window
        today = datetime.datetime.now().date()
        since = today - datetime.timedelta(days=int(window_days))
        since_iso = since.strftime("%Y-%m-%d")
        until_iso = (today + datetime.timedelta(days=1)).strftime("%Y-%m-%d")

        SQL = f"""
        WITH Salesman AS (
          SELECT 
              m.Code   AS SalesmanCode,
              LTRIM(RTRIM(m.Name)) AS SalesmanName,
              m.Alias,
              mai.Mobile
          FROM dbo.Master1 m
          LEFT JOIN dbo.MasterAddressInfo mai ON mai.MasterCode = m.Code
          WHERE m.MasterType = 19
        )
        SELECT DISTINCT
            sm.SalesmanName,
            sm.Mobile AS SalesmanMobile
        FROM dbo.Tran1 t1
        JOIN dbo.TmpTran2 t2
              ON t2.Vchcode = t1.VchCode
        LEFT JOIN Salesman sm
              ON sm.SalesmanCode = t2.BrokerCode
        WHERE t1.Date >= '{since_iso}'
          AND t1.Date <  '{until_iso}'
          AND t1.VchType = {int(vch_type)}
          AND ISNULL(LTRIM(RTRIM(sm.SalesmanName)),'') <> ''
        """

        # Pull rows
        names_to_mobile: dict[str, str] = {}
        missing_rows_for_lookup = []  # in case you want to inspect later

        with pyodbc.connect(SQL_CONN_STR) as conn:
            cur = conn.cursor()
            cur.execute(SQL)
            for r in cur.fetchall():
                # r[0]=SalesmanName, r[1]=SalesmanMobile
                name = _normalize_name(r[0] if hasattr(r, "SalesmanName") else r[0])
                mobile = _normalize_mobile_db(r[1] if hasattr(r, "SalesmanMobile") else r[1])

                if not name:
                    continue

                # If DB mobile missing, try from your manual sheet (if you’ve populated it)
                if not mobile:
                    looked = _lookup_mobile_from_upload_sheet("salesman", name)
                    if looked:
                        mobile = looked

                # Keep the first non-empty mobile we see for a given name
                key = name.upper()
                if key not in names_to_mobile:
                    names_to_mobile[key] = mobile or ""
                else:
                    # If we already have blank but this one has a mobile, upgrade it
                    if not names_to_mobile[key] and mobile:
                        names_to_mobile[key] = mobile

                if not mobile:
                    missing_rows_for_lookup.append(name)

        # Build DataFrame: only name, mobile (as your lookup_salesman expects)
        rows = []
        for key_upper, mob in names_to_mobile.items():
            rows.append({
                "name": key_upper.title(),   # pretty case for reading
                "mobile": mob
            })

        # Sort by name
        df = pd.DataFrame(rows, columns=["name", "mobile"])
        if not df.empty:
            df = df.sort_values("name")

        # Ensure target folder exists
        os.makedirs(os.path.dirname(SALESMAN_FILE), exist_ok=True)
        df.to_excel(SALESMAN_FILE, index=False)

        app.logger.info(f"[rebuild_salesman_file] wrote {len(df)} rows to {SALESMAN_FILE}")
        if missing_rows_for_lookup:
            app.logger.info(f"[rebuild_salesman_file] {len(missing_rows_for_lookup)} names missing mobiles (consider filling in uploadsalesmanmobileno.xlsx)")
        return True, len(df)
    except Exception as e:
        app.logger.exception("rebuild_salesman_file failed")
        return False, 0

def get_active_recipients(from_date_ddmmyyyy: str, to_date_ddmmyyyy: str, vch_type: int = 9) -> list[dict]:
    """
    Returns unique recipients from the given date window:
      - every Salesman who appears in vouchers (name+mobile)
      - every Helper (name+mobile) whose name is NOT in the Salesman list
    Output: [{kind:'salesman'|'helper', name:'...', mobile:'91XXXXXXXXXX'}]
    """
    fd = _date_ymd(from_date_ddmmyyyy)
    td = _date_ymd(to_date_ddmmyyyy)

    SQL = """
    WITH Salesman AS (
      SELECT 
          m.Code   AS SalesmanCode,
          m.Name   AS SalesmanName,
          m.Alias,
          mai.Mobile
      FROM dbo.Master1 m
      LEFT JOIN dbo.MasterAddressInfo mai ON mai.MasterCode = m.Code
      WHERE m.MasterType = 19
    )
    SELECT 
        t1.Date,
        t1.VchCode,
        t1.VchNo,
        t1.VchType,
        t2.BrokerCode                      AS SalesmanCode,
        sm.SalesmanName,
        sm.Mobile                          AS SalesmanMobile,
        t2.HelperName,
        hm.Mobile                          AS HelperMobile,
        t2.H1, t2.H2, t2.P1, t2.P_Mode, t2.UserName
    FROM dbo.Tran1 AS t1
    JOIN dbo.TmpTran2 AS t2
          ON t2.Vchcode = t1.VchCode
    LEFT JOIN Salesman sm
           ON sm.SalesmanCode = t2.BrokerCode
    LEFT JOIN Salesman hm
           ON LTRIM(RTRIM(hm.SalesmanName)) = LTRIM(RTRIM(t2.HelperName))
    WHERE t1.Date >= ? AND t1.Date < DATEADD(day, 1, ?)
      AND t1.VchType = ?
    """

    def _nm(s):  return (s or "").strip()
    def _norm_mob(m):
        m = re.sub(r"\D", "", str(m or ""))
        return ("91"+m) if len(m) == 10 else m

    try:
        out = []
        with pyodbc.connect(SQL_CONN_STR) as conn:
            cur = conn.cursor()
            cur.execute(SQL, (fd, td, int(vch_type)))
            rows = cur.fetchall()

        # sets for de-dupe
        salesmen_by_name = set()
        sales_seen = set()
        helper_seen = set()

        # 1) collect salesmen (unique by mobile if present, else by name)
        for r in rows:
            sm_name   = _nm(r.SalesmanName) if hasattr(r, "SalesmanName") else _nm(r[5])
            sm_mobile = _norm_mob(r.SalesmanMobile if hasattr(r, "SalesmanMobile") else r[6])
            if sm_name and sm_mobile:
                key = ("s", sm_mobile)
                if key not in sales_seen:
                    sales_seen.add(key)
                    salesmen_by_name.add(sm_name.upper())
                    out.append({"kind": "salesman", "name": sm_name, "mobile": sm_mobile})

        # 2) collect helpers NOT in salesman list (match by name; skip if no mobile)
        for r in rows:
            hp_name   = _nm(r.HelperName if hasattr(r, "HelperName") else r[7])
            hp_mobile = _norm_mob(r.HelperMobile if hasattr(r, "HelperMobile") else r[8])
            if not hp_name or not hp_mobile:
                continue
            if hp_name.upper() in salesmen_by_name:
                continue   # helper is actually a salesman → skip
            key = ("h", hp_mobile)
            if key not in helper_seen:
                helper_seen.add(key)
                out.append({"kind": "helper", "name": hp_name, "mobile": hp_mobile})

        return out
    except Exception:
        app.logger.exception("get_active_recipients failed")
        return []
# ============================================================
# BARCODE LOOKUP V2 (SQL-based) — Safe Add-on (no old edits)
# ============================================================

def _sql_has_column(conn, table_name: str, col_name: str) -> bool:
    """
    Returns True if dbo.<table_name> has <col_name>.
    This lets us write adaptive queries without breaking if schema differs.
    """
    try:
        sql = """
        SELECT 1
        FROM sys.columns c
        JOIN sys.objects o ON o.object_id = c.object_id
        JOIN sys.schemas s ON s.schema_id = o.schema_id
        WHERE s.name = 'dbo'
          AND o.name = ?
          AND c.name = ?
        """
        cur = conn.cursor()
        cur.execute(sql, (table_name, col_name))
        return cur.fetchone() is not None
    except Exception:
        app.logger.exception("[bcn_v2] _sql_has_column failed")
        return False


def _mc_from_prefix(prefix: str) -> str:
    """
    Same logic style as your collection code:
    M->MAIN, S->SSHN, R->SSHR, P->PALLY, T->TALLY, else OTHER
    """
    p = (prefix or "").strip().upper()[:1]
    return {
        "M": "MAIN",
        "S": "SSHN",
        "R": "SSHR",
        "P": "PALLY",
        "T": "TALLY",
    }.get(p, "OTHER")


def _derive_mc_from_any_code(code_str: str) -> str:
    """
    If we have any voucher/code string like VchNo etc, derive MC from first letter.
    """
    return _mc_from_prefix((code_str or "").strip())


def _get_local_media_items_for_barcode(barcode: str) -> list[dict]:
    """
    Reuse the SAME folder logic as /api/bcn_media but as a local helper
    so barcode_lookup_v2 can return images list without doing HTTP.
    """
    barcode = _clean_barcode(barcode or "")
    if not barcode:
        return []

    items = []
    prefix = barcode + "."
    prefix2 = barcode + "_"

    for fn in os.listdir(BCN_MEDIA_DIR):
        fn_low = fn.lower()
        full = os.path.join(BCN_MEDIA_DIR, fn)

        if not os.path.isfile(full):
            continue
        if fn_low == "_thumbs":
            continue

        if not (fn.startswith(prefix) or fn.startswith(prefix2)):
            continue

        ext = os.path.splitext(fn_low)[1]
        if ext not in ALLOWED_EXT:
            continue

        mtype = _media_type_from_ext(ext)

        # Build URLs same as your /api/bcn_media
        url_full = request.host_url.rstrip("/") + "/bcn_image/" + fn

        thumb_url = None
        if mtype == "image":
            thumb_fn = _ensure_image_thumb(full, fn)
            if thumb_fn:
                thumb_url = request.host_url.rstrip("/") + "/bcn_thumb/" + thumb_fn

        items.append({
            "name": fn,
            "type": mtype,
            "url": url_full,
            "thumb_url": thumb_url
        })

    # Sort: base first, then _1, _2...
    def sort_key(x):
        n = x["name"]
        m = re.match(rf"^{re.escape(barcode)}(?:_(\d+))?\.", n)
        idx = int(m.group(1)) if (m and m.group(1)) else 0
        return (idx, n.lower())

    items.sort(key=sort_key)
    return items

def fetch_bcn_block1_sql(barcode: str) -> dict:
    out = {
        "barcode": barcode,
        "design_no": "",
        "size": "",
        "mc": "",
        "purchase_code": "",   # BLACKWHITE
        "sale_price": None,
        "stock_qty": 0
    }

    try:
        bc = barcode.strip()

        sql = """
        SELECT
            ipd.BCN AS BarCode,
            MAX(ipd.C1) AS DesignNo,
            MAX(ipd.C2) AS Size,
            MAX(ipd.D4) AS SalePrice,
            SUM(ipd.Value1) AS StockQty,
            MAX(mc.PrintName) AS MCName,

            -- 🔥 BLACKWHITE purchase code
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                CONVERT(varchar(20), TRY_CAST(MAX(ipd.D1) AS int)),
                '1','B'),'2','L'),'3','A'),'4','C'),'5','K'),
                '6','W'),'7','H'),'8','I'),'9','T'),'0','E'
            ) AS PurchaseBW
        FROM dbo.ItemParamDet ipd
        LEFT JOIN dbo.Master1 mc ON mc.Code = ipd.MCCode
        WHERE ipd.BCN = ?
        GROUP BY ipd.BCN
        """

        with pyodbc.connect(SQL_CONN_STR) as conn:
            cur = conn.cursor()
            cur.execute(sql, bc)
            r = cur.fetchone()

        if not r:
            return out

        out.update({
            "barcode": str(r.BarCode),
            "design_no": r.DesignNo or "",
            "size": r.Size or "",
            "mc": r.MCName or "",
            "purchase_code": r.PurchaseBW or "",   # ✅ BLACKWHITE ONLY
            "sale_price": float(r.SalePrice) if r.SalePrice is not None else None,
            "stock_qty": float(r.StockQty or 0)
        })

        return out

    except Exception:
        app.logger.exception("[bcn] block1 failed")
        return out

def fetch_design_block2_sql(design_no: str) -> list[dict]:
    try:
        dno = design_no.strip()

        sql = """
        SELECT
            ipd.BCN AS BarCode,
            ipd.C1 AS DesignNo,
            ipd.C2 AS Size,
            MAX(ipd.D4) AS SalePrice,
            SUM(ipd.Value1) AS StockQty,
            MAX(mc.PrintName) AS MCName,

            -- 🔥 BLACKWHITE purchase code
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                CONVERT(varchar(20), TRY_CAST(MAX(ipd.D1) AS int)),
                '1','B'),'2','L'),'3','A'),'4','C'),'5','K'),
                '6','W'),'7','H'),'8','I'),'9','T'),'0','E'
            ) AS PurchaseBW
        FROM dbo.ItemParamDet ipd
        LEFT JOIN dbo.Master1 mc ON mc.Code = ipd.MCCode
        WHERE ipd.C1 = ?
        GROUP BY ipd.BCN, ipd.C1, ipd.C2
        HAVING SUM(ipd.Value1) <> 0
        ORDER BY TRY_CAST(ipd.C2 AS int), ipd.C2, ipd.BCN
        """

        with pyodbc.connect(SQL_CONN_STR) as conn:
            cur = conn.cursor()
            cur.execute(sql, dno)
            rows = cur.fetchall()

        out = []
        for r in rows:
            out.append({
                "barcode": str(r.BarCode),
                "design_no": r.DesignNo or "",
                "size": r.Size or "",
                "mc": r.MCName or "",
                "purchase_code": r.PurchaseBW or "",  # ✅ BLACKWHITE
                "sale_price": float(r.SalePrice) if r.SalePrice is not None else None,
                "stock_qty": float(r.StockQty or 0)
            })

        return out

    except Exception:
        app.logger.exception("[bcn] block2 failed")
        return []

# -------------------------------------------------------
# ROUTES (existing + WhatsApp)
# -------------------------------------------------------
@app.get("/api/barcode_lookup_v2")
def api_barcode_lookup_v2():
    """
    Android safe API:
      /api/barcode_lookup_v2?barcode=701701

    Returns:
      {
        ok: true/false,
        barcode: "...",
        block1: {...},
        block2: [ ... ],          # ✅ other sizes ONLY (main barcode removed)
        media: {count:int, items:[...]},   # ✅ ALL images/videos for MAIN barcode
        party: {...}
      }
    """
    try:
        barcode = _clean_barcode(request.args.get("barcode") or "")
        if not barcode:
            return jsonify({"ok": False, "error": "missing_barcode"}), 400

        def clean_code(x):
            if x is None:
                return ""
            s = str(x).strip()
            if s.endswith(".0"):
                s = s[:-2]
            return s

        # -------------------
        # BLOCK 1 (MAIN BCN)
        # -------------------
        b1 = fetch_bcn_block1_sql(barcode) or {}
        if not b1:
            return jsonify({"ok": False, "error": "not_found"}), 200

        b1["purchase_code"] = clean_code(b1.get("purchase_code"))

        # -------------------
        # PARTY SUMMARY for MAIN barcode
        # -------------------
        party = fetch_purchase_sale_summary_by_barcode(barcode) or {
            "purchase_party_alias": "",
            "sold_parties": [],
            "total_purchase_qty": 0,
            "total_sold_qty": 0
        }

        # Force block1 party to be real
        b1["purchase_party_alias"] = clean_code(party.get("purchase_party_alias", ""))
        b1["sold_parties"] = party.get("sold_parties", []) or []
        b1["total_purchase_qty"] = party.get("total_purchase_qty", 0) or 0
        b1["total_sold_qty"] = party.get("total_sold_qty", 0) or 0

        party["purchase_party_alias"] = b1["purchase_party_alias"]
        party["sold_parties"] = b1["sold_parties"]
        party["total_purchase_qty"] = b1["total_purchase_qty"]
        party["total_sold_qty"] = b1["total_sold_qty"]

        # -------------------
        # BLOCK 2 (OTHER SIZES)
        # -------------------
        design_no = (b1.get("design_no") or "").strip()
        b2_rows = []

        if design_no:
            raw_b2 = fetch_design_block2_sql(design_no)

            if isinstance(raw_b2, dict):
                b2_rows = raw_b2.get("rows") or []
            elif isinstance(raw_b2, list):
                b2_rows = raw_b2
            else:
                b2_rows = []

            # ✅ CLEANUP + REMOVE MAIN BARCODE FROM BLOCK2
            cleaned = []
            for r in b2_rows:
                if not isinstance(r, dict):
                    continue

                r["purchase_code"] = clean_code(r.get("purchase_code"))

                bcn2 = _clean_barcode(r.get("barcode") or "")
                if not bcn2:
                    continue

                # ✅ IMPORTANT: don't show searched barcode in "Other Sizes"
                if bcn2 == barcode:
                    continue

                r["barcode"] = bcn2
                cleaned.append(r)

            b2_rows = cleaned

        # ✅ Add PurchasedFrom / Sold Parties per BCN in OTHER SIZES
        ps_cache = {}
        for r in b2_rows:
            bcn2 = r.get("barcode", "")
            if not bcn2:
                continue

            if bcn2 not in ps_cache:
                ps_cache[bcn2] = fetch_purchase_sale_summary_by_barcode(bcn2) or {}

            ps = ps_cache[bcn2]
            r["purchase_party_alias"] = clean_code(ps.get("purchase_party_alias", ""))
            r["sold_parties"] = ps.get("sold_parties", []) or []
            r["total_purchase_qty"] = ps.get("total_purchase_qty", 0) or 0
            r["total_sold_qty"] = ps.get("total_sold_qty", 0) or 0

        # ✅ FILTER OTHER SIZES:
        # show ONLY rows where purchase party matches MAIN purchase party
        main_purchase_party = clean_code(party.get("purchase_party_alias", ""))
        if main_purchase_party:
            b2_rows = [
                r for r in b2_rows
                if clean_code(r.get("purchase_party_alias", "")) == main_purchase_party
            ]
        else:
            b2_rows = []

        # -------------------
        # MEDIA ✅ ALL FILES FOR MAIN BARCODE (images + videos)
        # folder: BCN_MEDIA_DIR
        # thumbs: BCN_THUMBS_DIR (same filename if thumb exists)
        # URLs: /bcn_image/<fname> and /bcn_thumb/<fname>
        # -------------------
        media_items = []
        exts_img = {".jpg", ".jpeg", ".png", ".webp"}
        exts_vid = {".mp4", ".3gp", ".mkv", ".mov", ".avi"}

        try:
            if os.path.isdir(BCN_MEDIA_DIR):
                for name in sorted(os.listdir(BCN_MEDIA_DIR)):
                    # ✅ IMPORTANT: include ALL variants like:
                    # 701701.jpg, 701701_1.jpg, 701701(2).mp4 etc.
                    if not name.lower().startswith(barcode.lower()):
                        continue

                    full_path = os.path.join(BCN_MEDIA_DIR, name)
                    if not os.path.isfile(full_path):
                        continue

                    ext = os.path.splitext(name)[1].lower()
                    if ext in exts_img:
                        ftype = "image"
                    elif ext in exts_vid:
                        ftype = "video"
                    else:
                        continue  # ignore unknown files

                    thumb_path = os.path.join(BCN_THUMBS_DIR, name)
                    thumb_url = (request.host_url.rstrip("/") + "/bcn_thumb/" + name) if os.path.exists(thumb_path) else ""

                    media_items.append({
                        "name": name,
                        "type": ftype,
                        "url": request.host_url.rstrip("/") + "/bcn_image/" + name,
                        "thumb_url": thumb_url
                    })

        except Exception:
            app.logger.exception("[barcode_lookup_v2] media scan failed")

        media = {"count": len(media_items), "items": media_items}

        return jsonify({
            "ok": True,
            "barcode": barcode,
            "block1": b1,
            "block2": b2_rows,
            "media": media,
            "party": party
        }), 200

    except Exception as e:
        app.logger.exception("barcode_lookup_v2 error: %s", e)
        return jsonify({"ok": False, "error": "server_error"}), 500

@app.route('/barcode_lookup', methods=['POST'])
def barcode_lookup():
    try:
        data = request.json
        barcode = (data or {}).get("barcode", "").strip()
        if not barcode:
            return jsonify({"error": "Barcode missing"}), 400
        if not os.path.exists(STOCK_FILE):
            return jsonify({"error": "Stock file not found"}), 500
        df = pd.read_excel(STOCK_FILE, dtype=str).fillna("")
        matching = df[df['BCN'].astype(str).str.strip() == barcode]
        if matching.empty:
            return jsonify({"status": "not_found", "barcode": barcode})
        results = []
        for _, row in matching.iterrows():
            try:
                cl_qty = float(row.get("Cl. Qty.", "0") or "0")
            except Exception:
                cl_qty = 0.0
            result = {
                "item_name": row.get("Item Details", ""),
                "store_name": row.get("Material Centre", ""),
                "barcode": row.get("BCN", ""),
                "design_no": row.get("P1( DESIGN NO. )", ""),
                "size": row.get("P2( SIZE )", ""),
                "sales_price": f"₹{row.get('Sales Price', '0')}",
                "stock_qty": int(cl_qty) if cl_qty == int(cl_qty) else cl_qty,
                "stock_status": "Out of Stock" if cl_qty == 0 else "In Stock"
            }
            results.append(result)
        return jsonify({"status": "found", "barcode": barcode, "items": results})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
@app.route('/trigger/points_expiry', methods=['GET'])
def trigger_points_expiry():
    try:
        d = request.args.get("days", "").strip()
        if d:
            d = int(d)
            send_points_expiry_reminders(d)
            return jsonify({"ok": True, "triggered_for_days": d})

        # default → run both
        send_points_expiry_reminders(10)
        send_points_expiry_reminders(5)

        return jsonify({"ok": True, "triggered_for_days": [10, 5]})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route('/trigger/sync_contacts', methods=['GET'])
def trigger_sync_contacts():
    try:
        python_exe = sys.executable
        subprocess.Popen([python_exe, "sync_google_contacts.py"], cwd=r"C:\BusyWin\AI BOT\BUSY_RECEIPT_BOT_STRUCTURE")
        return jsonify({"status": "sync started"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/salesmen', methods=['GET'])
def get_salesmen():
    try:
        if not os.path.exists(SALESMAN_FILE):
            return jsonify([])
        df = pd.read_excel(SALESMAN_FILE, dtype=str).fillna("")
        if "name" in df.columns:
            df = df.sort_values("name")
        salesmen = [{"name": row.get("name", "").strip(), "mobile": row.get("mobile", "").strip()} for _, row in df.iterrows()]
        return jsonify(salesmen)
    except Exception as e:
        return jsonify({"error": f"Failed to read salesmen list: {e}"}), 500

@app.route('/register_customer', methods=['POST'])
def register_customer():
    try:
        data = request.json or {}
        name = data.get("NAME", "").strip().upper()
        mobile = str(data.get("MOBILE NUMBER", "")).strip()
        email = data.get("EMAIL ID", "").strip()
        dob = data.get("BIRTHDATE", "").strip()
        anniv = data.get("ANNIVERSARY", "").strip()
        city = data.get("CITY", "").strip()
        if not mobile:
            return jsonify({"error": "Mobile number is required"}), 400
        conn = pyodbc.connect(SQL_CONN_STR); cursor = conn.cursor()
        cursor.execute("SELECT PrintName FROM dbo.Master1 WHERE Alias = ?", mobile)
        row = cursor.fetchone()
        if row:
            name = row.PrintName or name
            status = "already_registered"
        else:
            status = "new"
        now = datetime.datetime.now().strftime("%d-%m-%Y_%H%M%S")
        filename = f"register_customer_{mobile}_{now}.json"
        filepath = os.path.join(QUEUE_DIR, filename)
        with open(filepath, 'w') as f:
            json.dump({"type": "register_customer","name": name,"mobile": mobile,"email": email,"dob": dob,"anniversary": anniv,"city": city}, f)
        if status == "new":
            cursor.execute("INSERT INTO dbo.Master1 (Alias) VALUES (?)", mobile); conn.commit()
        conn.close()
        return jsonify({"status": status, "file": filename}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/submit', methods=['POST'])
def submit():
    data = request.json or {}
    receipt_type = data.get("type", "unknown")
    now = datetime.datetime.now().strftime("%d-%m-%Y_%H%M%S")
    filename = f"{receipt_type}_{data.get('mobile', 'unknown')}_{now}.json"
    filepath = os.path.join(QUEUE_DIR, filename)
    try:
        with open(filepath, 'w') as f: json.dump(data, f)
        return jsonify({"status": "success", "file": filename})
    except Exception as e:
        return jsonify({"error": f"Failed to write file: {e}"}), 500

@app.route('/customer')
def lookup():
    mobile = request.args.get("mobile", "").strip()
    if not mobile:
        return jsonify({"error": "Missing mobile number"}), 400
    dues = load_customer_dues()
    customer = dues.get(mobile[-10:])
    if customer: return jsonify(customer)
    else: return jsonify({"error": "Customer not found"}), 404

@app.route('/salesman', methods=['POST'])
def salesman_report():
    data = request.json or {}
    name = data.get("name", "").strip()
    from_date = data.get("from_date") or data.get("from")
    to_date   = data.get("to_date")   or data.get("to")
    
    if not name or not from_date or not to_date:
        return jsonify({"error": "Missing fields"}), 400
    mobile = lookup_salesman(name)
    if not mobile:
        return jsonify({"error": "Salesman name not found"}), 404
    now = datetime.datetime.now().strftime("%d-%m-%Y_%H%M%S")
    filename = f"salesman_{name}_{now}.json"
    filepath = os.path.join(QUEUE_DIR, filename)
    try:
        report_data = {"type":"salesman","name":name,"mobile":mobile,"from_date":from_date,"to_date":to_date,"date": datetime.datetime.now().strftime("%d-%m-%Y")}
        with open(filepath, 'w') as f: json.dump(report_data, f)
        return jsonify({"status": "success", "file": filename})
    except Exception as e:
        return jsonify({"error": f"Failed to create report file: {e}"}), 500

@app.route('/submit_daily_cash', methods=['POST'])
def submit_daily_cash():
    try:
        data = request.json or {}
        selected_date = data.get("date")
        if not selected_date:
            return jsonify({"error": "Date missing"}), 400
        filename = f"whatsapp_dailycash_{selected_date}_{datetime.datetime.now().strftime('%H%M%S')}.json"
        filepath = os.path.join(QUEUE_DIR, filename)
        with open(filepath, 'w') as f: json.dump({"type":"whatsapp","category":"dailycash","date":selected_date}, f)
        return jsonify({"status": "success", "file": filename})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/submit_daybook', methods=['POST'])
def submit_daybook():
    try:
        data = request.json or {}
        selected_date = data.get("date")
        if not selected_date:
            return jsonify({"error": "Date missing"}), 400
        filename = f"daybook_{selected_date}_{datetime.datetime.now().strftime('%H%M%S')}.json"
        filepath = os.path.join(QUEUE_DIR, filename)
        with open(filepath, 'w') as f: json.dump({"type":"daybook","date":selected_date}, f)
        return jsonify({"status": "success", "file": filename})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/pending_gr', methods=['POST'])
def pending_gr():
    try:
        now = datetime.datetime.now().strftime("%d-%m-%Y_%H%M%S")
        filename = f"pending_gr_{now}.json"
        filepath = os.path.join(QUEUE_DIR, filename)
        with open(filepath, 'w') as f: json.dump({"type":"pending_gr","timestamp":now}, f)
        return jsonify({"status": "success", "file": filename})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
from werkzeug.exceptions import ClientDisconnected, RequestEntityTooLarge, BadRequest

# Put this once near app init (top-level), not inside the function:
# app.config["MAX_CONTENT_LENGTH"] = 80 * 1024 * 1024  # 80 MB (adjust)

@app.route("/upload_document", methods=["POST"])
def upload_document():
    """
    Upload 1 or more images/PDFs for a voucher and store them as a single
    merged PDF in dbo.Images (Type=2, Code=VchCode).

    Expected form:
      - base_name = voucher number (e.g. 'RP213')  [optional but recommended]
      - files     = one or more files (pdf/jpg/png)  (input name can be 'files' or 'file' etc.)
    """
    try:
        # ✅ SAFE logs (do NOT touch request.files here)
        ct = request.content_type or ""
        app.logger.info(
            "[upload_document] content_type=%s content_length=%s form_keys=%s",
            ct, request.content_length, list(request.form.keys())
        )

        base_name = (request.form.get("base_name") or "").strip()

        # ✅ Must be multipart, otherwise request.files parsing can behave badly
        if not ct.startswith("multipart/form-data"):
            return jsonify({
                "ok": False,
                "error": "expected_multipart_form_data",
                "content_type": ct
            }), 400

        # ✅ Now parse files (still can disconnect mid-stream, so keep inside try)
        file_keys = list(request.files.keys())
        app.logger.info("[upload_document] request.files keys = %s", file_keys)
        app.logger.info("[upload_document] request.form = %s", dict(request.form))

        if not request.files:
            return jsonify({"ok": False, "error": "no_files"}), 400

        # ---- STEP 1: Save ALL uploaded files to disk ----
        saved_paths = []
        for field_name, file_list in request.files.lists():
            for file_obj in file_list:
                if not file_obj or not file_obj.filename:
                    continue

                # ✅ avoid overwrite if same filename comes twice
                original = secure_filename(file_obj.filename)
                name, ext = os.path.splitext(original)
                unique_name = f"{name}_{int(time.time()*1000)}{ext}"
                saved = os.path.join(PDF_DIR, unique_name)

                file_obj.save(saved)
                saved_paths.append(saved)
                app.logger.info("[upload_document] saved %s from field '%s'", saved, field_name)

        if not saved_paths:
            return jsonify({"ok": False, "error": "empty_files"}), 400

        # ---- STEP 2: First file decides voucher (and creates/merges row) ----
        info_list = []
        vch_code_hint = None
        vch_type_hint = None

        first_path = saved_paths[0]
        first_name = os.path.basename(first_path)

        info_first = _save_voucher_document_to_db(
            first_name,
            first_path,
            voucher_no_override=(base_name or None),
        )
        info_list.append(info_first)
        app.logger.info("[upload_document] first file result: %s", info_first)

        if info_first.get("ok"):
            vch_code_hint = info_first.get("vch_code")
            vch_type_hint = info_first.get("vch_type")

        # ---- STEP 3: Remaining files MERGE into same voucher PDF ----
        for extra_path in saved_paths[1:]:
            extra_name = os.path.basename(extra_path)
            info_extra = _save_voucher_document_to_db(
                extra_name,
                extra_path,
                vch_code_hint=vch_code_hint,
                vch_type_hint=vch_type_hint,
                voucher_no_override=(base_name or None),
            )
            info_list.append(info_extra)
            app.logger.info("[upload_document] merged extra file %s: %s", extra_name, info_extra)

        return jsonify({
            "ok": True,
            "files_saved": saved_paths,
            "db_results": info_list,
        }), 200

    except ClientDisconnected:
        # ✅ this is exactly your screenshot error
        app.logger.warning("[upload_document] client disconnected during upload")
        return jsonify({"ok": False, "error": "client_disconnected"}), 400

    except RequestEntityTooLarge:
        app.logger.warning("[upload_document] upload rejected: too large")
        return jsonify({"ok": False, "error": "file_too_large"}), 413

    except BadRequest as e:
        app.logger.warning("[upload_document] bad request: %s", str(e))
        return jsonify({"ok": False, "error": "bad_request"}), 400

    except Exception as e:
        app.logger.exception("[upload_document] error")
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route('/register_staff', methods=['POST'])
def register_staff():
    name = request.form.get('name', '').strip()
    mobile = request.form.get('mobile', '').strip()
    file = request.files.get('face')
    if not name or not mobile or not file:
        return jsonify({"error": "All fields required"}), 400
    folder = os.path.join(FACES_FOLDER, mobile)
    os.makedirs(folder, exist_ok=True)
    file.save(os.path.join(folder, "face.jpg"))
    try:
        if os.path.exists(STAFF_FILE):
            df = pd.read_excel(STAFF_FILE, dtype=str)
        else:
            df = pd.DataFrame(columns=["name", "mobile"])
        df = pd.concat([df, pd.DataFrame([{"name": name, "mobile": mobile}])], ignore_index=True)
        df.to_excel(STAFF_FILE, index=False)
        return jsonify({"status": "success", "message": "Staff registered"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/mark_attendance', methods=['POST'])
def mark_attendance():
    file = request.files.get('face')
    if not file:
        return jsonify({"error": "Face image required"}), 400
    temp_path = "temp_attendance.jpg"; file.save(temp_path)
    mobile = recognize_face(temp_path)
    try: os.remove(temp_path)
    except Exception: pass
    if not mobile:
        return jsonify({"status": "unrecognized", "message": "Face not matched"}), 404
    name = ""
    if os.path.exists(STAFF_FILE):
        try:
            df_staff = pd.read_excel(STAFF_FILE, dtype=str).fillna("")
            matched = df_staff[df_staff["mobile"].astype(str).str.strip() == str(mobile).strip()]
            if not matched.empty:
                name = matched.iloc[0]["name"].strip().title()
        except Exception as e:
            print("❌ Error reading staff.xlsx:", e)
    if not name: name = "Unknown"
    status = "IN"
    if os.path.exists(ATTENDANCE_FILE):
        df_log = pd.read_excel(ATTENDANCE_FILE, dtype=str)
        prev = df_log[df_log["mobile"] == mobile]
        if not prev.empty and prev.iloc[-1]["status"] == "IN":
            status = "OUT"
    else:
        df_log = pd.DataFrame(columns=["mobile", "name", "datetime", "status"])
    now = datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    df_log.loc[len(df_log)] = {"mobile": mobile,"name": name,"datetime": now,"status": status}
    df_log.to_excel(ATTENDANCE_FILE, index=False)
    return jsonify({"status": "success","message": f"Marked {status} for {name}"}), 200

@app.route('/get_attendance', methods=['GET'])
def get_attendance():
    if not os.path.exists(ATTENDANCE_FILE):
        return jsonify([])
    try:
        df = pd.read_excel(ATTENDANCE_FILE, dtype=str)
        data = df.to_dict(orient="records")
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
def _save_voucher_document_to_db(
    filename: str,
    full_path: str,
    vch_code_hint: int | None = None,
    vch_type_hint: int | None = None,
    voucher_no_override: str | None = None,
) -> dict:
    """
    Save/merge an uploaded document into dbo.Images as a **single PDF**.

    Behaviour:
      - If no row for this voucher -> INSERT new PDF row.
      - If row already exists      -> MERGE new pages into existing PDF.
    """
    result = {
        "ok": False,
        "reason": "",
        "voucher_no": None,
        "vch_code": None,
        "vch_type": None,
    }

    # 1) Voucher number: from override (base_name) or from filename
    base_name = (voucher_no_override or os.path.splitext(filename)[0]).strip()
    result["voucher_no"] = base_name

    if not base_name and not vch_code_hint:
        result["reason"] = "empty_filename_and_no_hint"
        return result

    try:
        conn = pyodbc.connect(SQL_CONN_STR)
        cur = conn.cursor()

        # 2) Get voucher details
        if vch_code_hint is not None:
            vch_code = int(vch_code_hint)
            vch_type = int(vch_type_hint or 0)
        else:
            cur.execute("""
                SELECT TOP 1 VchCode, VchType
                FROM dbo.Tran1
                WHERE LTRIM(RTRIM(VchNo)) = ?
            """, base_name)
            row = cur.fetchone()
            if not row:
                result["reason"] = "voucher_not_found"
                return result
            vch_code = int(row.VchCode)
            vch_type = int(row.VchType)

        result["vch_code"] = vch_code
        result["vch_type"] = vch_type

        # 3) Convert THIS uploaded file to PDF bytes
        ext = os.path.splitext(filename)[1].lower()
        import io
        from PIL import Image
        from PyPDF2 import PdfMerger, PdfReader

        new_pdf_bytes = None

        if ext in [".jpg", ".jpeg", ".png"]:
            img = Image.open(full_path).convert("RGB")
            buf = io.BytesIO()
            img.save(buf, format="PDF")
            new_pdf_bytes = buf.getvalue()
        elif ext == ".pdf":
            with open(full_path, "rb") as f:
                new_pdf_bytes = f.read()
        else:
            # Try as raw PDF anyway
            with open(full_path, "rb") as f:
                new_pdf_bytes = f.read()

        # 4) See if an image row already exists
        cur.execute("""
            SELECT TOP 1 Image1
            FROM dbo.Images
            WHERE [Type] = 2 AND [Code] = ?
        """, vch_code)
        row = cur.fetchone()

        if row and row.Image1:
            # 👉 MERGE existing PDF with new PDF
            existing_bytes = bytes(row.Image1)

            merger = PdfMerger()
            merger.append(PdfReader(io.BytesIO(existing_bytes)))
            merger.append(PdfReader(io.BytesIO(new_pdf_bytes)))
            out_buf = io.BytesIO()
            merger.write(out_buf)
            merger.close()
            merged_bytes = out_buf.getvalue()

            cur.execute("""
                UPDATE dbo.Images
                SET Image1 = ?, FormatType1 = '.Pdf'
                WHERE [Type] = 2 AND [Code] = ?
            """, (merged_bytes, vch_code))
            result["reason"] = "merged_into_existing_pdf"
        else:
            # 👉 First document for this voucher → INSERT
            cur.execute("""
                INSERT INTO dbo.Images ([Type], [Code], [Image1], [FormatType1])
                VALUES (?, ?, ?, ?)
            """, (2, vch_code, new_pdf_bytes, ".Pdf"))
            result["reason"] = "inserted_new_pdf"

        conn.commit()
        result["ok"] = True
        return result

    except Exception as e:
        result["reason"] = f"db_error: {e}"
        return result
    finally:
        try:
            conn.close()
        except Exception:
            pass

# ---- UPI / Daybook / misc helpers ----
def queue_upi_other_report():
    try:
        today = datetime.datetime.now().strftime("%d-%m-%Y")
        now = datetime.datetime.now().strftime("%H%M%S")
        filename = f"upi_other_{today}_{now}.json"
        filepath = os.path.join(QUEUE_DIR, filename)
        with open(filepath, 'w') as f:
            json.dump({"type":"upi_other_report","date": today,"name":"UPI REPORT"}, f)
        print("✅ UPI report queued:", filename)
    except Exception as e:
        print("❌ Failed to queue UPI report:", e)

PROCESSED_LOG = r"C:\\BusyWin\\AI BOT\\BUSY_RECEIPT_BOT_STRUCTURE\\processed_log.txt"
def is_already_processed(mobile):
    if not os.path.exists(PROCESSED_LOG): return False
    with open(PROCESSED_LOG, 'r') as f:
        return mobile in f.read().splitlines()
def mark_as_processed(mobile):
    with open(PROCESSED_LOG, 'a') as f:
        f.write(mobile + "\n")
# ---------- Hindi detection + translation helpers ----------
def _contains_hindi(text: str) -> bool:
    """Return True if the string contains any Devanagari (Hindi) characters."""
    if not text:
        return False
    for ch in str(text):
        if '\u0900' <= ch <= '\u097F':  # Unicode range for Hindi/Devanagari
            return True
    return False

# Create one translator instance (auto-detect language → English)
try:
    _FORM_TRANSLATOR = GoogleTranslator(source="auto", target="en")
except Exception as e:
    print("⚠️ GoogleTranslator init failed, running without translation:", e)
    _FORM_TRANSLATOR = None

def translate_if_hindi(text: str) -> str:
    """
    If the text contains Hindi characters, translate to English.
    If translation fails or translator not available, return original text.
    """
    s = str(text or "").strip()
    if not s:
        return s
    if _FORM_TRANSLATOR is None:
        return s
    if not _contains_hindi(s):
        return s
    try:
        translated = _FORM_TRANSLATOR.translate(s)
        return translated or s
    except Exception as e:
        print("⚠️ Translation failed for:", s, "| error:", e)
        return s
def fetch_influencer_sheet() -> bool:
    """
    Download the Influencer/Model Google Sheet as XLSX and save to INFLUENCER_TEMP_EXCEL.
    Returns True on success, False on failure.
    """
    try:
        # Convert edit URL -> export URL
        if "/edit" in INFLUENCER_SHEET_URL:
            export_url = INFLUENCER_SHEET_URL.split("/edit", 1)[0] + "/export?format=xlsx"
        else:
            export_url = INFLUENCER_SHEET_URL + "&export?format=xlsx"

        resp = requests.get(export_url, timeout=60)
        resp.raise_for_status()

        with open(INFLUENCER_TEMP_EXCEL, "wb") as f:
            f.write(resp.content)

        app.logger.info("[influencer] Sheet downloaded to %s", INFLUENCER_TEMP_EXCEL)
        return True
    except Exception as e:
        app.logger.exception("[influencer] Failed to download sheet: %s", e)
        return False
def _load_influencer_processed() -> set[str]:
    """
    Read processed influencer entries from log file.
    Each line is a key "timestamp|mobile".
    """
    try:
        if not os.path.exists(INFLUENCER_PROCESSED_LOG):
            return set()
        with open(INFLUENCER_PROCESSED_LOG, "r", encoding="utf-8") as f:
            return {line.strip() for line in f if line.strip()}
    except Exception:
        app.logger.exception("[influencer] Failed to read processed log")
        return set()

def _append_influencer_processed(keys: list[str]) -> None:
    """Append new processed keys to log file."""
    if not keys:
        return
    try:
        os.makedirs(os.path.dirname(INFLUENCER_PROCESSED_LOG), exist_ok=True)
        with open(INFLUENCER_PROCESSED_LOG, "a", encoding="utf-8") as f:
            for k in keys:
                f.write(k + "\n")
    except Exception:
        app.logger.exception("[influencer] Failed to append processed log")
def process_influencer_form():
    """
    1) Download Influencer/Model Google Sheet.
    2) For each NOT-YET-PROCESSED row:
         - Read ALL columns from the sheet.
         - Build one big formatted text containing ALL details.
         - Send WhatsApp (BSP first, fallback if needed) to INFLUENCER_ALERT_MSISDN
           using template 'influencer_model' (body has only {1}).
    3) Log processed rows in INFLUENCER_PROCESSED_LOG so they are not resent.
    """
    try:
        if not fetch_influencer_sheet():
            return

        df = pd.read_excel(INFLUENCER_TEMP_EXCEL, dtype=str).fillna("")

        processed = _load_influencer_processed()
        new_keys: list[str] = []

        # Optional safety limit per run
        MAX_PER_RUN = 50
        sent_count = 0

        def _s(val):
            return str(val).strip() if str(val).strip() else "-"

        for _, row in df.iterrows():
            # ---- READ ALL COLUMNS FROM SHEET ----
            ts          = str(row.get("Timestamp", "")).strip()
            email       = str(row.get("Email", "")).strip()
            form_type   = str(row.get("What do you want from us?", "")).strip()

            full_name_1 = str(row.get("Full Name", "")).strip()
            dob_1       = str(row.get("DOB", "")).strip()
            gender_1    = str(row.get("Gender", "")).strip()
            mobile_1    = str(row.get("Whatsapp Number", "")).strip()
            city_1      = str(row.get("City", "")).strip()

            insta_1     = str(row.get("Instagram Profile Link", "")).strip()
            followers   = str(row.get("Current Followers Count", "")).strip()
            target_city = str(row.get("which City in India does your target audience primarily belongs to?", "")).strip()
            proposal    = str(row.get("Describe your Collaboration proposal", "")).strip()

            full_name_2 = str(row.get("Full Name.1", "")).strip()
            dob_2       = str(row.get("DOB.1", "")).strip()
            gender_2    = str(row.get("Gender.1", "")).strip()
            mobile_2    = str(row.get("Whatsapp Number.1", "")).strip()
            city_2      = str(row.get("City.1", "")).strip()

            height      = str(row.get("Height", "")).strip()
            weight      = str(row.get("Weight", "")).strip()
            insta_2     = str(row.get("Instagram Profile Link or Portfolio link", "")).strip()
            about       = str(row.get("Description about yourself", "")).strip()

            # Without timestamp + something (email or mobile) we cannot uniquely track
            if not ts:
                continue

            # Prefer first mobile for key, else email
            uniq_id = mobile_1 or email or "noid"
            key = f"{ts}|{uniq_id}"

            if key in processed:
                continue  # already handled before

            # ---------- BUILD FULL DETAILS TEXT (ONE BIG VARIABLE) ----------
            details = f"""
🕒 Timestamp: {_s(ts)}
📧 Email: {_s(email)}
📄 Form Type: {_s(form_type)}

👤 Applicant Details (Form – Influencer / General)
• Full Name: {_s(full_name_1)}
• DOB: {_s(dob_1)}
• Gender: {_s(gender_1)}
• Whatsapp Number: {_s(mobile_1)}
• City: {_s(city_1)}

📷 Social Profile (From top section)
• Instagram Profile Link: {_s(insta_1)}
• Current Followers Count: {_s(followers)}
• Target Audience City (India): {_s(target_city)}

📝 Collaboration Proposal
{_s(proposal)}

────────────────────────

👗 Modeling Details (Second section of form)
• Full Name: {_s(full_name_2)}
• DOB: {_s(dob_2)}
• Gender: {_s(gender_2)}
• Whatsapp Number: {_s(mobile_2)}
• City: {_s(city_2)}
• Height: {_s(height)}
• Weight: {_s(weight)}
• Instagram / Portfolio Link: {_s(insta_2)}

🧾 Description About Applicant
{_s(about)}

(For any mismatch or correction, please check the master Google Sheet.)
""".strip()

            values = [details]  # ONLY ONE VARIABLE {1} IN TEMPLATE

            # ---------- SEND VIA EXISTING /send_whatsapp_invoice LOGIC ----------
            try:
                payload = {
                    "to": INFLUENCER_ALERT_MSISDN,
                    "template_name": INFLUENCER_TEMPLATE_NAME,
                    "language_code": "en",
                    "values": values,
                }

                # Call your existing endpoint internally (handles BSP + fallback)
                with app.test_request_context(
                    "/send_whatsapp_invoice",
                    method="POST",
                    json=payload
                ):
                    resp = send_whatsapp_invoice()
                    if isinstance(resp, tuple):
                        body, status = resp
                    else:
                        body, status = resp, getattr(resp, "status_code", 200)

                if hasattr(body, "json"):
                    jr = body.json
                else:
                    jr = str(body)

                if 200 <= status < 300:
                    app.logger.info(
                        "[influencer] queued notification for %s (%s)", full_name_1 or full_name_2, key
                    )
                    new_keys.append(key)
                    sent_count += 1
                else:
                    app.logger.error(
                        "[influencer] FAILED to queue notification for %s (%s) status=%s, resp=%s",
                        full_name_1 or full_name_2, key, status, jr
                    )

            except Exception:
                app.logger.exception(
                    "[influencer] exception while sending notification for %s (%s)",
                    full_name_1 or full_name_2, key
                )

            if sent_count >= MAX_PER_RUN:
                app.logger.info(
                    "[influencer] reached MAX_PER_RUN=%s, stopping this cycle", MAX_PER_RUN
                )
                break

        # ---------- UPDATE PROCESSED LOG ----------
        if new_keys:
            _append_influencer_processed(new_keys)
            app.logger.info(
                "[influencer] processed %s new entries", len(new_keys)
            )
        else:
            app.logger.info("[influencer] no new entries to process")

    except Exception:
        app.logger.exception("[influencer] process_influencer_form crashed")

def fetch_google_sheet():
    try:
        r = requests.get(SHEET_URL); r.raise_for_status()
        with open(TEMP_EXCEL, 'wb') as f: f.write(r.content)
        print("✅ Google Sheet downloaded."); return True
    except Exception as e:
        print("❌ Google Sheet fetch failed:", e); return False
import re  # make sure this is at the top of your file

def process_google_form():
    """
    1) Download Google Sheet and read all rows.
    2) For each NOT-YET-PROCESSED mobile:
        - Convert Hindi NAME / CITY / ANY SUGGESTION? to English.
        - Fix name so it ends with exactly one 'JI'.
        - Create register_customer JSON ONLY if NOT registered in DB.
        - If not in DB -> schedule coupon JSON (after 1 min).
    3) Process maximum 30 NEW records per run.
    """
    try:
        if not fetch_google_sheet():
            return

        df = pd.read_excel(TEMP_EXCEL, dtype=str).fillna("")

        conn = pyodbc.connect(SQL_CONN_STR)
        cursor = conn.cursor()

        MAX_NEW = 60      # ⬅️ LIMIT per run (new customers only)
        new_count = 0

        for _, row in df.iterrows():
            raw_mobile = str(row.get("MOBILE NUMBER", "")).strip()
            if not raw_mobile:
                continue

            # --------- MOBILE CLEANING (same as before) ----------
            try:
                if "e+" in raw_mobile.lower():
                    raw_mobile = str(int(float(raw_mobile)))
            except Exception:
                print(f"⚠️ Unable to convert mobile number: {raw_mobile}")
                continue

            cleaned_mobile = re.sub(r"[^\d]", "", raw_mobile)
            if cleaned_mobile.isdigit() and len(cleaned_mobile) == 10:
                json_mobile = "91" + cleaned_mobile
            else:
                json_mobile = cleaned_mobile

            # ❌ if already processed in previous runs → skip (SILENTLY now)
            if is_already_processed(cleaned_mobile):
                # earlier: printed "⏩ Already processed earlier: ..."
                # now: silently skip, no log
                continue

            # ✅ limit to MAX_NEW *new* records per run
            if new_count >= MAX_NEW:
                print(f"⏹️ Limit reached: {MAX_NEW} new records this run. Remaining will be processed next time.")
                break

            # --------- READ RAW FORM FIELDS ----------
            raw_name       = row.get("NAME", "").strip()
            raw_city       = row.get("CITY", "").strip()
            raw_suggestion = row.get("ANY SUGGESTION?", "").strip()
            email          = row.get("EMAIL ID", "").strip()
            dob            = row.get("BIRTHDATE", "")
            anniv          = row.get("ANNIVERSARY", "")
            instagram      = row.get("INSTAGRAM ID", "").strip()

            # --------- TRANSLATE HINDI → ENGLISH ----------
            name_form       = translate_if_hindi(raw_name)
            city_form       = translate_if_hindi(raw_city)
            suggestion_form = translate_if_hindi(raw_suggestion)

            # --------- CHECK IF CUSTOMER ALREADY IN DB ----------
            cursor.execute(
                "SELECT PrintName FROM dbo.Master1 WHERE Alias IN (?, ?)",
                (cleaned_mobile, json_mobile)
            )
            row_sql = cursor.fetchone()

            if row_sql:
                # ALREADY REGISTERED:
                # - NO coupon
                # - NO register_customer JSON
                base_name = row_sql.PrintName or name_form
                print(f"ℹ️ Already registered in DB: {cleaned_mobile}, raw name from DB: {base_name}")
                print(f"✅ Customer already registered. Skipping coupon and register_customer JSON for: {json_mobile}")

                # still mark this mobile as processed so we never re-handle this row
                mark_as_processed(cleaned_mobile)
                # do NOT increment new_count (because no new JSON created)
                continue

            # --------- NOT REGISTERED: normal flow ----------
            base_name = name_form
            print(f"⚠️ Not registered: {cleaned_mobile} — using form name: {base_name}")
            print(f"🕐 Scheduling coupon JSON for {json_mobile} in 1 minute...")
            create_coupon_json_later(json_mobile)

            # 🔹 Ensure exactly ONE 'JI' at the end of the name
            name = ensure_single_ji(base_name)
            print(f"👉 Final name used (with JI fix): {name}")

            # --------- CITY REGISTRATION (use translated city) ----------
            city = city_form
            if city:
                cursor.execute(
                    "SELECT COUNT(*) FROM dbo.Master1 WHERE MasterType = 56 AND Name = ?",
                    city
                )
                city_count = cursor.fetchone()[0]
                if city_count == 0:
                    city_slug = slugify_filename(city)
                    city_json_name = f"register_city_{city_slug}_{datetime.datetime.now().strftime('%d-%m-%Y_%H%M%S')}.json"
                    city_json_path = os.path.join(QUEUE_DIR, city_json_name)
                    with open(city_json_path, 'w', encoding="utf-8") as f:
                        json.dump({"type": "register_city", "city": city}, f, ensure_ascii=False)
                    print(f"🏙️ City '{city}' not found in DB. Created JSON: {city_json_name}")

            # --------- CREATE register_customer JSON (ONLY for new customers) ----------
            filename = f"register_customer_{cleaned_mobile}_{datetime.datetime.now().strftime('%d-%m-%Y_%H%M%S')}.json"
            filepath = os.path.join(QUEUE_DIR, filename)
            with open(filepath, 'w', encoding="utf-8") as f:
                json.dump({
                    "type": "register_customer",
                    "name": name,
                    "mobile": json_mobile,
                    "email": email,
                    "dob": dob,
                    "anniversary": anniv,
                    "city": city,
                    "suggestion": suggestion_form,
                    "instagram": instagram
                }, f, ensure_ascii=False)

            # Mark this mobile as processed ONLY after successful JSON
            mark_as_processed(cleaned_mobile)
            new_count += 1
            print(f"✅ JSON created and logged: {cleaned_mobile} (#{new_count} in this run)")

        conn.close()
        print(f"✅ Google Form processed. New records this run: {new_count}")

    except Exception as e:
        print("❌ Error processing Google Sheet:", e)

def queue_daybook_report_automatically():
    try:
        today = datetime.datetime.now().strftime("%d-%m-%Y")
        now = datetime.datetime.now().strftime("%H%M%S")
        filename = f"daybook_{today}_{now}.json"
        filepath = os.path.join(QUEUE_DIR, filename)
        with open(filepath, 'w') as f: json.dump({"type":"daybook","date": today}, f)
        print("✅ Daybook report queued at 22:03:", filename)
    except Exception as e:
        print("❌ Failed to queue daybook report:", e)
# --- timeout cancellation helpers ---
def _mark_delivered_by_track_id(track_id: str):
    try:
        rec = WA_PENDING.pop(track_id, None)
        if rec:
            rec["fallback_done"] = True
    except Exception:
        pass

def _mark_delivered_by_wamid(wamid: str):
    try:
        rec = WA_PENDING.pop(wamid, None)
        if rec:
            rec["fallback_done"] = True
    except Exception:
        pass

def _auto_fallback_timeout_checker():
    """
    Runs every minute. If there is no known success and the message is older
    than FALLBACK_TIMEOUT_MIN, we send via BotMaster fallback once.
    """
    try:
        if not _fallback_can_send():
            return
        now = time.time()
        timeout_sec = max(2, FALLBACK_TIMEOUT_MIN) * 60
        to_remove = []
        for msg_id, rec in list(WA_PENDING.items()):
            try:
                if rec.get("fallback_done"):
                    to_remove.append(msg_id)
                    continue
                sent_at = rec.get("sent_at") or 0
                if now - sent_at < timeout_sec:
                    continue

                ctx = rec.get("ctx") or {}
                text = ctx.get("raw_text") or _values_to_text(ctx.get("template_name",""), ctx.get("values") or [])
                media_url = ctx.get("media_url")
                local_path = None if media_url else ctx.get("local_path")

                to_msisdn = resolve_receiver(ctx.get("to"))
                if not to_msisdn:
                    app.logger.warning(f"[TIMEOUT] Skip fallback (invalid receiver): {ctx.get('to')}")
                    rec["fallback_done"] = True
                    to_remove.append(msg_id)
                    continue

                app.logger.warning(f"[TIMEOUT] >{FALLBACK_TIMEOUT_MIN}min, sending fallback for msg={msg_id} to={to_msisdn}")
                sc, body = send_fallback_with_sales_extras(
                    to_msisdn,
                    text,
                    media_url=media_url,
                    local_path=local_path,
                    template_name=ctx.get("template_name"),
                )

                rec["fallback_done"] = True
                to_remove.append(msg_id)
            except Exception:
                app.logger.exception("[TIMEOUT] per-message fallback failed")

        for k in to_remove:
            WA_PENDING.pop(k, None)
    except Exception:
        app.logger.exception("[TIMEOUT] checker crashed")

# Scheduler
from pytz import timezone
IST = timezone("Asia/Kolkata")
scheduler = BackgroundScheduler(timezone=IST, job_defaults={"coalesce": True, "misfire_grace_time": 3600, "max_instances": 1})
def process_coupon_reminders_daily():
    """
    09:30 daily (IST):
      - If a coupon expires in 3 days or 2 days, send reminder via SAME BSP template.
      - Attach EXPIRY_COUPON_VIDEO as header.
      - If coupon is already redeemed (TmpRedeemCupponCode), skip + archive.
      - After both reminders (3 and 2) or expiry passed, move JSON to processed.
    """
    try:
        today = datetime.date.today()

        for fname in os.listdir(COUPON_GEN_DIR):
            if not fname.lower().endswith(".json"):
                continue
            fpath = os.path.join(COUPON_GEN_DIR, fname)
            rec = _safe_json_read(fpath)
            if not rec:
                continue

            # --- Parse expiry date from JSON ---
            exp = _parse_any_date(rec.get("expires_on", ""))
            if not exp:
                app.logger.warning(f"[coupon] skip invalid expires_on in {fname}")
                continue

            # --- NEW: skip if coupon already redeemed ---
            coupon_code = str(rec.get("coupon_code", "")).strip()
            # we saved mobile as 'to_msisdn' in _record_coupon_send, but keep fallback keys
            mobile_no = str(
                rec.get("to_msisdn")
                or rec.get("mobile")
                or rec.get("msisdn")
                or ""
            ).strip()

            if coupon_code and mobile_no and is_coupon_redeemed(mobile_no, coupon_code):
                app.logger.info(
                    f"[coupon] already redeemed: mobile={mobile_no}, "
                    f"coupon={coupon_code}. Archiving {fname}."
                )
                _move_coupon_to_processed(fpath)
                continue

            days_left = (exp - today).days
            sent = set(rec.get("reminders_sent") or [])

            should_send = None
            if days_left == 3 and 3 not in sent:
                should_send = 3
            elif days_left == 2 and 2 not in sent:
                should_send = 2

            if should_send is None:
                # done or expired → archive
                if days_left < 0 or ({2, 3}.issubset(sent)):
                    _move_coupon_to_processed(fpath)
                continue

            ok = _enqueue_coupon_reminder(rec)
            if ok:
                sent.add(should_send)
                rec["reminders_sent"] = sorted(list(sent))
                _safe_json_write(fpath, rec)
                app.logger.info(
                    f"[coupon] marked reminder sent: T-{should_send} for {fname}"
                )

                # If both reminders sent or past expiry, archive
                if ({2, 3}.issubset(sent)) or ((exp - today).days < 0):
                    _move_coupon_to_processed(fpath)

    except Exception:
        app.logger.exception("[coupon] daily processor crashed")

def is_coupon_redeemed(mobile_no: str, coupon_code: str) -> bool:
    """
    Return True if this coupon has already been redeemed for this mobile number
    based on dbo.TmpRedeemCupponCode (MobileNo, CupponCode).
    """
    try:
        if not mobile_no or not coupon_code:
            return False

        sql = """
        SELECT TOP 1 1
        FROM dbo.TmpRedeemCupponCode
        WHERE LTRIM(RTRIM(MobileNo)) = ?
          AND LTRIM(RTRIM(CONVERT(varchar(50), CupponCode))) = ?
        """

        with pyodbc.connect(SQL_CONN_STR) as conn:
            cur = conn.cursor()
            cur.execute(sql, (mobile_no.strip(), coupon_code.strip()))
            row = cur.fetchone()
            return row is not None
    except Exception as e:
        app.logger.exception(
            f"[coupon] redeem-check failed for {mobile_no}/{coupon_code}: {e}"
        )
        # If check fails, better to treat as NOT redeemed than silently skip
        return False


def send_points_expiry_reminders(offset_days: int):
    """
    Send points-expiry reminders for customers whose points expire
    `offset_days` days from today (e.g. 10 or 5).

    Template: sshptsexp5  (Hindi)
    Values = [Act, ExpiringPoints, ExpiryDate(dd-mm-yyyy), TotalPoints]

    Header type in template: VIDEO
    """
    try:
        conn = pyodbc.connect(SQL_CONN_STR)
        cur = conn.cursor()

        sql = """
        WITH ExpToday AS (
            SELECT 
                kp.MasterCode1,
                SUM(kp.Points) AS ExpiringPoints,
                MIN(CONVERT(date, kp.Duedate)) AS ExpiryDate
            FROM dbo.KPSPoints kp
            WHERE 
                kp.Points > 0
                AND CONVERT(date, kp.Duedate) = DATEADD(day, ?, CONVERT(date, GETDATE()))
            GROUP BY kp.MasterCode1
        ),
        TotalPts AS (
            SELECT 
                kp.MasterCode1,
                SUM(kp.Points) AS TotalPoints
            FROM dbo.KPSPoints kp
            WHERE 
                CONVERT(date, kp.Duedate) >= CONVERT(date, GETDATE())      -- still valid
            GROUP BY kp.MasterCode1
        )
        SELECT
            m.Code       AS CustomerCode,
            m.PrintName  AS CustomerName,
            m.Alias      AS Mobile,
            e.ExpiringPoints,
            t.TotalPoints,
            e.ExpiryDate
        FROM ExpToday e
        JOIN TotalPts t
              ON e.MasterCode1 = t.MasterCode1
        JOIN dbo.Master1 m
              ON m.Code = e.MasterCode1
        WHERE 
            e.ExpiringPoints <= t.TotalPoints
            AND ISNULL(m.Alias,'') <> ''      -- only customers with mobile
        ORDER BY e.ExpiringPoints DESC;
        """

        # offset_days is used in DATEADD(day, ?, GETDATE())
        cur.execute(sql, offset_days)
        rows = cur.fetchall()
        conn.close()

        if not rows:
            app.logger.info(f"[points_expiry] No customers for T-{offset_days} days.")
            return

        template_name = "sshptsexp5"
        tinfo = _template_registry.get(template_name, {})
        language_code = (tinfo.get("language") or "hi").strip()

        expected_header = get_template_header_type(template_name)

        pdf_link   = None
        image_link = None
        video_link = None

        # --- Use LOCAL VIDEO FILE so BSP uploads once and reuses media_id ---
        local_media_path = None
        if expected_header == "video":
            local_media_path = PTS_EXPIRY_VIDEO

        sent = 0
        for r in rows:
            name   = (r.CustomerName or "").strip()
            mobile = (r.Mobile or "").strip()
            exp_pts = int(r.ExpiringPoints or 0)
            tot_pts = int(r.TotalPoints or 0)

            # r.ExpiryDate is a date/datetime from SQL
            exp_date_val = r.ExpiryDate
            if isinstance(exp_date_val, (datetime.date, datetime.datetime)):
                exp_date_str = exp_date_val.strftime("%d-%m-%Y")
            else:
                exp_date_str = str(exp_date_val)

            to_msisdn = normalize_receiver_id(mobile)
            if not to_msisdn:
                continue

            act_value = f"{name} ({mobile})"
            values = [act_value, str(exp_pts), exp_date_str, str(tot_pts)]

            # --- Busy-style debug string so you can see exactly what is sent ---
            busy_style = f"{template_name}|" + "|".join(values)
            app.logger.info(
                "[points_expiry_dbg] tpl=%s to=%s lang=%s header=video "
                "values=%s local_media=%s busy_format=%s",
                template_name, to_msisdn, language_code, values,
                local_media_path, busy_style
            )

            # Queue job: pass local_media_path via pdf_path (worker already knows
            # how to upload/cached media for non-PDF too, same as invoices)
            job_id = enqueue_send_job(
                to=to_msisdn,
                template_name=template_name,
                language_code=language_code,
                values=values,
                pdf_link=pdf_link,
                image_link=image_link,
                video_link=video_link,     # None → use local_media_path
                pdf_path=local_media_path,  # ⬅️ yahi main bug tha
                raw_text=busy_style,       # stored so webhook/fallback can rebuild text
                delay_seconds=0,
            )

            sent += 1
            app.logger.info(
                f"[points_expiry] queued T-{offset_days} to={to_msisdn} job_id={job_id}"
            )

        app.logger.info(f"[points_expiry] total queued: {sent} (T-{offset_days})")

    except Exception as e:
        app.logger.exception(f"[points_expiry] error for T-{offset_days}: {e}")
from daybook import build_daybook_pdf

from daybook import build_daybook_pdf

DAYBOOK_GROUP_ID = "120363367991085282@g.us"   # ✅ put @g.us here
DAYBOOK_TEMPLATE  = "daybook_pdf"              # ✅ must exist in templates_registry
BRAND_SALE_GROUP_ID = "120363407296746806@g.us"  # same group or another group
BRAND_SALE_TEMPLATE = "brand_sale_pdf"           # must exist in templates_registry
def send_brand_sale_pdf_on_whatsapp(from_date: str, to_date: str, brand: str):
    """
    Generates Brand Sale PDF and sends it via BSP template (DOCUMENT header, vars=1).
    from_date/to_date should be 'YYYY-MM-DD'
    """
    try:
        brand_clean = (brand or "").strip()
        if not brand_clean:
            return {"ok": False, "error": "missing_brand"}

        # 1) build pdf
        pdf_path = build_brand_sale_pdf(from_date, to_date, brand_clean, open_pdf=False)
        if not pdf_path or not os.path.exists(pdf_path):
            app.logger.error("[brand-sale] ❌ PDF not generated")
            return {"ok": False, "error": "pdf_not_generated"}

        # 2) receiver (group id must have @g.us if your sender supports groups)
        to_id = resolve_receiver(BRAND_SALE_GROUP_ID) or BRAND_SALE_GROUP_ID

        # 3) template language
        tpl = (BRAND_SALE_TEMPLATE or "").strip().lower()
        tinfo = _template_registry.get(tpl, {})
        language_code = (tinfo.get("language") or "en").strip()

        # 4) caption (BIG TITLE, small range, then brand)
        # You wanted: "SUBHASH SAREE HOUSE - BRAND SALE REPORT" + small date range + brand
        caption = (
            "SUBHASH SAREE HOUSE - BRAND SALE REPORT\n"
            f"{from_date} to {to_date}\n"
            f"{brand_clean.upper()}"
        )
        values = [caption]  # vars=1

        # 5) queue job (local PDF)
        job_id = enqueue_send_job(
            to=to_id,
            template_name=tpl,
            language_code=language_code,
            values=values,
            pdf_link=None,
            image_link=None,
            video_link=None,
            pdf_path=pdf_path,
            raw_text=caption,
            delay_seconds=0
        )

        app.logger.info("[brand-sale] ✅ queued job_id=%s to=%s pdf=%s", job_id, to_id, pdf_path)
        return {"ok": True, "job_id": job_id, "pdf": os.path.basename(pdf_path)}

    except Exception as e:
        app.logger.exception("[brand-sale] ❌ ERROR: %s", e)
        return {"ok": False, "error": str(e)}

def send_daybook_pdf_on_whatsapp():
    try:
        date_iso = datetime.date.today().strftime("%Y-%m-%d")
        app.logger.info(f"[daybook-job] generating daybook for {date_iso}")

        pdf_path = build_daybook_pdf(date_iso, date_iso, open_pdf=False)

        if not pdf_path or not os.path.exists(pdf_path):
            app.logger.error("[daybook-job] ❌ PDF not generated")
            return {"ok": False, "error": "pdf_not_generated"}

        app.logger.info(f"[daybook-job] PDF generated: {pdf_path}")

        to_id = resolve_receiver(DAYBOOK_GROUP_ID) or DAYBOOK_GROUP_ID

        tinfo = _template_registry.get(DAYBOOK_TEMPLATE.lower(), {})
        language_code = (tinfo.get("language") or "en").strip()

        caption = f"DAYBOOK {date_iso}"
        values = [caption]   # {1} in template body (keep vars=1)

        job_id = enqueue_send_job(
            to=to_id,
            template_name=DAYBOOK_TEMPLATE,
            language_code=language_code,
            values=values,
            pdf_link=None,
            image_link=None,
            video_link=None,
            pdf_path=pdf_path,        # ✅ local pdf file
            raw_text=caption,
            delay_seconds=0
        )

        app.logger.info("[daybook-job] ✅ queued job_id=%s to=%s", job_id, to_id)
        return {"ok": True, "job_id": job_id, "pdf": os.path.basename(pdf_path)}

    except Exception as e:
        app.logger.exception("[daybook-job] ❌ ERROR: %s", e)
        return {"ok": False, "error": str(e)}

# ==========================================
# WRAPPERS around salesman_commission_report
# ==========================================

def _dmy_to_iso(dmy: str) -> str:
    """
    Convert dd-mm-YYYY -> YYYY-MM-DD for SQL parameters.
    """
    d = datetime.datetime.strptime(dmy, "%d-%m-%Y").date()
    return d.strftime("%Y-%m-%d")


def generate_person_daily_report_pdf(name: str, role: str, report_date: str) -> str | None:
    """
    One-day report (for scheduler + /trigger/salesman)

    :param name: Salesman / Helper name EXACTLY as in Busy.
    :param role: 'salesman' or 'helper' (for naming only).
    :param report_date: 'dd-mm-YYYY' (e.g. '11-12-2025')
    :return: full PDF path or None
    """
    try:
        start_iso = _dmy_to_iso(report_date)
        end_iso = start_iso

        rows = fetch_salesman_rows(start_iso, end_iso, name)
        if not rows:
            app.logger.info(
                f"[salesman-pdf] No rows for {name} ({role}) on {report_date}"
            )
            return None

        safe_name = slugify_filename(name) or "Unknown"
        pdf_name = f"sales_comm_{role}_{safe_name}_{report_date.replace('-', '')}.pdf"
        out_path = os.path.join(PDF_DIR, pdf_name)

        # For display in header we keep dd-mm-YYYY
        generate_pdf(rows, report_date, report_date, name, out_path)
        return out_path

    except Exception:
        app.logger.exception(
            f"[salesman-pdf] Failed to build daily PDF for {name} ({role}) on {report_date}"
        )
        return None


def generate_person_summary_report_pdf(name: str, role: str, from_date: str, to_date: str) -> str | None:
    """
    Multi-day / monthly summary report (for summary scheduler and /trigger/sales_reports_window).

    :param name: Salesman / Helper name EXACTLY as in Busy.
    :param role: 'salesman' or 'helper' (for naming only).
    :param from_date: 'dd-mm-YYYY'
    :param to_date: 'dd-mm-YYYY'
    :return: full PDF path or None
    """
    try:
        start_iso = _dmy_to_iso(from_date)
        end_iso = _dmy_to_iso(to_date)

        rows = fetch_salesman_rows(start_iso, end_iso, name)
        if not rows:
            app.logger.info(
                f"[summary-pdf] No rows for {name} ({role}) in range {from_date}..{to_date}"
            )
            return None

        safe_name = slugify_filename(name) or "Unknown"
        pdf_name = f"sales_comm_{role}_{safe_name}_{from_date.replace('-', '')}_{to_date.replace('-', '')}.pdf"
        out_path = os.path.join(PDF_DIR, pdf_name)

        # Pass dd-mm-YYYY for display
        generate_pdf(rows, from_date, to_date, name, out_path)
        return out_path

    except Exception:
        app.logger.exception(
            f"[summary-pdf] Failed to build summary PDF for {name} ({role}) {from_date}..{to_date}"
        )
        return None
def _send_salesman_pdf(to_msisdn: str, name: str, role: str,
                       from_date: str, to_date: str,
                       pdf_path: str) -> str | None:
    """
    Use BSP template 'salesman' to send the generated PDF.

    Template vars (5):
      {1}  -> Name / role line
      {2}  -> From date (Hindi line)   e.g. 01-12-2025
      {3}  -> To date (Hindi line)
      {4}  -> From date (English line)
      {5}  -> To date (English line)
    """
    try:
        template_name = "salesman"
        tinfo = _template_registry.get(template_name, {})
        language_code = (tinfo.get("language") or "hi").strip()

        # You can change this line if you want a different display:
        act_value = f"{name} ({role})"

        values = [
            act_value,   # {1}
            from_date,   # {2} Hindi line
            to_date,     # {3}
            from_date,   # {4} English line (same dd-mm-YYYY text)
            to_date,     # {5}
        ]

        busy_style = f"{template_name}|" + "|".join(values)

        delay_sec = delay_minutes_for_template(template_name) * 60

        job_id = enqueue_send_job(
            to=to_msisdn,
            template_name=template_name,
            language_code=language_code,
            values=values,
            pdf_link=None,
            image_link=None,
            video_link=None,
            pdf_path=pdf_path,    # 👈 actual local PDF file
            raw_text=busy_style,
            delay_seconds=delay_sec,
        )

        app.logger.info(
            "[salesman-pdf-send] queued tpl=%s to=%s job_id=%s pdf=%s",
            template_name, to_msisdn, job_id, pdf_path
        )
        return job_id

    except Exception:
        app.logger.exception(
            "[salesman-pdf-send] failed for %s (%s) %s-%s",
            name, role, from_date, to_date
        )
        return None

def create_salesman_reports():
    """
    Nightly:
      1) Generate DAILY PDF commission report for each active
         salesman + helper (helpers only if not also salesman).
      2) Immediately send that PDF via BSP template 'salesman'.

    ✅ No JSON
    ✅ No AHK
    ✅ Direct WhatsApp with PDF
    """
    try:
        report_date = datetime.datetime.now().strftime("%d-%m-%Y")
        recipients = get_active_recipients(report_date, report_date, vch_type=9)

        pdf_ok = 0
        send_ok = 0
        send_fail = 0

        for p in recipients:
            role = p.get("kind", "salesman")   # 'salesman' or 'helper'
            name = (p.get("name") or "").strip()
            mobile = (p.get("mobile") or "").strip()

            if not name:
                continue
            to_msisdn = normalize_receiver_id(mobile)
            if not to_msisdn:
                app.logger.warning(
                    "[salesman-pdf] Skipping %s (%s) — invalid mobile '%s'",
                    name, role, mobile
                )
                continue

            # 1) build PDF for that person for this date
            pdf_path = generate_person_daily_report_pdf(
                name=name,
                role=role,
                report_date=report_date,
            )
            if not pdf_path:
                continue

            pdf_ok += 1

            # 2) send via BSP template
            job_id = _send_salesman_pdf(
                to_msisdn=to_msisdn,
                name=name,
                role=role,
                from_date=report_date,
                to_date=report_date,
                pdf_path=pdf_path,
            )
            if job_id:
                send_ok += 1
            else:
                send_fail += 1

        app.logger.info(
            "[salesman-pdf] date=%s pdf_ok=%s sent=%s failed=%s",
            report_date, pdf_ok, send_ok, send_fail
        )

    except Exception as e:
        app.logger.exception("Scheduled task error (create_salesman_reports): %s", e)

def create_salesman_summary_reports():
    """
    On 5,10,15,20,25 and month-end:
      1) Generate SUMMARY PDF (1st-of-month .. today)
      2) Send via BSP template 'salesman'.
    """
    try:
        today = datetime.datetime.now()
        current_day = today.day
        last_day = (today.replace(day=28) + datetime.timedelta(days=4)).replace(day=1) - datetime.timedelta(days=1)

        if current_day not in [5, 10, 15, 20, 25] and today.date() != last_day.date():
            return

        from_date = today.replace(day=1).strftime("%d-%m-%Y")
        to_date   = today.strftime("%d-%m-%Y")

        recipients = get_active_recipients(from_date, to_date, vch_type=9)

        pdf_ok = 0
        send_ok = 0
        send_fail = 0

        for p in recipients:
            role = p.get("kind", "salesman")
            name = (p.get("name") or "").strip()
            mobile = (p.get("mobile") or "").strip()

            if not name:
                continue
            to_msisdn = normalize_receiver_id(mobile)
            if not to_msisdn:
                app.logger.warning(
                    "[summary-pdf] Skipping %s (%s) — invalid mobile '%s'",
                    name, role, mobile
                )
                continue

            pdf_path = generate_person_summary_report_pdf(
                name=name,
                role=role,
                from_date=from_date,
                to_date=to_date,
            )
            if not pdf_path:
                continue

            pdf_ok += 1

            job_id = _send_salesman_pdf(
                to_msisdn=to_msisdn,
                name=name,
                role=role,
                from_date=from_date,
                to_date=to_date,
                pdf_path=pdf_path,
            )
            if job_id:
                send_ok += 1
            else:
                send_fail += 1

        app.logger.info(
            "[summary-pdf] %s..%s pdf_ok=%s sent=%s failed=%s",
            from_date, to_date, pdf_ok, send_ok, send_fail
        )

    except Exception as e:
        app.logger.exception("❌ Error in salesman summary report: %s", e)

def create_whatsapp_trigger():
    """
    ✅ OLD AHK FORMAT JSON (same keys)
    ✅ Only ONE JSON for ALL branches
    """
    try:
        today = datetime.datetime.now().strftime("%d-%m-%Y")
        now_time = datetime.datetime.now().strftime("%H:%M")

        series = "all"  # ✅ only one consolidated trigger

        filename = f"whatsapp_{series}_{today}_{datetime.datetime.now().strftime('%H%M%S')}.json"
        filepath = os.path.join(QUEUE_DIR, filename)

        payload = {
            "type": "whatsapp",          # ✅ keep OLD type (AHK expects this)
            "category": "schedule",
            "series": series,            # ✅ only 'all'
            "date": today,
            "time": now_time
        }

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)

        print(f"✅ WhatsApp trigger JSON created (ONLY ONE): {filename}")

    except Exception as e:
        print("❌ WhatsApp scheduler error:", e)
from points_status import fetch_points_status
@app.get("/api/points_status")
def api_points_status():
    try:
        mobile = (request.args.get("mobile") or "").strip()
        if not mobile:
            return jsonify({"ok": False, "error": "missing_mobile"}), 400

        rows = fetch_points_status(mobile, SQL_CONN_STR)

        return jsonify({
            "ok": True,
            "mobile": mobile,
            "count": len(rows),
            "rows": rows
        }), 200
    except Exception as e:
        app.logger.exception("points_status error: %s", e)
        return jsonify({"ok": False, "error": "server_error"}), 500
@app.get("/points_status")
def points_status_page():
    # Simple built-in HTML (no template file needed)
    return """
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Points Status</title>
  <style>
    body{font-family:Arial;margin:16px;}
    input{padding:10px;font-size:16px;width:220px;}
    button{padding:10px 14px;font-size:16px;cursor:pointer;}
    table{border-collapse:collapse;margin-top:14px;width:100%;font-size:14px;}
    th,td{border:1px solid #ddd;padding:8px;text-align:left;}
    th{background:#f3f3f3;}
    .row{display:flex;gap:10px;align-items:center;flex-wrap:wrap;}
  </style>
</head>
<body>
  <h2>Points Status</h2>
  <div class="row">
    <input id="mob" placeholder="Enter 10-digit mobile" maxlength="15"/>
    <button onclick="search()">Search</button>
  </div>

  <div id="meta" style="margin-top:10px;"></div>
  <div id="out"></div>

<script>
async function search(){
  const mob = document.getElementById('mob').value.trim();
  document.getElementById('meta').innerText = "Loading...";
  document.getElementById('out').innerHTML = "";

  const res = await fetch(`/api/points_status?mobile=${encodeURIComponent(mob)}`);
  const j = await res.json();

  if(!j.ok){
    document.getElementById('meta').innerText = "Error: " + (j.error || "unknown");
    return;
  }

  document.getElementById('meta').innerText = `Rows: ${j.count}`;

  if(!j.rows || j.rows.length===0){
    document.getElementById('out').innerText = "No data.";
    return;
  }

  const cols = Object.keys(j.rows[0]);
  let html = "<table><thead><tr>";
  for(const c of cols) html += `<th>${c}</th>`;
  html += "</tr></thead><tbody>";

  for(const r of j.rows){
    html += "<tr>";
    for(const c of cols){
      let v = r[c];
      if(v===null || v===undefined) v = "";
      html += `<td>${String(v)}</td>`;
    }
    html += "</tr>";
  }

  html += "</tbody></table>";
  document.getElementById('out').innerHTML = html;
}
</script>
</body>
</html>
"""

def check_daybook_queue():
    try:
        now = datetime.datetime.now()
        if now.hour == 22 and now.minute == 2:
            for file in os.listdir(QUEUE_DIR):
                if file.startswith("daybook_") and file.endswith(".json"):
                    print("Daybook JSON found. Triggering AHK...")
                    subprocess.Popen(["C:\\Program Files\\AutoHotkey\\AutoHotkey.exe", AHK_SCRIPT_PATH])
                    break
    except Exception as e:
        print("Error checking daybook queue:", e)

def queue_future_report():
    try:
        now = datetime.datetime.now()
        timestamp = now.strftime("%d-%m-%Y_%H%M%S")
        filename = f"future_report_{timestamp}.json"
        filepath = os.path.join(QUEUE_DIR, filename)
        with open(filepath, 'w') as f: json.dump({"type":"future_report","timestamp":timestamp}, f)
        print("✅ Future report queued:", filename)
    except Exception as e:
        print("❌ Failed to queue future report:", e)

def queue_coupon_expiry():
    try:
        now = datetime.datetime.now()
        start_date = now.strftime("%d-%m-%Y")
        end_date = (now + datetime.timedelta(days=6)).strftime("%d-%m-%Y")
        timestamp = now.strftime("%d-%m-%Y_%H%M%S")
        filename = f"coupon_expiry_{timestamp}.json"
        filepath = os.path.join(QUEUE_DIR, filename)
        with open(filepath, 'w') as f: json.dump({"type":"coupon_expiry","start_date":start_date,"end_date":end_date}, f)
        print("✅ Coupon Expiry JSON queued:", filename)
    except Exception as e:
        print("❌ Failed to create coupon expiry JSON:", e)

# Balance Confirmation
def queue_balance_confirmation_reports(branches_series=None, for_date=None):
    try:
        now = datetime.datetime.now()
        if for_date is None:
            for_date = now
        start_date = for_date.strftime("%d-%m-%Y")
        end_date = start_date
        timestamp = now.strftime("%d-%m-%Y_%H%M%S")
        mapping = branches_series or BALCONF_BRANCHES
        created = []
        for branch, series_list in (mapping or {}).items():
            for series in series_list:
                filename = f"balance_confirmation_{branch}_{series}_{timestamp}.json"
                filepath = os.path.join(QUEUE_DIR, filename)
                payload = {"type":"balance_confirmation","branch":branch,"series":series,"start_date":start_date,"end_date":end_date}
                with open(filepath, "w") as f: json.dump(payload, f)
                created.append(filename)
                print(f"✅ Balance Confirmation queued: {filename}")
        if not created:
            print("ℹ️ No Balance Confirmation files created (empty mapping).")
        return created
    except Exception as e:
        print("❌ Failed to queue Balance Confirmation:", e)
        return []
@app.route("/trigger/brand_sale", methods=["GET", "POST"])
def trigger_brand_sale():
    """
    Usage:
      /trigger/brand_sale?brand=LAXMIPATI&from=26-12-2025&to=26-12-2025

    POST JSON:
      {"brand":"LAXMIPATI","from":"26-12-2025","to":"26-12-2025"}
    """
    try:
        args = request.values or {}
        body = request.get_json(silent=True) if request.is_json else {}

        brand = (args.get("brand") or (body or {}).get("brand") or "").strip()
        from_str = (args.get("from") or (body or {}).get("from") or "").strip()
        to_str   = (args.get("to")   or (body or {}).get("to")   or "").strip()

        if not brand or not from_str or not to_str:
            return jsonify({
                "ok": False,
                "error": "missing_params",
                "need": "brand, from, to",
                "example": "/trigger/brand_sale?brand=LAXMIPATI&from=26-12-2025&to=26-12-2025"
            }), 400

        # If your build_brand_sale_pdf expects ISO, convert here.
        # Example: dd-mm-YYYY -> YYYY-MM-DD
        from_iso = _dmy_to_iso(from_str)
        to_iso   = _dmy_to_iso(to_str)

        result = send_brand_sale_pdf_on_whatsapp(from_iso, to_iso, brand)
        return jsonify(result), (200 if result.get("ok") else 500)

    except Exception as e:
        app.logger.exception("trigger_brand_sale error: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route('/trigger/balance_confirmation', methods=['GET', 'POST'])
def trigger_balance_confirmation():
    try:
        args = request.values or {}; body = {}
        if request.is_json:
            body = request.get_json(silent=True) or {}
        date_str = (args.get("date") or body.get("date") or "").strip()
        for_date = None
        if date_str:
            try:
                for_date = datetime.datetime.strptime(date_str, "%d-%m-%Y")
            except ValueError:
                return jsonify({"error": "Invalid date format. Use dd-mm-YYYY"}), 400
        mapping = body.get("mapping")
        if mapping and not isinstance(mapping, dict):
            return jsonify({"error": "`mapping` must be an object {branch: [series...] }"}), 400
        if not mapping:
            branches_param = (args.get("branches") or "").strip()
            series_param = (args.get("series") or "").strip()
            if branches_param or series_param:
                if branches_param:
                    selected_branches = [b.strip() for b in branches_param.split(",") if b.strip()]
                else:
                    selected_branches = list(BALCONF_BRANCHES.keys())
                if series_param:
                    series_list = [s.strip() for s in series_param.split(",") if s.strip()]
                    mapping = {b: series_list for b in selected_branches}
                else:
                    mapping = {b: BALCONF_BRANCHES.get(b, []) for b in selected_branches}
            else:
                mapping = None
        created = queue_balance_confirmation_reports(branches_series=mapping, for_date=for_date)
        return jsonify({"status":"manually triggered","count": len(created),"files": created})
    except Exception as e:
        return jsonify({"error": f"Failed to trigger Balance Confirmation: {e}"}), 500

# ---------------------------
# Health & utility endpoints
# ---------------------------
@app.get("/health")
def health():
    return jsonify({
        "ok": True,
        "dry_run": DRY_RUN,
        "pdf_dir": PDF_DIR,
        "api_url_configured": bool(API_URL),
        "templates_loaded": len(_template_registry),
        "key_header": KEY_HEADER_NAME,
        "auth_bearer": AUTH_BEARER,
        "s3_enabled": S3_ENABLE,
        "public_base_url": PUBLIC_BASE_URL,
        "queue_size": JobQueue.qsize(),
        "num_workers": len(_worker_threads),
        "retry_max": RETRY_MAX
    })
@app.post("/_fallback_test")
def _fallback_test():
    data = request.get_json(silent=True) or {}
    to = normalize_mobile(data.get("to") or "")
    text = data.get("text") or "Test via BotMasterSender — Subhash Saree House"
    mediaurl = data.get("mediaurl") or None
    if not _fallback_can_send():
        return jsonify({"ok": False, "error": "fallback_not_configured"}), 400
    sc, body = send_fallback_botmaster(to, text, media_url=mediaurl)
    append_fallback_log(to, text, mediaurl, sc, body)
    return jsonify({"ok": (200 <= sc < 300), "status": sc, "response": body})
# ---------------------------
# Config: Fallback mode toggle
# ---------------------------
# ---------------------------
# Config: Fallback mode toggle (GET & POST)
# ---------------------------
@app.route("/config/fallback_mode", methods=["GET", "POST"])
def fallback_mode_config():
    global GLOBAL_FORCE_FALLBACK_ALL

    # Read "mode" from JSON body, form, or query (?mode=on)
    data = {}
    if request.is_json:
        data = request.get_json(silent=True) or {}

    mode = (data.get("mode")
            or request.form.get("mode")
            or request.args.get("mode")
            or "").strip().lower()

    # If mode is provided → update flag
    if mode:
        if mode in ("on", "true", "1"):
            GLOBAL_FORCE_FALLBACK_ALL = True
        elif mode in ("off", "false", "0"):
            GLOBAL_FORCE_FALLBACK_ALL = False
        elif mode == "toggle":
            GLOBAL_FORCE_FALLBACK_ALL = not GLOBAL_FORCE_FALLBACK_ALL
        else:
            return jsonify({
                "ok": False,
                "error": "invalid_mode",
                "detail": "Use mode=on|off|toggle"
            }), 400
        app.logger.info(f"[config] GLOBAL_FORCE_FALLBACK_ALL set to {GLOBAL_FORCE_FALLBACK_ALL}")

    # Always return current state
    return jsonify({
        "ok": True,
        "force_all": GLOBAL_FORCE_FALLBACK_ALL,
        "env_default": FORCE_FALLBACK_ALL_DEFAULT,
        "fallback_configured": _fallback_can_send()
    })

@app.get("/templates")
def list_templates():
    return jsonify(_template_registry)

@app.route("/templates/reload", methods=["GET","POST"])
def reload_templates():
    count = len(load_template_registry())
    return jsonify({"ok": True, "count": count})

@app.get("/files/<path:fname>")
def serve_file(fname):
    full = os.path.join(PDF_DIR, fname)
    if not os.path.isfile(full):
        return abort(404, description="File not found")
    return send_from_directory(PDF_DIR, fname, as_attachment=False)

@app.get("/jobs/<job_id>")
def job_status(job_id):
    info = JOBS.get(job_id)
    if not info: return jsonify({"error": "not_found"}), 404
    return jsonify({"job_id": job_id, **info})
@app.post("/bsp/webhook")
def bsp_webhook():
    """
    Receive delivery status callbacks from your BSP/Cloud API.
    On statuses like 'failed', 'undelivered', etc., trigger fallback once.
    Configure your BSP to POST here.
    """
    try:
        data = request.get_json(silent=True) or {}
        # Handle common WhatsApp Cloud structure: entry -> changes -> value.statuses[]
        statuses = []
        try:
            for entry in data.get("entry", []) or []:
                for ch in entry.get("changes", []) or []:
                    val = ch.get("value") or {}
                    if isinstance(val.get("statuses"), list):
                        statuses.extend(val["statuses"])
        except Exception:
            pass

        # Some BSPs post a flatter shape like {"statuses":[{...}]}
        if not statuses and isinstance(data.get("statuses"), list):
            statuses = data["statuses"]

        processed = 0
        for st in statuses:
            try:
                wamid = str(st.get("id") or st.get("message_id") or "")
                status = (st.get("status") or "").lower().strip()   # delivered, sent, read, failed, undelivered, deleted...
                reason = (st.get("reason") or st.get("error_message") or "").lower()
                # WhatsApp Cloud often uses errors:[{"code":..., "title":..., "message":...}]
                if not reason and isinstance(st.get("errors"), list) and st["errors"]:
                    reason = (" ".join(str(e.get("message") or e.get("title") or "") for e in st["errors"])).lower()

                if not wamid:
                    continue

                # Only react to *failure-like* terminal states
                should_fallback = status in ("failed", "undelivered", "deleted")
                # Also catch the "healthy ecosystem engagement" message
                if "healthy ecosystem engagement" in reason:
                    should_fallback = True

                if not should_fallback:
                    continue
                if wamid in WA_FALLBACK_DONE:
                    continue

                ctx = WA_MSG_MAP.get(wamid)
                if not ctx:
                    app.logger.warning(f"[WEBHOOK] failure for unknown wamid={wamid}")
                    continue

                if not _fallback_can_send():
                    app.logger.warning("[WEBHOOK] fallback requested but not configured")
                    continue

                text = ctx.get("raw_text") or _values_to_text(ctx.get("template_name",""), ctx.get("values") or [])
                media_url = ctx.get("media_url")
                local_path = None if media_url else ctx.get("local_path")
                sc, body = send_fallback_with_sales_extras(
                    ctx["to"],
                    text,
                    media_url=media_url,
                    local_path=local_path,
                    template_name=ctx.get("template_name"),
                )

                if 200 <= sc < 300:
                    WA_FALLBACK_DONE.add(wamid)
                    app.logger.info(f"[WEBHOOK] ✅ fallback sent for wamid={wamid} to={ctx['to']}")
                else:
                    app.logger.error(f"[WEBHOOK] ❌ fallback failed ({sc}) for wamid={wamid}")

                processed += 1
            except Exception:
                app.logger.exception("[WEBHOOK] per-status handling failed")

        return jsonify({"ok": True, "processed": processed}), 200
    except Exception as e:
        app.logger.exception("bsp_webhook error")
        return jsonify({"ok": False, "error": str(e)}), 500

# ---------------------------
# Template send (explicit API)
# ---------------------------
@app.post("/send_whatsapp_invoice")
def send_whatsapp_invoice():
    try:
        data = request.get_json(force=True)
        to = str(data.get("to", "")).strip()
        template_name = str(data.get("template_name", "") or TEMPLATE_DEFAULT).strip().lower()
        values = data.get("values", [])
        if isinstance(values, (str, int, float)):
            values = [str(values)]
        if not to:
            return jsonify({"error": "Missing 'to'"}), 400
        tinfo = _template_registry.get(template_name, {})
        language_code = (data.get("language_code") or tinfo.get("language") or "en").strip()
        expected_vars = tinfo.get("vars")
        if expected_vars is not None and values and len(values) != expected_vars:
            return jsonify({"error":"invalid_param_count","detail": f"Template '{template_name}' expects {expected_vars} vars, got {len(values)}"}), 400
        pdf_link   = str(data.get("pdf_link", "")).strip()
        image_link = str(data.get("image_link", "")).strip()
        video_link = str(data.get("video_link", "")).strip()
        pdf_path   = str(data.get("pdf_path", "")).strip()

        # ----- FIXED COUPON VIDEOS (override caller media) -----
        fixed_video = COUPON_VIDEO_MAP.get(template_name)
        if fixed_video:
            # Force our local video file; ignore incoming links/paths
            pdf_link = ""
            image_link = ""
            video_link = ""
            pdf_path = fixed_video

        # Use Excel delay (or 10 min for "*cup")
        delay_sec = delay_minutes_for_template(template_name) * 60


        job_id = enqueue_send_job(
            to=to, template_name=template_name, language_code=language_code,
            values=values,
            pdf_link=(pdf_link or None),
            image_link=(image_link or None),
            video_link=(video_link or None),
            pdf_path=(pdf_path or None) if (not pdf_link and not image_link and not video_link) else None,
            delay_seconds=delay_sec,
        )


        app.logger.info(f"[queue] job queued id={job_id} to={to} tpl={template_name}")
        return jsonify({"queued": True, "job_id": job_id, "status": "queued"}), 202
    except Exception as e:
        app.logger.exception("send_whatsapp_invoice exception")
        return jsonify({"error": "bridge_failed", "detail": str(e)}), 500

# -------------------------------------------------------
# Helpers: Busy 'sale...' parser + mapping
# -------------------------------------------------------
def _looks_like_sale_format(msg: str) -> bool:
    if not msg or "|" not in msg:
        return False
    first = msg.split("|", 1)[0].strip()
    return first.lower().startswith("sale") and len(first) >= 5

def _parse_sale_and_build(template_by_series: dict, parts: list[str]) -> tuple[str, list[str]]:
    """
    parts[0]  = 'sale' + VCH_SERIES (e.g., 'saleSSHN', 'saleSSHR', 'saleMAIN', 'saleTALL'...)
    parts[1]  = BILLED_PARTY_NAME
    parts[2]  = VCH/BILL_DATE (10) + ' (' + VCH_CREATION_TIME (8) + ')'
    parts[3]  = VCH/BILL_NO
    parts[4]  = VCH_OPT_FIELD1 (Delivery Date)
    parts[5]  = AMOUNT_GRAND_TOTAL
    parts[6]  = VCH_OPT_FIELD2  -> Loyalty Previous
    parts[7]  = VCH_OPT_FIELD3  -> Loyalty Earned
    parts[8]  = VCH_OPT_FIELD4  -> Loyalty Redeemed
    parts[9]  = VCH_OPT_FIELD5  -> Loyalty Balance
    parts[10]..[13] = SETTLEMENT_AMT1..4
    parts[14] = SETTLEMENT_PARTY_AMT -> Current Balance
    parts[15] = PARTY_BALANCE        -> Total Balance till date
    """
    series_raw = (parts[0] or "").strip()
    series = series_raw[4:].strip().upper() if len(series_raw) > 4 else ""

    # Resolve template from series, falling back sensibly
    template_name = template_by_series.get(series) or template_by_series.get("_default") or _find_first_sale_template_default()
    template_name = template_name.lower().strip()

    def _g(i):
        return parts[i].strip() if len(parts) > i else ""

    values = [
        _g(1),  # name
        _g(2),  # bill_dt_time
        _g(3),  # bill_no
        _g(4),  # delivery_date
        _g(5),  # billed_amount
        _g(6),  # loy_prev
        _g(7),  # loy_earned
        _g(8),  # loy_redeemed
        _g(9),  # loy_balance
        _g(10), # pay_cash
        _g(11), # pay_line2
        _g(12), # pay_line3
        _g(13), # pay_line4
        _g(14), # current_balance
        _g(15)  # total_balance_till_date
    ]

    # Clean obvious blanks coming from Busy like '', '-', '—'
    def _clean(v):
        v = str(v).strip()
        return "" if v in ("-", "—", "--") else v
    values = [_clean(v) for v in values]
    return template_name, values

def normalize_template_and_values(tpl_raw: str | None, values_raw: list[str] | None) -> tuple[str, list[str]]:
    """
    Handles:
      1) Busy 'sale...' format with 16 parts (1 + 15 values) -> map to template by series.
      2) Normal 'template|v1|v2|...' format.
    Also normalizes blanks to '-' and enforces expected var count.
    """
    tpl_raw = (tpl_raw or "").strip()
    values_raw = values_raw or []

    # Build series->template map from registry (if present)
    series_map = {}
    for tname in _template_registry.keys():
        tl = tname.lower()
        if tl.startswith("sale"):
            series = tl.replace("sale", "", 1).upper() or "MAIN"
            series_map[series] = tl
    if "_default" not in series_map:
        series_map["_default"] = _find_first_sale_template_default()

    # Path A: sale... format
    if tpl_raw.lower().startswith("sale"):
        parts = [tpl_raw] + [str(p or "").strip() for p in values_raw]
        template_name, values = _parse_sale_and_build(series_map, parts)
    else:
        # Path B: normal explicit template
        template_name = tpl_raw.lower() if tpl_raw else (TEMPLATE_DEFAULT or "invoice1").lower()
        values = [str(v or "").strip() for v in values_raw]

    # 🔧 Normalize blanks -> '-'
    # Treat '-', '—', empty or whitespace as blank and replace with '-'
    def _nz(v: str) -> str:
        s = str(v).strip()
        if s in ("", "-", "—"):
            return "-"
        return s
    values = [_nz(v) for v in values]

    # Enforce expected var count from registry if available
    expected = _template_registry.get(template_name, {}).get("vars")
    if isinstance(expected, int) and expected > 0:
        if len(values) < expected:
            values = values + ["-"] * (expected - len(values))   # pad with '-'
        elif len(values) > expected:
            values = values[:expected]

    return template_name, values

# ---------------------------
# Busy bridge for *templates* (enhanced)
# ---------------------------
import re
import datetime
from decimal import Decimal

def _money_to_decimal(s: str) -> Decimal:
    s = (s or "").strip()
    s = s.replace(",", "")
    if not s:
        return Decimal("0")
    try:
        return Decimal(s)
    except Exception:
        return Decimal("0")

def _format_inr(d: Decimal) -> str:
    # Simple formatting with commas (works fine for your use)
    try:
        return f"{d:,.2f}"
    except Exception:
        return str(d)

# ==============================
# DESIGN LOOKUP (ALL DBs)
#   - barcode + store + closing qty (latest FY DB)
#   - purchase party (latest purchase across ALL DBs)
#   - sales price (latest D4 across ALL DBs)
#   - only rows where ClosingQty > 0
# ==============================

SQL_DESIGN_STOCK_ALLDB = r"""
SET NOCOUNT ON;

DECLARE @DesignNo NVARCHAR(50) = ?;

IF OBJECT_ID('tempdb..#BCNList')             IS NOT NULL DROP TABLE #BCNList;
IF OBJECT_ID('tempdb..#BCNListFinal')        IS NOT NULL DROP TABLE #BCNListFinal;
IF OBJECT_ID('tempdb..#PurCandidates')       IS NOT NULL DROP TABLE #PurCandidates;
IF OBJECT_ID('tempdb..#PurchasePartyFinal')  IS NOT NULL DROP TABLE #PurchasePartyFinal;
IF OBJECT_ID('tempdb..#SalePriceCandidates') IS NOT NULL DROP TABLE #SalePriceCandidates;
IF OBJECT_ID('tempdb..#SalesPriceFinal')     IS NOT NULL DROP TABLE #SalesPriceFinal;

DECLARE @LatestDb SYSNAME;

SELECT @LatestDb = MAX(name)
FROM sys.databases
WHERE name LIKE 'BusyComp0001_db1%'
  AND state_desc = 'ONLINE';

IF @LatestDb IS NULL
BEGIN
    -- Return empty resultset with expected columns
    SELECT CAST('' AS NVARCHAR(10))  AS LatestDB,
           @DesignNo                AS DesignNo,
           CAST('' AS NVARCHAR(100)) AS Barcode,
           CAST('' AS NVARCHAR(255)) AS Store,
           CAST(0 AS DECIMAL(18,3))  AS ClosingQty,
           CAST('' AS NVARCHAR(255)) AS PurchasePartyName,
           CAST(0 AS DECIMAL(18,2))  AS SalesPrice
    WHERE 1=0;
    RETURN;
END;

------------------------------------------------------------
-- 1) Collect ALL barcodes for this DesignNo from ALL DBs
------------------------------------------------------------
CREATE TABLE #BCNList ( Barcode NVARCHAR(100) NOT NULL );

DECLARE @db SYSNAME, @sql NVARCHAR(MAX);

DECLARE dbcur CURSOR FAST_FORWARD FOR
SELECT name
FROM sys.databases
WHERE name LIKE 'BusyComp0001_db1%'
  AND state_desc = 'ONLINE';

OPEN dbcur;
FETCH NEXT FROM dbcur INTO @db;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @sql = N'
    IF EXISTS (SELECT 1 FROM ' + QUOTENAME(@db) + N'.sys.tables WHERE name = ''ItemParamDet'')
    BEGIN
        INSERT INTO #BCNList(Barcode)
        SELECT DISTINCT LTRIM(RTRIM(CONVERT(NVARCHAR(100), ipd.BCN)))
        FROM ' + QUOTENAME(@db) + N'.dbo.ItemParamDet ipd
        WHERE LTRIM(RTRIM(CONVERT(NVARCHAR(100), ipd.C1))) = @DesignNo
          AND ISNULL(LTRIM(RTRIM(CONVERT(NVARCHAR(100), ipd.BCN))),'''') NOT IN ('''',''0'');
    END;';

    EXEC sp_executesql @sql, N'@DesignNo NVARCHAR(50)', @DesignNo=@DesignNo;
    FETCH NEXT FROM dbcur INTO @db;
END

CLOSE dbcur;
DEALLOCATE dbcur;

SELECT DISTINCT Barcode
INTO #BCNListFinal
FROM #BCNList
WHERE ISNULL(Barcode,'') NOT IN ('','0');

------------------------------------------------------------
-- 2) Purchase Party candidates (latest purchase per BCN)
------------------------------------------------------------
CREATE TABLE #PurCandidates
(
    DbName     SYSNAME,
    Barcode    NVARCHAR(100),
    VchDate    DATETIME,
    VchCode    INT,
    PartyName  NVARCHAR(255)
);

DECLARE dbcur2 CURSOR FAST_FORWARD FOR
SELECT name
FROM sys.databases
WHERE name LIKE 'BusyComp0001_db1%'
  AND state_desc = 'ONLINE';

OPEN dbcur2;
FETCH NEXT FROM dbcur2 INTO @db;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @sql = N'
    IF EXISTS (SELECT 1 FROM ' + QUOTENAME(@db) + N'.sys.tables WHERE name = ''ItemParamDet'')
       AND EXISTS (SELECT 1 FROM ' + QUOTENAME(@db) + N'.sys.tables WHERE name = ''Tran1'')
    BEGIN
        INSERT INTO #PurCandidates (DbName, Barcode, VchDate, VchCode, PartyName)
        SELECT
            N''' + REPLACE(@db,'''','''''') + N''' AS DbName,
            LTRIM(RTRIM(CONVERT(NVARCHAR(100), ipd.BCN))) AS Barcode,
            ipd.[Date] AS VchDate,
            ipd.VchCode,
            CASE
                WHEN p2.PrintName IS NOT NULL
                     AND LTRIM(RTRIM(p2.PrintName)) <> ''''
                     AND p2.PrintName NOT LIKE ''%WS%''
                     AND p2.PrintName NOT LIKE ''SSHN%''
                     AND p2.PrintName NOT LIKE ''SSHR%''
                     AND p2.PrintName NOT LIKE ''MAIN%''
                     AND p2.PrintName NOT LIKE ''PALLY%''
                     AND p2.PrintName NOT LIKE ''TALLY%''
                THEN LTRIM(RTRIM(p2.PrintName))
                ELSE LTRIM(RTRIM(p1.PrintName))
            END AS PartyName
        FROM ' + QUOTENAME(@db) + N'.dbo.ItemParamDet ipd
        LEFT JOIN ' + QUOTENAME(@db) + N'.dbo.Tran1 t1
               ON t1.VchType = ipd.VchType AND t1.VchCode = ipd.VchCode
        LEFT JOIN ' + QUOTENAME(@db) + N'.dbo.Master1 p2
               ON p2.Code = t1.MasterCode2
        LEFT JOIN ' + QUOTENAME(@db) + N'.dbo.Master1 p1
               ON p1.Code = t1.MasterCode1
        WHERE LTRIM(RTRIM(CONVERT(NVARCHAR(100), ipd.C1))) = @DesignNo
          AND ISNULL(LTRIM(RTRIM(CONVERT(NVARCHAR(100), ipd.BCN))),'''') NOT IN ('''',''0'')
          AND ipd.VchType = 2
          AND ISNULL(ipd.Value1,0) > 0
          AND ISNULL(ipd.VchCode,0) <> 0;
    END;';

    EXEC sp_executesql @sql, N'@DesignNo NVARCHAR(50)', @DesignNo=@DesignNo;
    FETCH NEXT FROM dbcur2 INTO @db;
END

CLOSE dbcur2;
DEALLOCATE dbcur2;

;WITH X AS
(
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY Barcode ORDER BY VchDate DESC, DbName DESC, VchCode DESC) AS rn
    FROM #PurCandidates
    WHERE ISNULL(LTRIM(RTRIM(PartyName)),'') <> ''
)
SELECT Barcode, PurchasePartyName = PartyName
INTO #PurchasePartyFinal
FROM X
WHERE rn = 1;

------------------------------------------------------------
-- 2B) Sales price candidates (latest D4 per BCN)
------------------------------------------------------------
CREATE TABLE #SalePriceCandidates
(
    DbName     SYSNAME,
    Barcode    NVARCHAR(100),
    VchDate    DATETIME,
    VchCode    INT,
    SalesPrice DECIMAL(18,2)
);

DECLARE dbcur3 CURSOR FAST_FORWARD FOR
SELECT name
FROM sys.databases
WHERE name LIKE 'BusyComp0001_db1%'
  AND state_desc = 'ONLINE';

OPEN dbcur3;
FETCH NEXT FROM dbcur3 INTO @db;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @sql = N'
    IF EXISTS (SELECT 1 FROM ' + QUOTENAME(@db) + N'.sys.tables WHERE name = ''ItemParamDet'')
    BEGIN
        INSERT INTO #SalePriceCandidates (DbName, Barcode, VchDate, VchCode, SalesPrice)
        SELECT
            N''' + REPLACE(@db,'''','''''') + N''' AS DbName,
            LTRIM(RTRIM(CONVERT(NVARCHAR(100), ipd.BCN))) AS Barcode,
            ipd.[Date] AS VchDate,
            ipd.VchCode,
            TRY_CONVERT(DECIMAL(18,2), ipd.D4) AS SalesPrice
        FROM ' + QUOTENAME(@db) + N'.dbo.ItemParamDet ipd
        WHERE LTRIM(RTRIM(CONVERT(NVARCHAR(100), ipd.C1))) = @DesignNo
          AND ISNULL(LTRIM(RTRIM(CONVERT(NVARCHAR(100), ipd.BCN))),'''') NOT IN ('''',''0'')
          AND TRY_CONVERT(DECIMAL(18,2), ipd.D4) IS NOT NULL
          AND TRY_CONVERT(DECIMAL(18,2), ipd.D4) > 0
          AND ISNULL(ipd.VchCode,0) <> 0;
    END;';

    EXEC sp_executesql @sql, N'@DesignNo NVARCHAR(50)', @DesignNo=@DesignNo;
    FETCH NEXT FROM dbcur3 INTO @db;
END

CLOSE dbcur3;
DEALLOCATE dbcur3;

;WITH S AS
(
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY Barcode ORDER BY VchDate DESC, DbName DESC, VchCode DESC) AS rn
    FROM #SalePriceCandidates
)
SELECT Barcode, SalesPrice
INTO #SalesPriceFinal
FROM S
WHERE rn = 1;

------------------------------------------------------------
-- 3) Closing stock from latest DB (ONLY AVAILABLE STOCK)
------------------------------------------------------------
SET @sql = N'
;WITH StockNow AS
(
    SELECT
        Store      = COALESCE(mc.PrintName, ''(No MC)''),

        Barcode    = LTRIM(RTRIM(CONVERT(NVARCHAR(100), ipd.BCN))),
        ClosingQty = CAST(SUM(CAST(ISNULL(ipd.Value1,0) AS DECIMAL(18,3))) AS DECIMAL(18,3))
    FROM ' + QUOTENAME(@LatestDb) + N'.dbo.ItemParamDet ipd
    LEFT JOIN ' + QUOTENAME(@LatestDb) + N'.dbo.Master1 mc
           ON mc.Code = ipd.MCCode
    WHERE LTRIM(RTRIM(CONVERT(NVARCHAR(100), ipd.C1))) = @DesignNo
      AND ISNULL(LTRIM(RTRIM(CONVERT(NVARCHAR(100), ipd.BCN))),'''') NOT IN ('''',''0'')
    GROUP BY
        COALESCE(mc.PrintName, ''(No MC)''), 
        LTRIM(RTRIM(CONVERT(NVARCHAR(100), ipd.BCN)))
)
SELECT
    LatestDB          = @LatestDb,
    DesignNo          = @DesignNo,
    Barcode           = b.Barcode,
    Store             = COALESCE(s.Store, ''(Not in Latest DB)''),

    ClosingQty        = COALESCE(s.ClosingQty, 0),
    PurchasePartyName = COALESCE(pp.PurchasePartyName, ''''),
    SalesPrice        = COALESCE(sp.SalesPrice, 0)

FROM #BCNListFinal b
LEFT JOIN StockNow s              ON s.Barcode = b.Barcode
LEFT JOIN #PurchasePartyFinal pp  ON pp.Barcode = b.Barcode
LEFT JOIN #SalesPriceFinal sp     ON sp.Barcode = b.Barcode

WHERE COALESCE(s.ClosingQty,0) > 0      -- ✅ ONLY AVAILABLE STOCK

ORDER BY Store, Barcode;
';

EXEC sp_executesql
    @sql,
    N'@DesignNo NVARCHAR(50), @LatestDb SYSNAME',
    @DesignNo=@DesignNo,
    @LatestDb=@LatestDb;
"""

def fetch_design_stock_all_dbs(design_no: str) -> list[dict]:
    design_no = (design_no or "").strip()
    if not design_no:
        return []

    with pyodbc.connect(SQL_CONN_STR) as conn:
        cur = conn.cursor()
        cur.execute(SQL_DESIGN_STOCK_ALLDB, (design_no,))
        cols = [c[0] for c in cur.description]
        out = []
        for row in cur.fetchall():
            d = dict(zip(cols, row))

            d["Barcode"] = str(d.get("Barcode") or "").strip()
            d["Store"] = str(d.get("Store") or "").strip()
            d["PurchasePartyName"] = str(d.get("PurchasePartyName") or "").strip()

            try:
                d["ClosingQty"] = float(d.get("ClosingQty") or 0)
            except Exception:
                d["ClosingQty"] = 0.0

            try:
                d["SalesPrice"] = float(d.get("SalesPrice") or 0)
            except Exception:
                d["SalesPrice"] = 0.0

            out.append(d)

        return out


@app.get("/api/design_lookup")
def api_design_lookup():
    try:
        design_no = (request.args.get("design_no") or "").strip()
        if not design_no:
            return jsonify({"ok": False, "error": "missing_design_no"}), 400

        rows = fetch_design_stock_all_dbs(design_no)

        # attach media per barcode (thumb + full)
        out_items = []
        for r in rows:
            bcn = (r.get("Barcode") or "").strip()
            media_items = _get_local_media_items_for_barcode(bcn)  # ✅ you already have this helper
            out_items.append({
                "barcode": bcn,
                "store": r.get("Store", ""),
                "qty": r.get("ClosingQty", 0),
                "purchase_party": r.get("PurchasePartyName", ""),
                "sales_price": r.get("SalesPrice", 0),
                "media": {
                    "count": len(media_items),
                    "items": media_items  # each has url + thumb_url
                }
            })

        return jsonify({
            "ok": True,
            "design_no": design_no,
            "count": len(out_items),
            "items": out_items
        }), 200

    except Exception as e:
        app.logger.exception("design_lookup error: %s", e)
        return jsonify({"ok": False, "error": "server_error"}), 500

def extract_text_from_pdf(pdf_path: str) -> str:
    """
    Extract all text from a PDF using PyPDF2.
    """
    try:
        from PyPDF2 import PdfReader
        reader = PdfReader(pdf_path)
        all_txt = []
        for p in reader.pages:
            all_txt.append(p.extract_text() or "")
        return "\n".join(all_txt)
    except Exception as e:
        app.logger.exception(f"[collection_pdf] extract_text_from_pdf failed: {e}")
        return ""

def parse_consolidated_summary_pdf(pdf_path: str) -> dict:
    """
    Parses Busy 'Consolidated Summary' PDF like:
      A - 1,06,200.00
      ATM
      MAIN 0.00
      SSHN 89,010.00
      ...

    Returns:
      {
        "date": "13-12-2025",
        "totals": {"CASH":Decimal, "ATM":Decimal, "UPI":Decimal, "UPI_OTHER":Decimal, "TOTAL":Decimal},
        "branches": {
            "MAIN":{"CASH":..,"ATM":..,"UPI":..,"UPI_OTHER":..},
            "SSHN":{...},
            ...
        }
      }
    """
    text = extract_text_from_pdf(pdf_path)
    if not text.strip():
        return {"date": "", "totals": {}, "branches": {}}

    # 1) Date (robust)
    # Sometimes extracted text truncates the "to" date like: "From 11-12-2025 to 11-1"
    m_from = re.search(r"\bFrom\s+(\d{2}-\d{2}-\d{4})\b", text, re.IGNORECASE)
    m_to   = re.search(r"\bto\s+(\d{2}-\d{2}-\d{4})\b", text, re.IGNORECASE)

    from_dt = (m_from.group(1) if m_from else "").strip()
    to_dt   = (m_to.group(1) if m_to else "").strip()

    # If "to" is missing/truncated, use "from"
    report_date = to_dt or from_dt

    # 2) Total at bottom: "Total 5,81,072.50"
    mtot = re.search(r"\bTotal\s+([\d,]+\.\d{2})\b", text, re.IGNORECASE)
    grand_total = _money_to_decimal(mtot.group(1)) if mtot else Decimal("0")

    # 3) Split sections by account headings in Busy PDF
    # We will detect blocks for ATM, Cash, UPI, UPI OTHER
    # Pattern: "A - <amt>\n<Heading>\n .... (branch lines) ...."
    def _extract_block(heading_regex: str):
        # returns (account_total_decimal, block_text)
        # use non-greedy until next "A -" OR end
        pat = re.compile(
            rf"A\s*-\s*([\d,]+\.\d{{2}})\s*[\r\n]+{heading_regex}\s*[\r\n]+(.*?)(?=(?:A\s*-\s*[\d,]+\.\d{{2}})|\bTotal\b|$)",
            re.IGNORECASE | re.DOTALL
        )
        m = pat.search(text)
        if not m:
            return Decimal("0"), ""
        return _money_to_decimal(m.group(1)), (m.group(2) or "")

    atm_total, atm_block = _extract_block(r"ATM")
    cash_total, cash_block = _extract_block(r"Cash")
    upi_total, upi_block = _extract_block(r"UPI(?!\s*OTHER)")
    upio_total, upio_block = _extract_block(r"UPI\s*OTHER")

    totals = {
        "CASH": cash_total,
        "ATM": atm_total,
        "UPI": upi_total,
        "UPI_OTHER": upio_total,
        "TOTAL": grand_total,
    }

    # 4) Parse branch lines: e.g. "SSHN 1,63,811.50"
    def _parse_branches(block_text: str) -> dict:
        out = {}
        # Matches: BRANCH AMOUNT
        # Branch can be MAIN / SSHN / SSHR / PALLY / TALLY / 0070 etc.
        for line in (block_text or "").splitlines():
            line = line.strip()
            if not line:
                continue
            # Skip weird standalone codes/headers
            if line.upper() in ("0070",):
                continue
            mm = re.match(r"^([A-Z0-9]+)\s+([\d,]+\.\d{2})$", line.upper())
            if mm:
                br = mm.group(1).strip()
                amt = _money_to_decimal(mm.group(2))
                out[br] = amt
        return out

    branches = {}
    for br, amt in _parse_branches(cash_block).items():
        branches.setdefault(br, {})["CASH"] = amt
    for br, amt in _parse_branches(atm_block).items():
        branches.setdefault(br, {})["ATM"] = amt
    for br, amt in _parse_branches(upi_block).items():
        branches.setdefault(br, {})["UPI"] = amt
    for br, amt in _parse_branches(upio_block).items():
        branches.setdefault(br, {})["UPI_OTHER"] = amt

    # Ensure all keys exist
    for br in branches:
        branches[br].setdefault("CASH", Decimal("0"))
        branches[br].setdefault("ATM", Decimal("0"))
        branches[br].setdefault("UPI", Decimal("0"))
        branches[br].setdefault("UPI_OTHER", Decimal("0"))

    return {"date": report_date, "totals": totals, "branches": branches}

def build_collection_message(name: str, date_str: str, totals: dict, branches: dict) -> str:
    """
    Minimal caption only.
    Detailed data is inside the PDF.
    """
    return "Today’s Collection Summary"

def generate_todays_collection_pdf(pdf_out_path: str, from_date: str, to_date: str,
                                  totals: dict, branches: dict) -> str:
    """
    Clean PDF with date range in title.
    """
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.lib import colors

        styles = getSampleStyleSheet()
        doc = SimpleDocTemplate(
            pdf_out_path,
            pagesize=A4,
            rightMargin=30,
            leftMargin=30,
            topMargin=30,
            bottomMargin=30
        )

        story = []

        # ✅ HEADER (DATE RANGE)
        title_range = from_date if from_date == to_date else f"{from_date} to {to_date}"
        story.append(Paragraph(
            f"<b>Today’s Collection Summary</b> ({title_range})",
            styles["Title"]
        ))
        story.append(Spacer(1, 12))

        # ===== TOTALS TABLE =====
        totals_table = [
            ["MODE", "AMOUNT"],
            ["A-Cash", _format_inr(totals.get("CASH"))],
            ["A-ATM", _format_inr(totals.get("ATM"))],
            ["A-UPI", _format_inr(totals.get("UPI"))],
            ["A-UPI Other", _format_inr(totals.get("UPI_OTHER"))],
            ["Grand Total", _format_inr(totals.get("TOTAL"))],
        ]

        t1 = Table(totals_table, colWidths=[220, 220])
        t1.setStyle(TableStyle([
            ("BACKGROUND", (0,0), (-1,0), colors.lightgrey),
            ("GRID", (0,0), (-1,-1), 0.5, colors.grey),
            ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
            ("FONTNAME", (0,-1), (-1,-1), "Helvetica-Bold"),
            ("BACKGROUND", (0,-1), (-1,-1), colors.whitesmoke),
            ("ALIGN", (1,1), (-1,-1), "RIGHT"),
        ]))

        story.append(t1)
        story.append(Spacer(1, 18))

        # ===== BRANCH-WISE TABLE =====
        story.append(Paragraph("<b>Branch-wise</b>", styles["Heading2"]))
        story.append(Spacer(1, 8))

        branch_rows = [
            ["Branch", "Cash", "ATM", "UPI", "UPI Other", "Total"]
        ]

        for br in sorted(branches.keys()):
            b = branches[br]
            branch_total = (
                b.get("CASH", 0)
                + b.get("ATM", 0)
                + b.get("UPI", 0)
                + b.get("UPI_OTHER", 0)
            )

            branch_rows.append([
                br,
                _format_inr(b.get("CASH")),
                _format_inr(b.get("ATM")),
                _format_inr(b.get("UPI")),
                _format_inr(b.get("UPI_OTHER")),
                _format_inr(branch_total),
            ])

        t2 = Table(
            branch_rows,
            colWidths=[70, 80, 80, 80, 90, 90]
        )

        t2.setStyle(TableStyle([
            ("BACKGROUND", (0,0), (-1,0), colors.lightgrey),
            ("GRID", (0,0), (-1,-1), 0.5, colors.grey),
            ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
            ("ALIGN", (1,1), (-1,-1), "RIGHT"),
        ]))

        story.append(t2)
        story.append(Spacer(1, 20))

        # ===== FOOTER =====
        story.append(Paragraph("— Subhash Saree House", styles["Normal"]))

        doc.build(story)
        return pdf_out_path

    except Exception as e:
        app.logger.exception(f"[collection_pdf] generate_todays_collection_pdf failed: {e}")
        return ""
from decimal import Decimal
import datetime
import os
import pyodbc

# ============================
# COLLECTION SUMMARY (SQL -> PDF -> BSP)
# ============================

COLLECTION_GROUP_ID = "120363367991085282"   # your WA group id
COLLECTION_TEMPLATE = "daily_collection_pdf"  # create this template in registry (1 var, DOCUMENT header)

# Your ledger MasterCode1 mappings
CASH_CODE = 1
ATM_CODE = 15188
UPI_CODE = 18884
UPI_OTHER_CODE = 81784


def fetch_collection_summary_sql(from_date: datetime.date, to_date: datetime.date) -> dict:
    """
    Uses your working SQL logic to return:
      {
        "date": "dd-mm-YYYY",
        "totals": {"CASH":Decimal, "ATM":Decimal, "UPI":Decimal, "UPI_OTHER":Decimal, "TOTAL":Decimal},
        "branches": {"MAIN": {...}, "SSHN": {...}, ...}
      }
    """
    sql = r"""
    DECLARE @FromDate date = ?;
    DECLARE @ToDate   date = ?;

    DECLARE @CASH int = ?;
    DECLARE @ATM  int = ?;
    DECLARE @UPI  int = ?;
    DECLARE @UPI_OTHER int = ?;

    ;WITH Txn AS (
        SELECT
            Dt = CONVERT(date, t1.[Date]),
            BranchName =
                CASE LEFT(LTRIM(RTRIM(t2.VchNo)), 1)
                  WHEN 'M' THEN 'MAIN'
                  WHEN 'S' THEN 'SSHN'
                  WHEN 'R' THEN 'SSHR'
                  WHEN 'P' THEN 'PALLY'
                  WHEN 'T' THEN 'TALLY'
                  ELSE 'OTHER'
                END,
            Mode = CASE t2.MasterCode1
                WHEN @CASH THEN 'CASH'
                WHEN @ATM  THEN 'ATM'
                WHEN @UPI  THEN 'UPI'
                WHEN @UPI_OTHER THEN 'UPI_OTHER'
            END,
            Amt = CAST(ISNULL(t2.Value1,0) AS decimal(18,2))
        FROM dbo.Tran1 t1
        JOIN dbo.Tran2 t2 ON t2.VchCode = t1.VchCode
        WHERE t1.[Date] >= @FromDate
          AND t1.[Date] < DATEADD(day,1,@ToDate)
          AND t2.MasterCode1 IN (@CASH,@ATM,@UPI,@UPI_OTHER)
          AND ISNULL(t2.Value1,0) <> 0
    ),
    Agg AS (
        SELECT
            BranchName,
            CASH_OUT = SUM(CASE WHEN Mode='CASH'      AND Amt < 0 THEN -Amt ELSE 0 END),
            ATM_OUT  = SUM(CASE WHEN Mode='ATM'       AND Amt < 0 THEN -Amt ELSE 0 END),
            UPI_OUT  = SUM(CASE WHEN Mode='UPI'       AND Amt < 0 THEN -Amt ELSE 0 END),
            UPI_OTHER_OUT = SUM(CASE WHEN Mode='UPI_OTHER' AND Amt < 0 THEN -Amt ELSE 0 END)
        FROM Txn
        GROUP BY BranchName
    ),
    Final AS (
        SELECT
            SortOrder = 1,
            BranchName,
            CASH_OUT,
            ATM_OUT,
            UPI_OUT,
            UPI_OTHER_OUT,
            TOTAL_OUT = (CASH_OUT + ATM_OUT + UPI_OUT + UPI_OTHER_OUT)
        FROM Agg

        UNION ALL

        SELECT
            2,
            'TOTAL',
            SUM(CASH_OUT),
            SUM(ATM_OUT),
            SUM(UPI_OUT),
            SUM(UPI_OTHER_OUT),
            SUM(CASH_OUT + ATM_OUT + UPI_OUT + UPI_OTHER_OUT)
        FROM Agg
    )
    SELECT
        SortOrder,
        BranchName AS Branch,
        CASH_OUT   AS Cash,
        ATM_OUT    AS ATM,
        UPI_OUT    AS UPI,
        UPI_OTHER_OUT AS UPIOther,
        TOTAL_OUT  AS Total
    FROM Final
    ORDER BY SortOrder, Branch;
    """

    params = (
        from_date.strftime("%Y-%m-%d"),
        to_date.strftime("%Y-%m-%d"),
        CASH_CODE, ATM_CODE, UPI_CODE, UPI_OTHER_CODE
    )

    with pyodbc.connect(SQL_CONN_STR) as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()

    # Build dicts
    branches = {}
    totals = {"CASH": Decimal("0"), "ATM": Decimal("0"), "UPI": Decimal("0"), "UPI_OTHER": Decimal("0"), "TOTAL": Decimal("0")}

    for r in rows:
        br = (r.Branch or "").strip().upper()
        cash = Decimal(str(r.Cash or 0))
        atm  = Decimal(str(r.ATM or 0))
        upi  = Decimal(str(r.UPI or 0))
        upio = Decimal(str(r.UPIOther or 0))
        tot  = Decimal(str(r.Total or 0))

        if br == "TOTAL":
            totals["CASH"] = cash
            totals["ATM"] = atm
            totals["UPI"] = upi
            totals["UPI_OTHER"] = upio
            totals["TOTAL"] = tot
        else:
            branches[br] = {"CASH": cash, "ATM": atm, "UPI": upi, "UPI_OTHER": upio}

    date_str = to_date.strftime("%d-%m-%Y")
    return {"date": date_str, "totals": totals, "branches": branches}

def queue_collection_summary_sql(from_date: datetime.date | None = None,
                                 to_date: datetime.date | None = None) -> dict:
    """
    1) SQL -> totals/branches
    2) Create clean PDF
    3) Send via BSP template (DOCUMENT header) to group
    """
    from_date = from_date or datetime.date.today()
    to_date   = to_date or datetime.date.today()

    parsed = fetch_collection_summary_sql(from_date, to_date)

    date_str = parsed["date"]
    totals = parsed["totals"]
    branches = parsed["branches"]

    # Build PDF
    out_name = f"todays_collection_sql_{date_str.replace('-','')}_{datetime.datetime.now().strftime('%H%M%S')}.pdf"
    out_path = os.path.join(PDF_DIR, out_name)

    pdf_path = generate_todays_collection_pdf(
        pdf_out_path=out_path,
        from_date=from_date,   # "dd-mm-YYYY"
        to_date=to_date,       # "dd-mm-YYYY"
        totals=totals,
        branches=branches,
    )

    if not pdf_path or not os.path.exists(pdf_path):
        raise RuntimeError("collection PDF not created")

    # Build message text (same style you showed)
    msg_text = build_collection_message(
        name="Team",
        date_str=date_str,
        totals=totals,
        branches=branches
    )

    # Send to group (via BSP template with 1 var and DOCUMENT header)
    to_id = resolve_receiver(COLLECTION_GROUP_ID) or COLLECTION_GROUP_ID

    tinfo = _template_registry.get(COLLECTION_TEMPLATE, {})
    language_code = (tinfo.get("language") or "en").strip()

    # IMPORTANT: This assumes COLLECTION_TEMPLATE has vars=1
    values = [msg_text]

    job_id = enqueue_send_job(
        to=to_id,
        template_name=COLLECTION_TEMPLATE,
        language_code=language_code,
        values=values,
        pdf_link=None,
        image_link=None,
        video_link=None,
        pdf_path=pdf_path,          # local document
        raw_text=msg_text,          # for webhook/fallback rebuild
        delay_seconds=0,
    )

    app.logger.info("[collection_sql] queued to=%s job_id=%s pdf=%s", to_id, job_id, pdf_path)
    return {"ok": True, "job_id": job_id, "pdf": os.path.basename(pdf_path), "date": date_str}

@app.route("/busy_send", methods=["POST", "GET"])
def busy_send():
    # ==== DEBUG: dump what Busy sent ====
    try:
        raw_body = request.get_data(as_text=True)  # works for both GET/POST (form/json)
    except Exception:
        raw_body = "<unreadable>"

    files_meta = {}
    try:
        for k in request.files:
            f = request.files[k]
            files_meta[k] = {"filename": getattr(f, "filename", ""), "content_type": getattr(f, "content_type", "")}
    except Exception:
        pass

    show_headers = {}
    try:
        for hk, hv in request.headers.items():
            hkl = hk.lower()
            if hkl in ("content-type", "content-length", "user-agent", "x-busy-token", "authorization"):
                show_headers[hk] = hv
    except Exception:
        pass

    debug_dump = {
        "method": request.method,
        "path": request.path,
        "remote_addr": request.remote_addr,
        "args": request.args.to_dict(flat=False),
        "form": request.form.to_dict(flat=False),
        "files": files_meta,
        "headers": show_headers,
    }
    import json as _json
    app.logger.info("[busy_debug] " + _json.dumps(debug_dump, ensure_ascii=False))

    if request.args.get("debug") == "1":
        return jsonify({"_echo": debug_dump}), 200
    # =====================================

    try:
        # --- Auth check ---
        token_req = request.form.get("authToken") or request.args.get("authToken")
        if BUSY_TOKEN and token_req != BUSY_TOKEN:
            return jsonify({"error": "unauthorized", "detail": "Invalid Busy authToken"}), 401

        # --- Basic fields ---
        sender_id   = (request.form.get("senderId") or request.args.get("senderId") or "").strip()
        raw_receiver = (request.form.get("receiverId") or request.args.get("receiverId") or "").strip()
        receiver_id = resolve_receiver(raw_receiver)
        if not receiver_id:
            return jsonify({"error": "invalid_receiver", "detail": f"Invalid receiver ID format: {raw_receiver}"}), 400

        message_txt = (request.form.get("messageText") or request.args.get("messageText") or "").strip()

        if not receiver_id:
            return jsonify({"error": "missing_receiverId"}), 400
        if not message_txt:
            return jsonify({"error": "missing_messageText"}), 400

        # Helper: fetch ANY uploaded files (pdf/jpg/jpeg),
        # save ALL of them, and merge them into ONE voucher PDF.
        # Returns:
        #   - primary_path: first saved file path (WhatsApp media)
        #   - info_list: results from DB save/merge
        def _handle_uploaded_files_for_voucher():
            saved_paths = []

            # STEP 1 — Collect ALL uploaded files under ANY key
            for field_name, file_list in request.files.lists():
                for file_obj in file_list:
                    if not file_obj or not file_obj.filename:
                        continue

                    fname = secure_filename(file_obj.filename)
                    saved = os.path.join(PDF_DIR, fname)
                    file_obj.save(saved)
                    saved_paths.append(saved)
                    app.logger.info(f"[busy] uploaded file saved: {saved}")

            if not saved_paths:
                return None, []

            # STEP 2 — First file determines voucher (VchNo, VchCode)
            first_path = saved_paths[0]
            first_name = os.path.basename(first_path)

            info_list = []
            info_first = _save_voucher_document_to_db(first_name, first_path)
            info_list.append(info_first)

            if not info_first.get("ok"):
                # If voucher not found from first file, skip the rest
                app.logger.warning(f"[busy_send] first file could not resolve voucher: {info_first}")
                return first_path, info_list

            vch_code_hint = info_first.get("vch_code")
            vch_type_hint = info_first.get("vch_type")

            # STEP 3 — Remaining files MUST be merged into SAME voucher PDF
            for extra_path in saved_paths[1:]:
                extra_name = os.path.basename(extra_path)
                info_extra = _save_voucher_document_to_db(
                    extra_name,
                    extra_path,
                    vch_code_hint=vch_code_hint,
                    vch_type_hint=vch_type_hint,
                )
                info_list.append(info_extra)
                app.logger.info(f"[busy_send] merged extra file {extra_name}: {info_extra}")

            return first_path, info_list

        # Decide if this looks like a template message
        mt_clean = (message_txt or "").strip()
        looks_like_template = (
            "|" in mt_clean or
            any(k in mt_clean.lower() for k in ("t=","template=","img=","image=","doc=","pdf=","vid=","video=","lang=","l="))
        )

        # ================================
        # A) NON-TEMPLATE → SPECIAL: Daily cash sale consolidated PDF
        # ================================
        if not looks_like_template:
            local_path, info_list = _handle_uploaded_files_for_voucher()
            app.logger.info(f"[busy_send] non-template upload merge results: {info_list}")

            # If Busy is sending consolidated collection PDF with message "Daily cash sale"
            mt_low = (mt_clean or "").strip().lower()
            is_daily_cash_sale = mt_low in ("daily cash sale", "daily cash", "today cash sale", "todays collection", "today collection")

            if is_daily_cash_sale and local_path and local_path.lower().endswith(".pdf"):
                parsed = parse_consolidated_summary_pdf(local_path)

                # Safety: if parsing failed, do NOT send blank
                if not parsed.get("totals") or parsed["totals"].get("TOTAL", Decimal("0")) == Decimal("0"):
                    app.logger.warning(f"[collection_pdf] Parsing failed or TOTAL=0, sending ORIGINAL pdf. src={local_path}")
                    # Fallback to original PDF (better than blank)
                    if _fallback_can_send():
                        sc, body = send_fallback_with_sales_extras(
                            receiver_id,
                            "Today’s Collection Summary",
                            media_url=None,
                            local_path=local_path,
                            template_name="daily_collection_original",
                        )
                        return jsonify({"routed":"fallback","status": sc, "response": body}), (200 if 200 <= sc < 300 else 502)

                    return jsonify({"error": "fallback_not_configured"}), 400

                date_str = parsed.get("date") or datetime.datetime.now().strftime("%d-%m-%Y")
                out_name = f"todays_collection_summary_{date_str.replace('-','')}_{datetime.datetime.now().strftime('%H%M%S')}.pdf"
                out_path = os.path.join(PDF_DIR, out_name)

                new_pdf = generate_todays_collection_pdf(
                    pdf_out_path=out_path,
                    title_date=date_str,
                    totals=parsed["totals"],
                    branches=parsed["branches"],
                )

                if not new_pdf or not os.path.exists(new_pdf):
                    app.logger.warning(f"[collection_pdf] New PDF not created, sending ORIGINAL pdf. src={local_path}")
                    if _fallback_can_send():
                        sc, body = send_fallback_with_sales_extras(
                            receiver_id,
                            "Today’s Collection Summary",
                            media_url=None,
                            local_path=local_path,
                            template_name="daily_collection_original",
                        )
                        return jsonify({"routed":"fallback","status": sc, "response": body}), (200 if 200 <= sc < 300 else 502)

                    return jsonify({"error": "fallback_not_configured"}), 400

                # ✅ SEND ONLY NEW FORMATTED PDF (as you asked)
                # (Text optional; WhatsApp media message needs some caption - keep minimal)
                caption = "Today’s Collection Summary"
                if _fallback_can_send():
                    sc, body = send_fallback_with_sales_extras(
                        receiver_id,
                        caption,
                        media_url=None,
                        local_path=new_pdf,
                        template_name="daily_collection_newpdf",
                    )
                    return jsonify({"routed":"fallback","status": sc, "response": body, "pdf": os.path.basename(new_pdf)}), (200 if 200 <= sc < 300 else 502)

                return jsonify({"error": "fallback_not_configured"}), 400

            # ---- Normal non-template behaviour (your old logic) ----
            if _fallback_can_send():
                sc, body = send_fallback_with_sales_extras(
                    receiver_id,
                    mt_clean,
                    media_url=None,
                    local_path=local_path,
                    template_name=mt_clean.split("|", 1)[0] if "|" in mt_clean else mt_clean,
                )
                return jsonify({"routed":"fallback","status": sc, "response": body}), (200 if 200 <= sc < 300 else 502)

            return jsonify({
                "error": "template_required",
                "detail": "This endpoint expects a template or a configured BotMaster fallback. For pure text use /busy/otp."
            }), 400

        # ================================
        # B) TEMPLATE FLOW (BSP) — with fallback if unknown template name
        # ================================
        # ✅ IMPORTANT RULE:
        # Template messages should NOT upload or merge any files into SQL
        pdf_path, info_list = _handle_uploaded_files_for_voucher()
        app.logger.info(f"[busy_send] template upload merge results: {info_list}")
        # Parse into template + values (+ optional media hints)
        template_name = None
        language_code = None
        values = []
        image_link = None
        video_link = None
        pdf_link = None

        if "|" in mt_clean:
            parts = mt_clean.split("|")
            tpl_raw = parts[0] if parts else ""
            values_raw = parts[1:] if len(parts) > 1 else []
            template_name, values = normalize_template_and_values(tpl_raw, values_raw)
        else:
            chunks = [c for c in mt_clean.replace(";", " ").split() if c]
            for c in chunks:
                if "=" in c:
                    k, v = c.split("=", 1)
                    k = k.strip().upper(); v = v.strip()
                    if k in ("T", "TEMPLATE"): template_name = v.lower()
                    elif k in ("L", "LANG", "LANGUAGE"): language_code = v
                    elif k in ("IMG", "IMAGE"): image_link = v
                    elif k in ("DOC", "PDF", "PDF_LINK"): pdf_link = v
                    elif k in ("VID", "VIDEO"): video_link = v
                else:
                    if not template_name: template_name = c.lower()
            template_name, values = normalize_template_and_values(template_name, [])

        if not template_name:
            template_name = (TEMPLATE_DEFAULT or "invoice1").strip().lower()

        # Unknown template → route via BotMaster fallback immediately
        if template_name not in _template_registry:
            if not _fallback_can_send():
                return jsonify({"error": "unknown_template",
                                "detail": f"Template '{template_name}' not in registry and fallback not configured."}), 400
            fb_text = _values_to_text(template_name, values or [])
            fb_media = pdf_link or image_link or video_link or None
            local_path_for_fb = None if fb_media else (pdf_path or None)
            delay_sec = delay_minutes_for_template(template_name) * 60

            def _send_fb():
                sc, body = send_fallback_with_sales_extras(
                    receiver_id,
                    fb_text,
                    media_url=fb_media,
                    local_path=local_path_for_fb,
                    template_name=template_name,
                )
                app.logger.info(f"[fallback] sent tpl={template_name} to={receiver_id} sc={sc}")

            if delay_sec > 0:
                threading.Timer(delay_sec, _send_fb).start()
                app.logger.info(f"[fallback] scheduled in {delay_sec}s tpl={template_name} to={receiver_id}")
                return jsonify({"routed":"fallback","scheduled_in_sec": delay_sec}), 202
            else:
                _send_fb()
                return jsonify({"routed":"fallback","status":200}), 200

        # Template exists → continue BSP flow
        tinfo = _template_registry.get(template_name, {})
        language_final = (language_code or tinfo.get("language") or "en").strip()

        # ----- FIXED COUPON VIDEOS (override any Busy media/file) -----
        fixed_video = COUPON_VIDEO_MAP.get(template_name)
        if fixed_video:
            pdf_link = None
            image_link = None
            video_link = None
            pdf_path = fixed_video

        # ==== MEDIA SELECTION & VALIDATION ====
        expected_header = get_template_header_type(template_name)  # 'document'|'image'|'video'|'none'

        local_kind = detect_local_media_kind(pdf_path) if pdf_path else "none"
        provided_kind = (
            local_kind
            if local_kind != "none"
            else (
                "image"    if image_link else
                "document" if pdf_link   else
                ("video"   if video_link else "none")
            )
        )

        # If template expects media and none provided, try default link from registry
        if provided_kind == "none" and expected_header in ("document","image","video"):
            default_link = get_template_default_media_link(template_name)
            if default_link:
                if expected_header == "document": pdf_link = default_link
                elif expected_header == "image":   image_link = default_link
                elif expected_header == "video":   video_link = default_link
                provided_kind = expected_header
                app.logger.info(f"[busy] using registry default media for {template_name}: {default_link}")
            else:
                return jsonify({"error": "media_required",
                                "detail": f"Template '{template_name}' expects a {expected_header.upper()} header but no media provided and no default_media_link found."}), 400

        # Enforce header type match
        if expected_header in ("document","image","video"):
            if provided_kind not in ("none", expected_header):
                return jsonify({"error": "header_mismatch",
                                "detail": f"Template '{template_name}' expects {expected_header.upper()} header but received {provided_kind.upper()}."}), 400

        app.logger.info(f"[busy_send] to={receiver_id} tpl={template_name} lang={language_final} values={values}")

        delay_sec = delay_minutes_for_template(template_name) * 60
        if delay_sec > 0:
            app.logger.info(f"[queue] scheduling send in {delay_sec}s for tpl={template_name}")

        job_id = enqueue_send_job(
            to=receiver_id,
            template_name=template_name,
            language_code=language_final,
            values=values or [],
            pdf_link=(pdf_link or None),
            image_link=(image_link or None),
            video_link=(video_link or None),
            pdf_path=(pdf_path or None) if (not pdf_link and not image_link and not video_link) else None,
            raw_text=message_txt,
            delay_seconds=delay_sec,
        )

        # If this is a coupon template (name ends with 'cup'), record it for expiry reminders
        if template_name.endswith("cup"):
            try:
                if values:
                    act = values[0] or ""
                    m = re.match(r"^(.*?)(\s*\(\s*\+?\d.*\))\s*$", act)
                    if m:
                        raw_name = (m.group(1) or "").strip()
                        tail     = m.group(2)
                        norm_name = normalize_name_with_ji(raw_name)
                        values[0] = f"{norm_name} {tail}".strip()
                    else:
                        values[0] = normalize_name_with_ji(act)

                coupon_code = (values[1] if len(values) >= 2 else "").strip()
                coupon_exp  = (values[-1] if len(values) >= 1 else "").strip()

                if coupon_code and coupon_exp:
                    _record_coupon_send(
                        to_msisdn=receiver_id,
                        coupon_code=coupon_code,
                        expires_on=coupon_exp,
                        template_name=template_name,
                        language_code=language_final,
                        values=values or [],
                        media_local=EXPIRY_COUPON_VIDEO,
                        meta={"source": "busy_send", "job_id": job_id}
                    )
                else:
                    app.logger.warning(f"[coupon] cannot record (missing code/expiry) tpl={template_name} values={values}")
            except Exception:
                app.logger.exception("[coupon] auto-record failed")

        return jsonify({"queued": True, "job_id": job_id, "status": "queued",
                        "note": "Message queued for background delivery."}), 202

    except Exception as e:
        app.logger.exception("busy_send exception")
        return jsonify({"error": "busy_send_failed", "detail": str(e)}), 500
def _send_template_wrapper(to, template_name, language_code, values):
    payload = {
        "senderId": "BUSY123",   # or your default senderId
        "receiverId": to,
        "messageText": f"{template_name}|{'|'.join(values)}",
        "authToken": BUSY_TOKEN
    }

    with app.test_request_context(
        "/busy_send",
        method="POST",
        data=payload
    ):
        resp = busy_send()
        if isinstance(resp, tuple):
            return resp[1], resp[0]  # status, body
        return 200, resp
app.config["SEND_TEMPLATE_FUNC"] = _send_template_wrapper
@app.route("/test_points_expiry", methods=["GET"])
def test_points_expiry():
    try:
        test_data = {
            "to": "917828580401",
            "template_name": "sshptsexp5",
            "language_code": "hi",
            "values": [
                "TEST CUSTOMER (917828580401)",
                "100",
                "05-12-2025",
                "1200"
            ]
        }

        # Monkey-patch request.get_json() to return our test_data
        request.get_json = lambda force=True: test_data

        # Call your existing function
        return send_whatsapp_invoice()

    except Exception as e:
        app.logger.exception("test_points_expiry failed")
        return {"error": str(e)}, 500


# ---------------------------
# Busy OTP → Plain text send
# ---------------------------
@app.get("/api/bcn_thumb")
def api_bcn_thumb():
    """
    Returns thumbnail strictly from:
      C:\BusyWin\IMAGES\BCN\_thumbs

    Must be mapped into Flask static folder as:
      <project>\static\BCN\_thumbs\

    Response:
      {
        ok: true/false,
        barcode: "801801",
        thumb_url: "http://.../static/BCN/_thumbs/801801.jpg",
        url: "http://.../static/BCN/801801.jpg"
      }
    """
    try:
        barcode = _clean_barcode(request.args.get("barcode") or "")
        if not barcode:
            return jsonify({"ok": False, "error": "missing_barcode"}), 400

        # ✅ Your real folders (outside project)
        base_dir = r"C:\BusyWin\IMAGES\BCN"
        thumbs_dir = os.path.join(base_dir, "_thumbs")

        # ✅ thumb candidates
        thumb_candidates = [
            os.path.join(thumbs_dir, f"{barcode}.jpg"),
            os.path.join(thumbs_dir, f"{barcode}.jpeg"),
            os.path.join(thumbs_dir, f"{barcode}.png"),
            os.path.join(thumbs_dir, f"{barcode}.webp"),
        ]

        thumb_path = next((p for p in thumb_candidates if os.path.exists(p)), "")

        # ✅ full image candidates
        full_candidates = [
            os.path.join(base_dir, f"{barcode}.jpg"),
            os.path.join(base_dir, f"{barcode}.jpeg"),
            os.path.join(base_dir, f"{barcode}.png"),
            os.path.join(base_dir, f"{barcode}.webp"),
        ]
        full_path = next((p for p in full_candidates if os.path.exists(p)), "")

        # ----------------------------
        # IMPORTANT:
        # Your Flask must actually SERVE these files via /static
        # So we create (or reuse) copies/symlinks under app.static_folder
        # ----------------------------
        static_bcn_dir = os.path.join(app.static_folder, "BCN")
        static_thumbs_dir = os.path.join(static_bcn_dir, "_thumbs")
        os.makedirs(static_thumbs_dir, exist_ok=True)

        def _ensure_static_copy(src_path: str, dst_dir: str) -> str:
            """Copy file into static folder if not already copied or size differs."""
            if not src_path or not os.path.exists(src_path):
                return ""
            dst_path = os.path.join(dst_dir, os.path.basename(src_path))
            try:
                if (not os.path.exists(dst_path)) or (os.path.getsize(dst_path) != os.path.getsize(src_path)):
                    shutil.copy2(src_path, dst_path)
                return dst_path
            except Exception:
                return ""

        static_thumb = _ensure_static_copy(thumb_path, static_thumbs_dir)
        static_full  = _ensure_static_copy(full_path, static_bcn_dir)

        thumb_url = _to_static_url(static_thumb)
        full_url  = _to_static_url(static_full)

        if not thumb_url and not full_url:
            return jsonify({"ok": False, "error": "not_found", "barcode": barcode}), 200

        return jsonify({
            "ok": True,
            "barcode": barcode,
            "thumb_url": thumb_url,
            "url": full_url or thumb_url
        }), 200

    except Exception as e:
        app.logger.exception("api_bcn_thumb error: %s", e)
        return jsonify({"ok": False, "error": "server_error"}), 500

@app.route("/busy/otp", methods=["GET","POST"])
def busy_otp_plain_text():
    try:
        data = {}
        if request.is_json:
            data = request.get_json(silent=True) or {}

        to = resolve_receiver(
            data.get("to") or data.get("mobile") or
            request.form.get("to") or request.form.get("mobile") or
            request.args.get("to") or request.args.get("mobile") or ""
        )
        if not to:
            return jsonify({"ok": False, "error": "Invalid receiver ID format"}), 400

        text = (data.get("text") or data.get("message") or
                request.form.get("text") or request.form.get("message") or
                request.args.get("text") or request.args.get("message") or "").strip()

        if BUSY_TOKEN:
            tok = (request.headers.get("X-Busy-Token") or
                   request.form.get("authToken") or
                   request.args.get("authToken") or
                   data.get("authToken") or "")
            if tok != BUSY_TOKEN:
                return jsonify({"ok": False, "error": "unauthorized"}), 401

        if not text:
            return jsonify({"ok": False, "error": "Missing 'text' (or 'message')"}), 400

        status, jr = send_plain_text(to, text)
        ok = 200 <= status < 300
        return jsonify({"ok": ok, "status": status, "bsp_response": jr}), (200 if ok else 502)

    except Exception as e:
        app.logger.exception("busy_otp_plain_text error")
        return jsonify({"ok": False, "error": str(e)}), 500

@app.get("/_tail")
def tail_log():
    n = int(request.args.get("n", 200))
    path = os.path.join(LOG_DIR, "server.log")
    if not os.path.exists(path):
        return jsonify({"error": "no_log_file"}), 404
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        lines = f.readlines()[-n:]
    return "<pre>" + "".join(lines).replace("<", "&lt;").replace(">", "&gt;") + "</pre>"

@app.route('/preview/points_expiry', methods=['GET'])
def preview_points_expiry():
    """
    Preview customers whose points are expiring in X days.
    Default: show both (10 and 5 days)
    Usage:
        /preview/points_expiry
        /preview/points_expiry?days=10
        /preview/points_expiry?days=5
    """
    try:
        days_param = request.args.get("days", "").strip()

        def fetch_preview(days):
            target_date = datetime.date.today() + datetime.timedelta(days=days)

            sql = """
            WITH ExpToday AS (
                SELECT 
                    kp.MasterCode1,
                    SUM(kp.Points) AS ExpiringPoints,
                    MIN(CONVERT(date, kp.Duedate)) AS ExpiryDate
                FROM dbo.KPSPoints kp
                WHERE 
                    kp.Points > 0
                    AND CONVERT(date, kp.Duedate) = CONVERT(date, ?)
                GROUP BY kp.MasterCode1
            ),
            TotalPts AS (
                SELECT 
                    kp.MasterCode1,
                    SUM(kp.Points) AS TotalPoints
                FROM dbo.KPSPoints kp
                WHERE 
                    CONVERT(date, kp.Duedate) >= CONVERT(date, GETDATE())
                GROUP BY kp.MasterCode1
            )
            SELECT
                m.PrintName  AS CustomerName,
                m.Alias      AS Mobile,
                e.ExpiringPoints,
                t.TotalPoints,
                CONVERT(varchar(10), e.ExpiryDate, 105) AS ExpiryDateText   -- dd-mm-yyyy
            FROM ExpToday e
            JOIN TotalPts t ON e.MasterCode1 = t.MasterCode1
            JOIN dbo.Master1 m ON m.Code = e.MasterCode1
            WHERE 
                e.ExpiringPoints <= t.TotalPoints
                AND ISNULL(m.Alias,'') <> ''
            ORDER BY e.ExpiringPoints DESC;
            """

            with pyodbc.connect(SQL_CONN_STR) as conn:
                cur = conn.cursor()
                target_date_str = target_date.strftime("%Y-%m-%d")
                cur.execute(sql, (target_date_str,))
                cols = [c[0] for c in cur.description]
                data = [dict(zip(cols, r)) for r in cur.fetchall()]

            return data

        if days_param:
            days = int(days_param)
            return jsonify({
                "days": days,
                "preview": fetch_preview(days)
            }), 200

        else:
            return jsonify({
                "days_10": fetch_preview(10),
                "days_5": fetch_preview(5)
            }), 200

    except Exception as e:
        app.logger.exception("preview_points_expiry error")
        return jsonify({"error": str(e)}), 500

# ==========================================
# NEW ROUTE: Get Salesmen + Helpers from SQL
# ==========================================
@app.route('/sales_people', methods=['GET'])
def sales_people():
    """
    Returns a merged list of Salesmen and Helpers from Busy SQL.
    - Salesmen: from Master1 (MasterType=19)
    - Helpers: from TmpTran2.HelperName (recent 365 days)
    """
    try:
        conn = pyodbc.connect(SQL_CONN_STR)
        cur = conn.cursor()

        # Salesmen from Master1
        cur.execute("""
            SELECT DISTINCT LTRIM(RTRIM(Name)) AS Name
            FROM dbo.Master1
            WHERE MasterType = 19
              AND ISNULL(LTRIM(RTRIM(Name)),'') <> ''
        """)
        salesmen = sorted({(r[0] or '').strip() for r in cur.fetchall()})

        # Helpers from TmpTran2.HelperName (use same tables as your working route)
        cur.execute("""
            SELECT DISTINCT LTRIM(RTRIM(t2.HelperName)) AS HelperName
            FROM dbo.TmpTran2 t2
            JOIN dbo.Tran1 t1 ON t1.VchCode = t2.Vchcode
            WHERE ISNULL(LTRIM(RTRIM(t2.HelperName)),'') <> ''
              AND t1.Date >= DATEADD(day, -365, CAST(GETDATE() AS date))
        """)
        helpers = sorted({(r[0] or '').strip() for r in cur.fetchall()})

        conn.close()

        all_people = sorted(set(salesmen) | set(helpers))
        return jsonify({
            "salesmen": salesmen,
            "helpers": helpers,
            "all": all_people,
            "count": len(all_people)
        })

    except Exception as e:
        return jsonify({"error": f"Failed to fetch names: {e}"}), 500

# -------------------------------------------------------
# Manual triggers (existing)
# -------------------------------------------------------
@app.route("/trigger/collection_sql", methods=["GET", "POST"])
def trigger_collection_sql():
    """
    Manual trigger for SQL-based Collection Summary -> PDF -> WhatsApp (group).

    Usage:
      /trigger/collection_sql
      /trigger/collection_sql?date=13-12-2025
      /trigger/collection_sql?from=13-12-2025&to=15-12-2025

    JSON (POST) also supported:
      {"date":"13-12-2025"}   OR   {"from":"13-12-2025","to":"15-12-2025"}
    """
    try:
        args = request.values or {}
        body = {}
        if request.is_json:
            body = request.get_json(silent=True) or {}

        date_str = (args.get("date") or body.get("date") or "").strip()
        from_str = (args.get("from") or body.get("from") or "").strip()
        to_str   = (args.get("to")   or body.get("to")   or "").strip()

        # ---- resolve dates ----
        if date_str and (from_str or to_str):
            return jsonify({"ok": False, "error": "Use either date= OR from/to, not both."}), 400

        if date_str:
            try:
                d = datetime.datetime.strptime(date_str, "%d-%m-%Y").date()
            except ValueError:
                return jsonify({"ok": False, "error": "Invalid date. Use dd-mm-YYYY"}), 400
            from_date = d
            to_date = d
        else:
            if not from_str and not to_str:
                today = datetime.date.today()
                from_date = today
                to_date = today
            else:
                if not from_str:
                    from_str = to_str
                if not to_str:
                    to_str = from_str
                try:
                    from_date = datetime.datetime.strptime(from_str, "%d-%m-%Y").date()
                    to_date   = datetime.datetime.strptime(to_str, "%d-%m-%Y").date()
                except ValueError:
                    return jsonify({"ok": False, "error": "Invalid from/to. Use dd-mm-YYYY"}), 400

        # ---- run job ----
        result = queue_collection_summary_sql(from_date=from_date, to_date=to_date)
        return jsonify(result), 200

    except Exception as e:
        app.logger.exception("trigger_collection_sql error")
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route('/trigger/rebuild_salesman_file', methods=['GET'])
def trigger_rebuild_salesman_file():
    days = int(request.args.get("days", "45"))
    ok, count = rebuild_salesman_file(window_days=days, vch_type=9)
    if ok:
        return jsonify({"ok": True, "written": count, "file": SALESMAN_FILE, "window_days": days})
    return jsonify({"ok": False, "error": "failed"}), 500

@app.route('/trigger/upi_report', methods=['GET'])
def trigger_upi_report():
    queue_upi_other_report(); return jsonify({"status": "manually triggered", "task": "UPI Report"})
@app.route("/trigger/sales_reports_window", methods=["GET", "POST"])
def trigger_sales_reports_window():
    """
    Manually trigger summary commission reports for a custom date window.

    Usage:
        /trigger/sales_reports_window?from=01-12-2025&to=11-12-2025
    """
    try:
        # ----- read input dates (dd-mm-YYYY) -----
        args = request.values or {}
        body = {}
        if request.is_json:
            body = request.get_json(silent=True) or {}

        from_str = (args.get("from") or body.get("from") or "").strip()
        to_str   = (args.get("to")   or body.get("to")   or "").strip()

        # default both to today if missing
        if not from_str and not to_str:
            today = datetime.datetime.now().strftime("%d-%m-%Y")
            from_str = today
            to_str   = today
        elif not from_str:
            from_str = to_str
        elif not to_str:
            to_str = from_str

        # ----- find active recipients in that window -----
        recipients = get_active_recipients(from_str, to_str, vch_type=9)

        pdf_ok = 0
        send_ok = 0
        send_fail = 0

        for p in recipients:
            role   = p.get("kind", "salesman")  # 'salesman' or 'helper'
            name   = (p.get("name")   or "").strip()
            mobile = (p.get("mobile") or "").strip()

            if not name:
                continue

            to_msisdn = normalize_receiver_id(mobile)
            if not to_msisdn:
                app.logger.warning(
                    "[sales_window_pdf] Skipping %s (%s) — invalid mobile '%s'",
                    name, role, mobile
                )
                continue

            # ---------- 1) build summary PDF for this person ----------
            pdf_path = generate_person_summary_report_pdf(
                name=name,
                role=role,
                from_date=from_str,
                to_date=to_str,
            )
            if not pdf_path:
                # no rows for this person in this window
                continue

            pdf_ok += 1

            app.logger.info(
                "[sales_window_pdf] OK %s=%s, %s..%s, pdf=%s",
                role, name, from_str, to_str, pdf_path
            )

            # ---------- 2) send via BSP 'salesman' template ----------
            job_id = _send_salesman_pdf(
                to_msisdn=to_msisdn,
                name=name,
                role=role,
                from_date=from_str,
                to_date=to_str,
                pdf_path=pdf_path,
            )

            if job_id:
                send_ok += 1
            else:
                send_fail += 1

        return jsonify({
            "ok": True,
            "from": from_str,
            "to": to_str,
            "recipients": len(recipients),
            "pdf_ok": pdf_ok,
            "send_ok": send_ok,
            "send_fail": send_fail,
        }), 200

    except Exception as e:
        app.logger.exception("trigger_sales_reports_window error")
        return jsonify({"ok": False, "error": str(e)}), 500
@app.route('/trigger/daybook', methods=['GET'])
def trigger_daybook():
    queue_daybook_report_automatically(); return jsonify({"status": "manually triggered", "task": "Daybook Report"})
@app.route('/trigger/influencer_form', methods=['GET'])
def trigger_influencer_form():
    process_influencer_form()
    return jsonify({"status": "Triggered", "task": "Influencer / Model form processing"})
@app.route('/trigger/coupon_reminders', methods=['GET'])
def trigger_coupon_reminders():
    try:
        process_coupon_reminders_daily()
        return jsonify({"status": "manually triggered", "task": "coupon_reminders"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
@app.get("/api/bcn_media")
def api_bcn_media():
    """
    /api/bcn_media?barcode=40219
    Returns ALL images/videos for SAME barcode
    """
    try:
        barcode = _clean_barcode(request.args.get("barcode") or "")
        if not barcode:
            return jsonify({"ok": False, "error": "missing_barcode"}), 400

        items = []
        exts_img = {".jpg", ".jpeg", ".png", ".webp"}
        exts_vid = {".mp4", ".3gp", ".mkv", ".mov", ".avi"}

        if not os.path.isdir(BCN_MEDIA_DIR):
            return jsonify({"ok": True, "count": 0, "items": []}), 200

        for fname in sorted(os.listdir(BCN_MEDIA_DIR)):
            if not fname.lower().startswith(barcode.lower()):
                continue

            full_path = os.path.join(BCN_MEDIA_DIR, fname)
            if not os.path.isfile(full_path):
                continue

            ext = os.path.splitext(fname)[1].lower()
            if ext in exts_img:
                ftype = "image"
            elif ext in exts_vid:
                ftype = "video"
            else:
                continue

            thumb_path = os.path.join(BCN_THUMBS_DIR, fname)

            items.append({
                "name": fname,
                "type": ftype,
                "url": _file_url(fname),                       # ✅ FIXED
                "thumb_url": _thumb_url(fname) if os.path.exists(thumb_path) else ""
            })

        return jsonify({
            "ok": True,
            "count": len(items),
            "items": items
        }), 200

    except Exception as e:
        app.logger.exception("api_bcn_media error: %s", e)
        return jsonify({"ok": False, "error": "server_error"}), 500

# ============================
# BCN UPLOAD (FINAL)
# ============================

from concurrent.futures import ThreadPoolExecutor

# ✅ background workers for compress/thumb (keep small to avoid CPU overload)
UPLOAD_EXEC = ThreadPoolExecutor(max_workers=4)

@app.post("/api/bcn_upload")
def api_bcn_upload():
    # ✅ Debug helps to catch "missing_file" issues from some devices
    app.logger.info(
        "[bcn_upload_dbg] form_keys=%s files_keys=%s content_length=%s",
        list(request.form.keys()),
        list(request.files.keys()),
        request.content_length
    )

    barcode = _clean_barcode(request.form.get("barcode") or request.args.get("barcode") or "")
    if not barcode:
        return jsonify({"ok": False, "error": "missing_barcode"}), 400

    if "file" not in request.files:
        return jsonify({
            "ok": False,
            "error": "missing_file",
            "got_files": list(request.files.keys())
        }), 400

    f = request.files["file"]
    if not f or not f.filename:
        return jsonify({"ok": False, "error": "empty_filename"}), 400

    original = secure_filename(f.filename)
    ext = os.path.splitext(original)[1].lower()

    if ext not in ALLOWED_EXT:
        return jsonify({"ok": False, "error": "invalid_file_type", "ext": ext}), 400

    # ✅ Detect image/video
    exts_img = {".jpg", ".jpeg", ".png", ".webp"}
    is_img = ext in exts_img

    # ✅ Unique name (NO race condition even with 10 phones)
    unique = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")

    # ✅ If image: always store as JPG (smaller). If video: keep extension.
    save_name = f"{barcode}_{unique}.jpg" if is_img else f"{barcode}_{unique}{ext}"
    save_path = os.path.join(BCN_MEDIA_DIR, save_name)

    # ✅ Save immediately (fast response)
    f.save(save_path)

    # ✅ Background compress + thumb only for images (do NOT block upload)
    if is_img:
        def _bg():
            try:
                _compress_image_inplace(save_path)
                _ensure_image_thumb(save_path, save_name)
            except Exception:
                app.logger.exception("[bcn_upload] bg compress failed for %s", save_name)

        UPLOAD_EXEC.submit(_bg)

    return jsonify({
        "ok": True,
        "barcode": barcode,
        "saved_as": save_name,
        "type": "image" if is_img else "video",
        "url": request.host_url.rstrip("/") + "/bcn_image/" + save_name,
        "thumb_url": (request.host_url.rstrip("/") + "/bcn_thumb/" + save_name) if is_img else ""
    }), 200

@app.get("/bcn_image/<path:fname>")
def serve_bcn_image(fname):
    # prevent path traversal
    fname = os.path.basename(fname)
    full = os.path.join(BCN_MEDIA_DIR, fname)
    if not os.path.isfile(full):
        return abort(404, description="File not found")
    return send_from_directory(BCN_MEDIA_DIR, fname, as_attachment=False)
@app.get("/bcn_thumb/<path:fname>")
def serve_bcn_thumb(fname):
    fname = os.path.basename(fname)
    full = os.path.join(BCN_THUMBS_DIR, fname)
    if not os.path.isfile(full):
        return abort(404, description="Thumb not found")
    return send_from_directory(BCN_THUMBS_DIR, fname, as_attachment=False)
@app.get("/api/sales_people_active")
def api_sales_people_active():
    """
    /api/sales_people_active?from=01-12-2025&to=11-12-2025

    Returns only those salesmen/helpers who have data in that date range (vch_type=9).
    """
    try:
        from_str = (request.args.get("from") or "").strip()
        to_str   = (request.args.get("to") or "").strip()

        if not from_str or not to_str:
            return jsonify({"ok": False, "error": "missing_from_to"}), 400

        # ✅ uses your existing function (same as scheduled reports)
        recipients = get_active_recipients(from_str, to_str, vch_type=9) or []

        # Remove blanks and invalid numbers (optional)
        out = []
        seen = set()
        for r in recipients:
            name = (r.get("name") or "").strip()
            kind = (r.get("kind") or "salesman").strip()
            mobile = (r.get("mobile") or "").strip()

            if not name:
                continue

            # keep unique by (name, kind)
            key = (name.lower(), kind.lower())
            if key in seen:
                continue
            seen.add(key)

            out.append({
                "name": name,
                "kind": kind,     # 'salesman' or 'helper'
                "mobile": mobile  # keep for debug (Android doesn’t need to show)
            })

        return jsonify({"ok": True, "count": len(out), "people": out}), 200

    except Exception as e:
        app.logger.exception("api_sales_people_active error: %s", e)
        return jsonify({"ok": False, "error": "server_error"}), 500
@app.post("/api/send_salesman_report")
def api_send_salesman_report():
    """
    POST JSON:
    {
      "from": "01-12-2025",
      "to":   "11-12-2025",
      "name": "RAVI KUMAR",
      "kind": "salesman"   // optional
    }

    Server will:
      - find receiver mobile from get_active_recipients() (same source as scheduled job)
      - build PDF (daily or summary depending on range)
      - queue BSP template 'salesman'
    """
    try:
        data = request.get_json(silent=True) or {}
        from_str = (data.get("from") or "").strip()
        to_str   = (data.get("to") or "").strip()
        name     = (data.get("name") or "").strip()
        kind_in  = (data.get("kind") or "").strip().lower()  # optional

        if not from_str or not to_str or not name:
            return jsonify({"ok": False, "error": "missing_params"}), 400

        # ✅ Find this person from active recipients list (same truth as scheduler)
        recipients = get_active_recipients(from_str, to_str, vch_type=9) or []

        match = None
        name_l = name.lower().strip()
        for r in recipients:
            rname = (r.get("name") or "").strip()
            if rname.lower() != name_l:
                continue
            if kind_in and (r.get("kind") or "").strip().lower() != kind_in:
                continue
            match = r
            break

        if not match:
            return jsonify({"ok": False, "error": "not_found_in_range"}), 404

        role = (match.get("kind") or "salesman").strip()
        mobile = (match.get("mobile") or "").strip()

        to_msisdn = normalize_receiver_id(mobile)
        if not to_msisdn:
            return jsonify({"ok": False, "error": "invalid_mobile", "detail": mobile}), 400

        # ✅ Choose daily vs summary based on from/to
        if from_str == to_str:
            pdf_path = generate_person_daily_report_pdf(
                name=name,
                role=role,
                report_date=from_str,
            )
        else:
            pdf_path = generate_person_summary_report_pdf(
                name=name,
                role=role,
                from_date=from_str,
                to_date=to_str,
            )

        if not pdf_path or not os.path.exists(pdf_path):
            return jsonify({"ok": False, "error": "pdf_not_generated"}), 500

        job_id = _send_salesman_pdf(
            to_msisdn=to_msisdn,
            name=name,
            role=role,
            from_date=from_str,
            to_date=to_str,
            pdf_path=pdf_path,
        )

        if not job_id:
            return jsonify({"ok": False, "error": "enqueue_failed"}), 500

        return jsonify({
            "ok": True,
            "job_id": job_id,
            "to": to_msisdn,
            "role": role,
            "pdf": os.path.basename(pdf_path)
        }), 200

    except Exception as e:
        app.logger.exception("api_send_salesman_report error: %s", e)
        return jsonify({"ok": False, "error": "server_error", "detail": str(e)}), 500

@app.route('/trigger/salesman', methods=['GET'])
def trigger_salesman():
    """
    SAFETY:
      - This endpoint triggers ALL salesmen.
      - To avoid accidental ALL sends from app, we require ?all=1

    Use:
      /trigger/salesman?all=1   -> will trigger ALL
      otherwise -> returns error with instructions
    """
    try:
        all_flag = (request.args.get("all") or "").strip().lower()
        if all_flag not in ("1", "true", "yes", "on"):
            app.logger.warning("⚠️ BLOCKED: /trigger/salesman called without all=1")
            return jsonify({
                "ok": False,
                "error": "blocked_all_trigger",
                "detail": "This endpoint sends to ALL salesmen. To run ALL, call /trigger/salesman?all=1",
                "use_instead": {
                    "one_person_post": "/api/send_salesman_report (POST JSON)",
                    "one_person_get": "/trigger/salesman_one?from=dd-mm-YYYY&to=dd-mm-YYYY&name=NAME&kind=salesman"
                }
            }), 400

        app.logger.info("✅ HIT: /trigger/salesman?all=1 (ALL SALES PEOPLE)")
        create_salesman_reports()
        return jsonify({"ok": True, "status": "manually triggered", "task": "Salesman Reports (ALL)"}), 200

    except Exception as e:
        app.logger.exception("trigger_salesman error: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500
@app.route("/trigger/salesman_one", methods=["GET"])
def trigger_salesman_one():
    """
    Trigger only ONE salesman/helper via simple GET (easy from Android/WebView).

    Example:
      /trigger/salesman_one?from=26-12-2025&to=26-12-2025&name=RAVI%20KUMAR&kind=salesman
    """
    try:
        from_str = (request.args.get("from") or "").strip()
        to_str   = (request.args.get("to") or "").strip()
        name     = (request.args.get("name") or "").strip()
        kind_in  = (request.args.get("kind") or "").strip().lower()

        if not from_str or not to_str or not name:
            return jsonify({
                "ok": False,
                "error": "missing_params",
                "need": "from,to,name",
                "example": "/trigger/salesman_one?from=26-12-2025&to=26-12-2025&name=RAVI%20KUMAR&kind=salesman"
            }), 400

        app.logger.info("✅ HIT: /trigger/salesman_one name=%s kind=%s from=%s to=%s",
                        name, kind_in, from_str, to_str)

        # ✅ Find this person from active recipients list (same truth as scheduler)
        recipients = get_active_recipients(from_str, to_str, vch_type=9) or []

        match = None
        name_l = name.lower().strip()
        for r in recipients:
            rname = (r.get("name") or "").strip()
            if rname.lower() != name_l:
                continue
            if kind_in and (r.get("kind") or "").strip().lower() != kind_in:
                continue
            match = r
            break

        if not match:
            return jsonify({"ok": False, "error": "not_found_in_range"}), 404

        role = (match.get("kind") or "salesman").strip()
        mobile = (match.get("mobile") or "").strip()

        to_msisdn = normalize_receiver_id(mobile)
        if not to_msisdn:
            return jsonify({"ok": False, "error": "invalid_mobile", "detail": mobile}), 400

        # ✅ Choose daily vs summary based on from/to
        if from_str == to_str:
            pdf_path = generate_person_daily_report_pdf(
                name=name,
                role=role,
                report_date=from_str,
            )
        else:
            pdf_path = generate_person_summary_report_pdf(
                name=name,
                role=role,
                from_date=from_str,
                to_date=to_str,
            )

        if not pdf_path or not os.path.exists(pdf_path):
            return jsonify({"ok": False, "error": "pdf_not_generated"}), 500

        job_id = _send_salesman_pdf(
            to_msisdn=to_msisdn,
            name=name,
            role=role,
            from_date=from_str,
            to_date=to_str,
            pdf_path=pdf_path,
        )

        if not job_id:
            return jsonify({"ok": False, "error": "enqueue_failed"}), 500

        return jsonify({
            "ok": True,
            "job_id": job_id,
            "to": to_msisdn,
            "role": role,
            "pdf": os.path.basename(pdf_path)
        }), 200

    except Exception as e:
        app.logger.exception("trigger_salesman_one error: %s", e)
        return jsonify({"ok": False, "error": "server_error", "detail": str(e)}), 500

@app.route('/trigger/whatsapp', methods=['GET'])
def trigger_whatsapp():
    create_whatsapp_trigger(); return jsonify({"status": "manually triggered", "task": "WhatsApp Report"})

@app.route('/trigger/google_form', methods=['GET'])
def trigger_google_form():
    print("🔁 Trigger received at /trigger/google_form")
    process_google_form(); return jsonify({"status": "Triggered", "message": "Google Form data processed."})

@app.route('/trigger/future_report', methods=['GET'])
def trigger_future_report():
    queue_future_report(); return jsonify({"status": "manually triggered", "task": "Future Report"})

@app.route('/trigger/coupon_expiry', methods=['GET'])
def trigger_coupon_expiry():
    queue_coupon_expiry(); return jsonify({"status": "manually triggered", "task": "Coupon Expiry JSON"})
# ---------------------------
# Gmail -> WhatsApp Group (FINAL)
# ---------------------------
import os
import datetime
from flask import request, jsonify

try:
    from gmail_dump_to_whatsapp import fetch_new_emails
except Exception:
    fetch_new_emails = None

# ✅ Your Group ID (jid)
GMAIL_TO_WA_GROUP_ID = "120363402090989768@g.us"


def _format_email_for_whatsapp(item: dict) -> str:
    ts = int(item.get("ts") or 0)
    dt = datetime.datetime.fromtimestamp(ts).strftime("%d-%m-%Y %I:%M %p")

    lines = [
        "📧 *New Email*",
        f"🕒 {dt}",
        f"👤 *From:* {item.get('from','')}",
        f"🧾 *Subject:* {item.get('subject','')}",
    ]

    snippet = (item.get("snippet") or "").strip()
    if snippet:
        lines.append(f"✂️ *Snippet:* {snippet}")

    body = (item.get("text") or "").strip()
    if body:
        if len(body) > 2500:
            body = body[:2500] + "\n...(trimmed)"
        lines.append("\n📝 *Body:*")
        lines.append(body)

    atts = item.get("attachments") or []
    if atts:
        lines.append(f"\n📎 Attachments: {len(atts)}")
        for p in atts[:10]:
            lines.append(f"- {os.path.basename(p)}")
        if len(atts) > 10:
            lines.append(f"...(+{len(atts)-10} more)")

    return "\n".join(lines)


def wa_send_group_internal(group_id: str, text: str = "", attachments=None):
    """
    ✅ Uses your EXISTING BotMaster fallback sender which supports GROUP IDs (@g.us).
    ✅ Sends text + uploads attachments (local_path).
    """
    attachments = attachments or []

    if not group_id:
        app.logger.warning("[wa/group] missing group_id")
        return

    # Ensure @g.us
    if group_id.isdigit():
        group_id = group_id + "@g.us"
    elif not group_id.endswith("@g.us"):
        app.logger.warning(f"[wa/group] group_id not ending @g.us: {group_id}")

    # Must be configured
    if not _fallback_can_send():
        app.logger.error("[wa/group] fallback not configured (FALLBACK_API_URL / SENDER_ID / AUTH_TOKEN missing)")
        return

    # 1) Send text (via BotMaster)
    if text and text.strip():
        sc, body = send_fallback_botmaster(group_id, text.strip())
        append_fallback_log(group_id, text.strip(), None, sc, body)
        app.logger.info(f"[wa/group] fallback TEXT status={sc}")

    # 2) Send each attachment (via BotMaster file upload)
    for p in attachments:
        p = str(p or "").strip()
        if not p:
            continue
        if not os.path.exists(p):
            app.logger.warning(f"[wa/group] attachment missing: {p}")
            continue

        sc, body = send_fallback_botmaster(group_id, "", local_path=p)  # ✅ uploadFile
        append_fallback_log(group_id, "", None, sc, body)
        app.logger.info(f"[wa/group] fallback FILE status={sc} path={p}")

@app.post("/wa/send_group")
def wa_send_group():
    data = request.get_json(force=True)
    group_id = str(data.get("group_id", "")).strip()
    text = str(data.get("text", "")).strip()
    attachments = data.get("attachments", []) or []

    if not group_id:
        return jsonify({"error": "Missing group_id"}), 400

    wa_send_group_internal(group_id, text, attachments)
    return jsonify({"ok": True, "group_id": group_id, "sent_files": len(attachments)})


def job_gmail_to_whatsapp():
    try:
        if fetch_new_emails is None:
            app.logger.error("[gmail->wa] fetch_new_emails import failed (gmail_dump_to_whatsapp.py not found).")
            return

        items = fetch_new_emails(query_base="", max_results=50)
        if not items:
            app.logger.info("[gmail->wa] no new emails")
            return

        for item in items:
            msg = _format_email_for_whatsapp(item)
            atts = item.get("attachments") or []
            wa_send_group_internal(GMAIL_TO_WA_GROUP_ID, msg, atts)

        app.logger.info(f"[gmail->wa] done | sent={len(items)}")

    except Exception:
        app.logger.exception("[gmail->wa] job failed")


@app.get("/gmail/run_now")
def gmail_run_now():
    job_gmail_to_whatsapp()
    return jsonify({"ok": True, "message": "Gmail job executed manually"})

# -------------------------------------------------------
# Scheduler jobs (existing + balance confirmation daily)
# -------------------------------------------------------
scheduler.add_job(
    send_daybook_pdf_on_whatsapp,
    'cron',
    hour=13,
    minute=10,
    id="daybook_1306",
    replace_existing=True
)

scheduler.add_job(
    send_daybook_pdf_on_whatsapp,
    'cron',
    hour=22,
    minute=55,
    id="daybook_2255",
    replace_existing=True
)

scheduler.add_job(create_salesman_reports, 'cron', hour=22, minute=1)
scheduler.add_job(create_salesman_summary_reports, 'cron', hour=22, minute=2)
scheduler.add_job(check_daybook_queue, 'interval', minutes=1)
scheduler.add_job(queue_upi_other_report, 'cron', hour=22, minute=58)
scheduler.add_job(queue_daybook_report_automatically, 'cron', hour=21, minute=58)
# scheduler.add_job(queue_future_report, 'cron', day_of_week='wed,sat', hour=9, minute=0)
scheduler.add_job(
    process_google_form,
    'cron',
    minute='0',          # runs at 11:00, 12:00, 13:00, ... 22:00
    hour='8-21'         # allowed hours
)
# Every 60 minutes between 11:00 and 22:00 (example – adjust as you like)
scheduler.add_job(
    process_influencer_form,
    'cron',
    minute='15',         # eg. at xx:15
    hour='11-22'         # 11:15, 12:15, ... 22:15
)

scheduler.add_job(lambda: subprocess.call(["python", "sync_google_contacts.py"], cwd=r"C:\BusyWin\AI BOT\BUSY_RECEIPT_BOT_STRUCTURE"), 'interval', minutes=240)
# scheduler.add_job(queue_coupon_expiry, 'cron', day_of_week='wed,sat', hour=9, minute=0)
scheduler.add_job(queue_balance_confirmation_reports, 'cron', hour=22, minute=0)
scheduler.add_job(_auto_fallback_timeout_checker, 'interval', minutes=1)
scheduler.add_job(lambda: rebuild_salesman_file(window_days=45, vch_type=9), 'cron', hour=2, minute=0)
scheduler.add_job(process_coupon_reminders_daily, 'cron', hour=9, minute=30)
scheduler.add_job(lambda: send_points_expiry_reminders(10), 'cron', hour=9, minute=55)
scheduler.add_job(lambda: send_points_expiry_reminders(5),  'cron', hour=10, minute=10)
scheduler.add_job(queue_collection_summary_sql, 'cron', hour='13-23', minute=0)
scheduler.add_job(
    send_daybook_pdf_on_whatsapp,
    "cron",
    hour=20,
    minute=1,
    id="daybook_daily_2001",
    replace_existing=True
)
scheduler.add_job(
    send_daybook_pdf_on_whatsapp,
    "cron",
    hour=22,
    minute=55,
    id="daybook_daily_2255",
    replace_existing=True
)
# -------------------------------------------------------
# Main
# -------------------------------------------------------
if __name__ == '__main__':
    scheduler.start()
    print("✅ Scheduler started.")
    print("✅ Upload folder:", UPLOAD_FOLDER)
    print("✅ Queue folder:", QUEUE_DIR)
    app.logger.info("Starting http://0.0.0.0:5000 | DRY_RUN=%s | workers=%s | retries=%s | PDF_DIR=%s",
                    DRY_RUN, NUM_WORKERS, RETRY_MAX, PDF_DIR)
    app.run(host='0.0.0.0', port=5000, debug=False)
