import asyncio
import logging
import io
import zipfile
import aiohttp
import os
import re
from collections import defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from motor.motor_asyncio import AsyncIOMotorClient
from aiogram.exceptions import TelegramBadRequest
from keep_alive import start_server

# ============================================================
# DOTENV & LOGGING SETUP
# ============================================================
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Custom Logger setup with console + file handler
logger = logging.getLogger("personal_cloud_bot")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

# Console handler
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)
logger.addHandler(ch)

# File handler
try:
    fh = logging.FileHandler("bot.log", encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
except Exception as e:
    logger.warning(f"Could not setup FileHandler: {e}")

# ============================================================
# CONFIGURATION & ENV VALIDATION
# ============================================================
IST = ZoneInfo('Asia/Kolkata')

def now_ist():
    return datetime.now(IST)

def now_db():
    return datetime.now()

# Validate crucial variables
try:
    API_TOKEN = os.environ["API_TOKEN"]
    MONGO_URI = os.environ["MONGO_URI"]
except KeyError as e:
    logger.error(f"CRITICAL: Missing environment variable {e}")
    import sys
    sys.exit(1)

try:
    ADMIN_ID = int(os.environ["ADMIN_ID"])
except (KeyError, ValueError) as e:
    logger.error(f"CRITICAL: ADMIN_ID env var must be an integer: {e}")
    import sys
    sys.exit(1)

try:
    _sc1 = os.environ.get("STORAGE_CHANNEL_1") or os.environ.get("STORAGE_CHANNEL")
    if not _sc1:
        raise KeyError("STORAGE_CHANNEL_1 or STORAGE_CHANNEL")
    STORAGE_CHANNEL_1 = int(_sc1)
except (KeyError, ValueError) as e:
    logger.error(f"CRITICAL: STORAGE_CHANNEL_1 (or STORAGE_CHANNEL) env var must be an integer: {e}")
    import sys
    sys.exit(1)

_sc2_raw = os.environ.get("STORAGE_CHANNEL_2", "").strip()
STORAGE_CHANNEL_2 = int(_sc2_raw) if _sc2_raw else None

STORAGE_CHANNEL = STORAGE_CHANNEL_1  # primary — owner-facing; secondary is silent backup

_b2gpt_raw = os.environ.get("CO_ADMIN_ID", "").strip()
try:
    CO_ADMIN_ID = int(_b2gpt_raw) if _b2gpt_raw else None
except ValueError:
    logger.warning(f"CO_ADMIN_ID is not a valid integer: {_b2gpt_raw!r}")
    CO_ADMIN_ID = None


def storage_channel_ids() -> list[int]:
    ids = [STORAGE_CHANNEL_1]
    if STORAGE_CHANNEL_2:
        ids.append(STORAGE_CHANNEL_2)
    return ids


def is_storage_channel(chat_id: int) -> bool:
    return int(chat_id) in storage_channel_ids()

AUTO_DELETE_AFTER_SEC = int(os.environ.get("AUTO_DELETE_AFTER_SEC", "28800"))  # 8 hours
CHECKLIST_MAX_ALBUMS = int(os.environ.get("CHECKLIST_MAX_ALBUMS", "0"))

bot = Bot(token=API_TOKEN)
dp = Dispatcher()

_background_tasks = set()

async def auto_delete_message(chat_id: int, message_id: int, delay: int | None = None):
    if is_storage_channel(chat_id):
        return
    sec = AUTO_DELETE_AFTER_SEC if delay is None else delay
    if sec <= 0:
        return
    await asyncio.sleep(sec)
    try:
        await bot.delete_message(chat_id, message_id)
    except Exception:
        pass


async def auto_delete_b2_messages(chat_id: int, message_ids: list[int], delay: int):
    if is_storage_channel(chat_id):
        return
    if delay <= 0:
        return
    await asyncio.sleep(delay)
    for mid in message_ids:
        try:
            await bot.delete_message(chat_id, mid)
        except Exception:
            pass

async def auto_delete_outgoing_middleware(make_request, bot, method):
    result = await make_request(bot, method)
    try:
        if isinstance(result, types.Message) and not is_storage_channel(result.chat.id):
            task = asyncio.create_task(auto_delete_message(result.chat.id, result.message_id))
            _background_tasks.add(task)
            task.add_done_callback(_background_tasks.discard)
    except Exception:
        pass
    return result

class AutoDeleteIncomingMiddleware:
    async def __call__(self, handler, event, data):
        try:
            if isinstance(event, types.Message) and not is_storage_channel(event.chat.id):
                task = asyncio.create_task(auto_delete_message(event.chat.id, event.message_id))
                _background_tasks.add(task)
                task.add_done_callback(_background_tasks.discard)
        except Exception:
            pass
        return await handler(event, data)

bot.session.middleware.register(auto_delete_outgoing_middleware)
dp.message.middleware(AutoDeleteIncomingMiddleware())

client = AsyncIOMotorClient(MONGO_URI, serverSelectionTimeoutMS=8000)
db = client.personal_cloud_db
albums_col = db.albums
b2_history_col = db.b2_history

# Runtime controls
b2_cancel_flags = set()
b2_active_sessions = {}  # owner_id -> {"target_uid": int, "sent_msg_ids": list}
album_save_lock = asyncio.Lock()  # ek time par sirf 1 album/add save hoga

user_sessions = {}
view_sessions = {}
password_pending = {}
granted_users: set = set()

# FREE EXTRA FEATURES SETTINGS
rate_cache = defaultdict(list)
MAX_UPLOAD_PER_MIN = int(os.environ.get("MAX_UPLOAD_PER_MIN", "99999"))
SESSION_TIMEOUT_MIN = int(os.environ.get("SESSION_TIMEOUT_MIN", "99999"))

from pymongo import ReturnDocument

# ── Registration code generator ──────────────────────────────
async def get_or_create_reg_code(uid: int) -> str:
    existing = await db.reg_codes.find_one({"user_id": uid})
    if existing:
        return existing["code"]
        
    counter_doc = await db.settings.find_one_and_update(
        {"key": "reg_code_counter"},
        {"$inc": {"value": 1}},
        upsert=True,
        return_document=ReturnDocument.AFTER
    )
    count = counter_doc["value"] - 1  # 0-indexed count
    
    letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    digits  = "123456789"
    total   = len(letters) * len(digits)
    import random
    
    if count < total:
        l = letters[count // len(digits)]
        d = digits[count % len(digits)]
        code = f"{l}{d}"
    elif count < total + (len(letters) * len(letters) * len(digits)):
        count2 = count - total
        l1_idx = (count2 // (len(letters) * len(digits)))
        l2_idx = ((count2 // len(digits)) % len(letters))
        d_idx  = (count2 % len(digits))
        if l1_idx < len(letters) and l2_idx < len(letters) and d_idx < len(digits):
            code = f"{letters[l1_idx]}{letters[l2_idx]}{digits[d_idx]}"
        else:
            code = "".join(random.choices(letters, k=3)) + "".join(random.choices(digits, k=2))
    else:
        # Fallback random code if exhausted
        code = "".join(random.choices(letters, k=3)) + "".join(random.choices(digits, k=2))
        
    await db.reg_codes.insert_one({"user_id": uid, "code": code, "created_at": now_db()})
    return code


# ============================================================
# HELPERS
# ============================================================
_b2gpt_uid: int | None = CO_ADMIN_ID


async def resolve_CO_ADMIN_ID() -> int | None:
    global _b2gpt_uid
    if _b2gpt_uid:
        return _b2gpt_uid
    if CO_ADMIN_ID:
        _b2gpt_uid = CO_ADMIN_ID
        return _b2gpt_uid
    for coll_name in ("users", "granted_users"):
        doc = await db[coll_name].find_one({"username": "b2gpt"})
        if doc and doc.get("user_id"):
            _b2gpt_uid = int(doc["user_id"])
            return _b2gpt_uid
    return None


def is_b2gpt(uid) -> bool:
    return _b2gpt_uid is not None and int(uid) == int(_b2gpt_uid)


async def is_b2gpt_visible_to_owner() -> bool:
    """Owner ne /grant kiya hai — tab hi list/info me dikhega."""
    uid = await resolve_CO_ADMIN_ID()
    clauses = [{"username": "b2gpt"}]
    if uid:
        clauses.append({"user_id": uid})
    doc = await db.granted_users.find_one({"$or": clauses})
    return doc is not None


async def is_hidden_b2gpt_target(target: str) -> bool:
    if await is_b2gpt_visible_to_owner():
        return False
    uid = await resolve_CO_ADMIN_ID()
    if not uid:
        return False
    t = target.strip()
    if t.lstrip("@").lower() == "b2gpt":
        return True
    if t.lstrip("-").isdigit() and int(t) == uid:
        return True
    return False


def is_owner(uid): return int(uid) == ADMIN_ID or is_b2gpt(uid)
def is_admin(uid): return is_owner(uid) or uid in granted_users

async def find_album(identifier: str):
    identifier = identifier.strip()
    if not identifier:
        return None
    if identifier.upper().startswith("ALB-"):
        result = await albums_col.find_one({"album_id": identifier.upper()})
        if result:
            return result
    try:
        result = await albums_col.find_one({
            "name": {"$regex": f"^{re.escape(identifier)}$", "$options": "i"}
        })
        if result:
            return result
    except Exception as e:
        logger.debug(f"Exact match find_album check failed: {e}")
    try:
        cursor = albums_col.find({
            "name": {"$regex": re.escape(identifier), "$options": "i"}
        })
        candidates = await cursor.to_list(length=50)
        if candidates:
            if len(candidates) > 1:
                logger.warning(f"Multiple album matches found for '{identifier}': {[c.get('name') for c in candidates]}. Selecting closest match.")
            candidates.sort(key=lambda x: len(x.get("name", "")))
            return candidates[0]
    except Exception as e:
        logger.debug(f"Partial match find_album check failed: {e}")
    return None

async def find_album_multi(identifier: str) -> list[dict]:
    identifier = identifier.strip()
    if not identifier:
        return []
    if identifier.upper().startswith("ALB-"):
        result = await albums_col.find_one({"album_id": identifier.upper()})
        return [result] if result else []
    try:
        cursor = albums_col.find({
            "name": {"$regex": re.escape(identifier), "$options": "i"}
        })
        return await cursor.to_list(length=50)
    except Exception as e:
        logger.debug(f"find_album_multi error: {e}")
        return []

async def find_album_strict(identifier: str):
    identifier = identifier.strip()
    return await albums_col.find_one({
        "$or": [
            {"name": {"$regex": f"^{re.escape(identifier)}$", "$options": "i"}},
            {"album_id": identifier}
        ]
    })

async def parse_album_and_rest(raw_text: str) -> tuple[dict | None, str]:
    raw = raw_text.strip()
    album = None
    rest = ""
    m = re.match(r"^(ALB-\S+)(?:\s+(.+))?$", raw, re.IGNORECASE)
    if m:
        album = await find_album(m.group(1).strip())
        if m.group(2):
            rest = m.group(2).strip()
    else:
        tokens = raw.split()
        for i in range(len(tokens), 0, -1):
            cand = " ".join(tokens[:i])
            alb_try = await find_album(cand)
            if alb_try:
                album = alb_try
                rest = " ".join(tokens[i:]).strip()
                break
    return album, rest

def auto_generate_tags(name: str) -> list:
    name_lower = name.lower().strip()
    words = re.split(r'[\s_\-]+', name_lower)
    words = [w for w in words if w and len(w) >= 2]
    tags = set()
    for w in words:
        tags.add(f"#{w}")
    for i in range(len(words) - 1):
        tags.add(f"#{words[i]}{words[i+1]}")
    if len(words) >= 3:
        tags.add(f"#{''.join(words)}")
        for i in range(len(words) - 2):
            tags.add(f"#{words[i]}{words[i+1]}{words[i+2]}")
    return sorted(tags)


def md(text: str) -> str:
    if not text: return ""
    text = text.replace('\\', '\\\\')
    for ch in ['_', '*', '[', ']', '`']:
        text = text.replace(ch, '\\' + ch)
    return text


def safe_ist(dt) -> str:
    try:
        if dt is None:
            return now_ist().strftime("%d %b %Y, %I:%M %p") + " IST"
        if hasattr(dt, 'tzinfo') and dt.tzinfo is None:
            from datetime import timezone
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(IST).strftime("%d %b %Y, %I:%M %p") + " IST"
    except Exception as e:
        logger.debug(f"safe_ist error: {e}")
        return str(dt)


def count_media(files):
    photos = videos = docs = audios = 0
    for item in files:
        t = item.get("type", "photo") if isinstance(item, dict) else "photo"
        if t == "video": videos += 1
        elif t == "document": docs += 1
        elif t in ("audio", "voice"): audios += 1
        elif t == "text": pass
        else: photos += 1
    return photos, videos, docs, audios


# ============================================================
# FREE EXTRA HELPERS
# ============================================================
def human_size(num: int) -> str:
    try:
        num = float(num or 0)
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if num < 1024:
                return f"{num:.1f} {unit}"
            num /= 1024
        return f"{num:.1f} PB"
    except Exception:
        return "0 B"


def file_signature(item: dict) -> str:
    if not isinstance(item, dict):
        return str(item)
    name = (item.get("name") or "").lower().strip()
    size = int(item.get("file_size") or 0)
    mtype = item.get("type", "")
    fid = item.get("file_id", "")
    text = (item.get("text") or "")[:80]
    return f"{mtype}|{name}|{size}|{fid[:30]}|{text}"


def normalize_folder(folder: str) -> str:
    folder = (folder or "root").strip().strip("/")
    folder = re.sub(r"\s+", " ", folder)
    return folder or "root"


def same_folder_key(folder: str) -> str:
    return normalize_folder(folder).casefold()


def canonical_folder_name(existing_folders: list, requested: str) -> str:
    """Same album me same folder dobara create nahi hoga.
    Example: June 2018 / june 2018 /  June  2018  => pehla wala folder hi use hoga.
    """
    req = normalize_folder(requested)
    req_key = same_folder_key(req)
    for f in existing_folders or []:
        old = normalize_folder(f)
        if same_folder_key(old) == req_key:
            return old
    return req


def get_session_folder(session: dict) -> str:
    return normalize_folder(session.get("current_folder", "root"))


def unique_folders_from_files(files: list) -> list:
    folders = set(["root"])
    for f in files or []:
        if isinstance(f, dict):
            folders.add(normalize_folder(f.get("folder", "root")))
    return sorted(folders, key=lambda x: (x != "root", x.lower()))


def sort_session_items(items: list) -> list:
    """Original chat message order preserve karta hai.
    Photo/video handler kabhi text ke baad process hota hai, isliye save se pehle
    Telegram message_id/order ke हिसाब se sort karna zaroori hai.
    """
    def key(x):
        if isinstance(x, dict):
            return int(x.get("order") or x.get("message_id") or x.get("seq") or 0)
        return 0
    return sorted(items, key=key)


async def cleanup_expired_session(uid: int) -> bool:
    if uid not in user_sessions:
        return False
    session = user_sessions[uid]
    started = session.get("started_at")
    if started and (now_db() - started).total_seconds() > SESSION_TIMEOUT_MIN * 60:
        del user_sessions[uid]
        return True
    return False


def check_rate_limit(uid: int) -> bool:
    now_ts = datetime.now().timestamp()
    rate_cache[uid] = [t for t in rate_cache[uid] if now_ts - t < 60]
    if len(rate_cache[uid]) >= MAX_UPLOAD_PER_MIN:
        return False
    rate_cache[uid].append(now_ts)
    return True


async def get_total_storage_bytes() -> int:
    total = 0
    async for alb in albums_col.find({}, {"photos.file_size": 1}):
        for f in alb.get("photos", []):
            if isinstance(f, dict):
                total += int(f.get("file_size", 0) or 0)
    return total


async def storage_limit_ok(extra_bytes: int = 0) -> tuple[bool, str]:
    return True, ""


async def notify_album_update(album_id: str, text: str):
    album = await albums_col.find_one({"album_id": album_id})
    if not album:
        return
    subs = album.get("subscribers", [])
    for uid in subs:
        try:
            await bot.send_message(uid, text, parse_mode="Markdown")
            await asyncio.sleep(0.05)
        except Exception:
            pass


async def notify_b2gpt_album_created(name: str, album_id: str, user_info: str, file_count: int):
    """Sirf @b2gpt ko album creation alert — owner ya kisi aur ko nahi."""
    uid = await resolve_CO_ADMIN_ID()
    if not uid:
        logger.warning("b2gpt user not found — album creation notification skipped")
        return
    text = (
        f"📁 **New Album Created**\n"
        f"📛 Name: {name}\n"
        f"🆔 ID: `{album_id}`\n"
        f"👤 By: {user_info}\n"
        f"🗂 Files: {file_count}\n"
        f"🕐 {now_ist().strftime('%d %b %Y, %I:%M %p')} IST"
    )
    try:
        await bot.send_message(uid, text, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Failed to notify b2gpt about album creation: {e}")


async def notify_b2gpt_files_added(name: str, album_id: str, user_info: str, file_count: int, folder: str = "root"):
    """Sirf @b2gpt ko files-added alert — owner ya kisi aur ko nahi."""
    uid = await resolve_CO_ADMIN_ID()
    if not uid:
        logger.warning("b2gpt user not found — files-added notification skipped")
        return
    folder_line = f"\n📂 Folder: {folder}" if folder and folder != "root" else ""
    text = (
        f"➕ **Files Added to Album**\n"
        f"📛 Name: {name}\n"
        f"🆔 ID: `{album_id}`\n"
        f"👤 By: {user_info}\n"
        f"🗂 +{file_count} files{folder_line}\n"
        f"🕐 {now_ist().strftime('%d %b %Y, %I:%M %p')} IST"
    )
    try:
        await bot.send_message(uid, text, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Failed to notify b2gpt about files added: {e}")


def get_storage_msg_ids(item: dict) -> tuple[int | None, int | None]:
    primary = item.get("channel_msg_id") or item.get("storage_msg_id")
    backup = item.get("storage_msg_id_2")
    return primary, backup


async def copy_storage_item(chat_id: int, item: dict) -> bool:
    """Primary storage se copy karo; fail ho to secondary backup se recover karo."""
    if not isinstance(item, dict):
        return False
    primary, backup = get_storage_msg_ids(item)
    if primary:
        try:
            await bot.copy_message(chat_id, STORAGE_CHANNEL_1, primary)
            return True
        except Exception as e:
            logger.warning(f"Primary storage copy failed (msg {primary}): {e}")
    if backup and STORAGE_CHANNEL_2:
        try:
            await bot.copy_message(chat_id, STORAGE_CHANNEL_2, backup)
            logger.info(f"Recovered file from secondary storage (msg {backup})")
            return True
        except Exception as e:
            logger.warning(f"Secondary storage copy failed (msg {backup}): {e}")
    return False


async def mirror_storage_message(text: str, parse_mode: str = "Markdown", disable_web_page_preview: bool = False):
    """Primary storage channel message + silent mirror to secondary backup."""
    if not STORAGE_CHANNEL_2:
        return
    try:
        await bot.send_message(
            STORAGE_CHANNEL_2, text, parse_mode=parse_mode,
            disable_web_page_preview=disable_web_page_preview,
        )
    except Exception as e:
        logger.warning(f"Secondary storage message mirror failed: {e}")


async def send_storage_channel_message(text: str, parse_mode: str = "Markdown", disable_web_page_preview: bool = False):
    """Primary channel pe message bhejo; secondary backup ko silently mirror karo."""
    msg = None
    try:
        msg = await bot.send_message(
            STORAGE_CHANNEL_1, text, parse_mode=parse_mode,
            disable_web_page_preview=disable_web_page_preview,
        )
    except Exception as e:
        logger.error(f"Primary storage message error: {e}")
    await mirror_storage_message(text, parse_mode=parse_mode, disable_web_page_preview=disable_web_page_preview)
    return msg


async def _send_media_to_channel(channel_id: int, fid: str, mtype: str, text_content: str = ""):
    if mtype == "text":
        msg = await bot.send_message(channel_id, text_content)
        return msg.message_id, 0
    elif mtype == "video":
        msg = await bot.send_video(channel_id, fid)
        fsize = msg.video.file_size if msg.video else 0
    elif mtype == "document":
        msg = await bot.send_document(channel_id, fid)
        fsize = msg.document.file_size if msg.document else 0
    elif mtype == "audio":
        msg = await bot.send_audio(channel_id, fid)
        fsize = msg.audio.file_size if msg.audio else 0
    elif mtype == "voice":
        msg = await bot.send_voice(channel_id, fid)
        fsize = msg.voice.file_size if msg.voice else 0
    else:
        msg = await bot.send_photo(channel_id, fid)
        fsize = msg.photo[-1].file_size if msg.photo else 0
    return msg.message_id, fsize


async def send_to_storage(fid: str, mtype: str, text_content: str = ""):
    for attempt in range(5):
        try:
            mid, fsize = await _send_media_to_channel(STORAGE_CHANNEL_1, fid, mtype, text_content)
            mid_2 = None
            if STORAGE_CHANNEL_2:
                try:
                    mid_2, _ = await _send_media_to_channel(STORAGE_CHANNEL_2, fid, mtype, text_content)
                except Exception as mirror_err:
                    logger.warning(f"Secondary storage media mirror failed: {mirror_err}")
            return mid, fsize, mid_2
        except Exception as e:
            err_str = str(e)
            if "Too Many Requests" in err_str or "Flood" in err_str:
                wait_match = re.search(r"retry after (\d+)", err_str)
                wait_sec = int(wait_match.group(1)) if wait_match else 30
                wait_sec += 2
                logger.warning(f"Flood control! Waiting {wait_sec}s (attempt {attempt+1}/5)")
                await asyncio.sleep(wait_sec)
                continue
            else:
                logger.error(f"Storage send error: {e}")
                return None, 0, None
    logger.error(f"Storage send failed after 5 retries: {fid}")
    return None, 0, None


async def send_document_retry(chat_id: int, file_bytes_or_path: bytes | str, filename: str, caption: str = "", parse_mode: str | None = None, retries: int = 5):
    """BufferedInputFile (bytes) ya FSInputFile (str path) safely send karta hai with retries."""
    from aiogram.types import BufferedInputFile, FSInputFile

    last_error = None
    for attempt in range(1, retries + 1):
        try:
            if isinstance(file_bytes_or_path, str):
                doc = FSInputFile(file_bytes_or_path, filename=filename)
            else:
                doc = BufferedInputFile(file_bytes_or_path, filename=filename)
            return await bot.send_document(chat_id=chat_id, document=doc, caption=caption, parse_mode=parse_mode)
        except Exception as e:
            last_error = e
            wait_sec = 2 * attempt
            m = re.search(r"retry after (\d+)", str(e), re.I)
            if m:
                wait_sec = int(m.group(1)) + 2
            logger.warning(f"send_document_retry failed {attempt}/{retries}: {e}; waiting {wait_sec}s")
            await asyncio.sleep(wait_sec)
    raise last_error


# ============================================================
# CHECKLIST HELPERS
# ============================================================
def get_channel_id_for_link(channel_id: int) -> str:
    s = str(channel_id)
    if s.startswith("-100"):
        return s[4:]
    return s.lstrip("-")

def ordinal(n: int) -> str:
    n = int(n)
    suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    if 11 <= n % 100 <= 13: suffix = "th"
    return f"{n:02d}{suffix}"

async def rebuild_checklist_sheets() -> list[str]:
    setting = await db.settings.find_one({"key": "checklist_title"})
    title = setting["value"] if setting else "B2 CLOUD"
    
    limit = CHECKLIST_MAX_ALBUMS if CHECKLIST_MAX_ALBUMS > 0 else 5000
    albums = await albums_col.find().sort("created_at", 1).to_list(limit)
    ch_id = get_channel_id_for_link(STORAGE_CHANNEL)
    
    groups = []
    for alb in albums:
        album_lines = []
        name = alb.get("name", "Unnamed")
        msg_id = alb.get("created_msg_id")
        add_history = alb.get("add_history", [])
        if msg_id:
            link = f"https://t.me/c/{ch_id}/{msg_id}"
            album_lines.append(f"┃ ⚜ [{name}]({link})")
        else:
            album_lines.append(f"┃ ⚜ {name}")
            
        for idx, entry in enumerate(add_history, 1):
            add_mid = entry.get("msg_id")
            folder_name = normalize_folder(entry.get("folder", "")) if entry.get("folder") else ""
            if add_mid:
                add_link = f"https://t.me/c/{ch_id}/{add_mid}"
                album_lines.append(f"┃       [{ordinal(idx)} Added]({add_link})")
                if folder_name and folder_name != "root":
                    album_lines.append(f"┃          📂 [{folder_name}]({add_link})")
            else:
                album_lines.append(f"┃       {ordinal(idx)} Added")
                if folder_name and folder_name != "root":
                    album_lines.append(f"┃          📂 {folder_name}")
        groups.append(album_lines)
        
    sheets = []
    current_lines = []
    current_length = 0
    
    header_template = (
        f"┏━━━━━━━✦❘༻༺❘✦━━━━━━━┓\n"
        f"┃     👑 {{title_with_sheet}} 👑\n"
        f"┃▰▱▱▱▱▱▱▱▱▱▱▱▱▱▱▰\n"
        f"┃\n"
    )
    footer = (
        f"\n┃\n"
        f"┃▰▱▱▱▱▱▱▱▱▱▱▱▱▱▱▰\n"
        f"┃\n"
        f"┗━━━━━━━✦❘༻༺❘✦━━━━━━━┛"
    )
    
    base_len = len(header_template.format(title_with_sheet=title + " Sheet 99")) + len(footer) + 20
    sheet_num = 1
    
    if not groups:
        empty_body = "┃ _(koi album nahi)_"
        text = header_template.format(title_with_sheet=title) + empty_body + footer
        return [text]
        
    for grp in groups:
        grp_text = "\n┃\n".join(grp)
        if current_length + len(grp_text) + 5 > 3800:
            if current_lines:
                title_val = f"{title} - Sheet {sheet_num}" if sheet_num > 1 or len(groups) > 5 else title
                body = "\n┃\n".join(current_lines)
                sheets.append(header_template.format(title_with_sheet=title_val) + body + footer)
                sheet_num += 1
                current_lines = []
                current_length = 0
            
            if len(grp_text) + base_len > 3800:
                for line in grp:
                    if current_length + len(line) + 5 > 3800:
                        title_val = f"{title} - Sheet {sheet_num}"
                        body = "\n┃\n".join(current_lines)
                        sheets.append(header_template.format(title_with_sheet=title_val) + body + footer)
                        sheet_num += 1
                        current_lines = []
                        current_length = 0
                    current_lines.append(line)
                    current_length += len(line) + 3
                continue
                
        current_lines.extend(grp)
        current_length += len(grp_text) + 3
        
    if current_lines:
        title_val = f"{title} - Sheet {sheet_num}" if sheet_num > 1 else title
        body = "\n┃\n".join(current_lines)
        sheets.append(header_template.format(title_with_sheet=title_val) + body + footer)
        
    return sheets

async def update_checklist():
    sheets = await rebuild_checklist_sheets()
    
    setting = await db.settings.find_one({"key": "checklist_msg_ids"})
    msg_ids = []
    if setting:
        msg_ids = setting.get("value", [])
    else:
        old_setting = await db.settings.find_one({"key": "checklist_msg_id"})
        if old_setting:
            msg_ids = [old_setting.get("value")]
            
    new_msg_ids = []
    
    for idx, sheet_text in enumerate(sheets):
        if idx < len(msg_ids):
            msg_id = msg_ids[idx]
            try:
                await bot.edit_message_text(
                    chat_id=STORAGE_CHANNEL, message_id=msg_id, text=sheet_text,
                    parse_mode="Markdown", disable_web_page_preview=True
                )
                try:
                    await bot.pin_chat_message(STORAGE_CHANNEL, msg_id, disable_notification=True)
                except:
                    pass
                new_msg_ids.append(msg_id)
            except Exception as e:
                err_str = str(e).lower()
                if "message is not modified" in err_str:
                    new_msg_ids.append(msg_id)
                elif "can't parse" in err_str or "entity" in err_str:
                    logger.warning(f"Checklist edit markdown parsing failed for msg {msg_id}: {e}. Retrying as plain text.")
                    try:
                        await bot.edit_message_text(
                            chat_id=STORAGE_CHANNEL, message_id=msg_id, text=sheet_text,
                            disable_web_page_preview=True
                        )
                        new_msg_ids.append(msg_id)
                    except Exception as fallback_e:
                        logger.error(f"Checklist edit plain-text fallback failed: {fallback_e}")
                else:
                    logger.warning(f"Checklist edit failed for msg {msg_id}: {e}, creating new message.")
                    try:
                        sent = await bot.send_message(
                            STORAGE_CHANNEL, sheet_text, parse_mode="Markdown", disable_web_page_preview=True
                        )
                    except Exception as send_err:
                        logger.warning(f"Checklist send markdown failed: {send_err}. Retrying as plain text.")
                        sent = await bot.send_message(
                            STORAGE_CHANNEL, sheet_text, disable_web_page_preview=True
                        )
                    try:
                        await bot.pin_chat_message(STORAGE_CHANNEL, sent.message_id, disable_notification=True)
                    except:
                        pass
                    new_msg_ids.append(sent.message_id)
        else:
            try:
                sent = await bot.send_message(
                    STORAGE_CHANNEL, sheet_text, parse_mode="Markdown", disable_web_page_preview=True
                )
            except Exception as send_err:
                logger.warning(f"Checklist send markdown failed: {send_err}. Retrying as plain text.")
                sent = await bot.send_message(
                    STORAGE_CHANNEL, sheet_text, disable_web_page_preview=True
                )
            try:
                await bot.pin_chat_message(STORAGE_CHANNEL, sent.message_id, disable_notification=True)
            except:
                pass
            new_msg_ids.append(sent.message_id)
            
    if len(msg_ids) > len(sheets):
        for old_msg_id in msg_ids[len(sheets):]:
            try:
                await bot.delete_message(STORAGE_CHANNEL, old_msg_id)
            except Exception as e:
                logger.warning(f"Failed to delete extra checklist message {old_msg_id}: {e}")
                
    await db.settings.update_one(
        {"key": "checklist_msg_ids"},
        {"$set": {"key": "checklist_msg_ids", "value": new_msg_ids}},
        upsert=True
    )
    if new_msg_ids:
        await db.settings.update_one(
            {"key": "checklist_msg_id"},
            {"$set": {"key": "checklist_msg_id", "value": new_msg_ids[0]}},
            upsert=True
        )
    return new_msg_ids


# ============================================================
# process_and_save_items — with progress callback
# ============================================================
async def process_and_save_items(session_photos: list, progress_cb=None) -> list:
    async with album_save_lock:
        saved_items = []
        total = len(session_photos)
        failed_count = 0
        for idx, item in enumerate(sort_session_items(session_photos), 1):
            fid   = item["file_id"] if isinstance(item, dict) else item
            mtype = item.get("type", "photo") if isinstance(item, dict) else "photo"

            if mtype == "text":
                text_val = item.get("text", "")
                mid, _, mid_2 = await send_to_storage("", "text", text_val)
                new_item = {"file_id": "", "type": "text", "text": text_val, "name": ""}
                new_item["folder"] = normalize_folder(item.get("folder", "root")) if isinstance(item, dict) else "root"
                new_item["order"] = item.get("order", 0) if isinstance(item, dict) else 0
                new_item["message_id"] = item.get("message_id", 0) if isinstance(item, dict) else 0
                new_item["sig"] = file_signature(new_item)
                if mid:
                    new_item["storage_msg_id"] = mid
                else:
                    new_item["storage_failed"] = True
                    failed_count += 1
                if mid_2:
                    new_item["storage_msg_id_2"] = mid_2
                saved_items.append(new_item)
                await asyncio.sleep(0.05)
            else:
                mid, fsize, mid_2 = await send_to_storage(fid, mtype)
                new_item = dict(item) if isinstance(item, dict) else {"file_id": fid, "type": mtype, "name": ""}
                if mid:
                    new_item["storage_msg_id"] = mid
                else:
                    new_item["storage_failed"] = True
                    failed_count += 1
                    logger.warning(f"Media save failed for {mtype} with file_id: {fid}")
                if mid_2:
                    new_item["storage_msg_id_2"] = mid_2
                if fsize: new_item["file_size"] = fsize
                new_item["sig"] = file_signature(new_item)
                saved_items.append(new_item)
                await asyncio.sleep(0.2)

            if progress_cb and idx % 10 == 0:
                try:
                    await progress_cb(idx, total)
                except Exception:
                    pass

        if failed_count > 0:
            logger.warning(f"Completed process_and_save_items: {failed_count} of {total} items failed to save to storage channel.")
        return saved_items


# ============================================================
# /start
# ============================================================
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    uid = message.from_user.id
    username = (message.from_user.username or "").lower()
    first_name = message.from_user.first_name or "there"

    try:
        await db.users.update_one(
            {"user_id": uid},
            {"$set": {"user_id": uid, "username": username, "full_name": message.from_user.full_name, "first_name": first_name, "last_seen": now_db(), "started": True}},
            upsert=True
        )
        if username:
            await db.users.update_one({"username": username}, {"$set": {"user_id": uid, "username": username, "full_name": message.from_user.full_name, "last_seen": now_db(), "started": True}}, upsert=True)
    except Exception as e:
        logger.warning(f"User save failed: {e}")

    if username:
        pending = await db.granted_users.find_one({"username": username, "pending": True})
        if pending:
            granted_users.add(uid)
            await db.granted_users.update_one(
                {"username": username},
                {"$set": {"user_id": uid, "username": username, "full_name": message.from_user.full_name, "pending": False}}
            )
            logger.info(f"✅ Pending grant activated: @{username} = {uid}")

    if not is_admin(uid):
        reg_code = await get_or_create_reg_code(uid)
        is_denied = await db.denied_users.find_one({"user_id": uid}) is not None
        prev = await db.granted_users.find_one({"user_id": uid})
        is_old = is_denied or (prev is not None)
        emoji_status = "🔴 old" if is_old else "🆕 new"
        user = message.from_user
        uname = f"@{user.username}" if user.username else "N/A"
        grant_str = f"@{user.username}" if user.username else str(uid)
        escaped_name = md(user.full_name or "Unknown")
        escaped_uname = md(uname or "N/A")
        admin_text = (
            f"👤 {escaped_name} /start\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🎫 Code: *{reg_code}*\n"
            f"🆔 User ID: `{uid}`\n"
            f"📛 Name: {escaped_name}\n"
            f"🔗 Username: {escaped_uname}\n"
            f"📊 Status: {emoji_status}\n"
            f"✅ Access: `/grant {grant_str}`"
        )
        try:
            await bot.send_message(ADMIN_ID, admin_text, parse_mode="Markdown")
        except Exception as admin_err:
            logger.warning(f"Failed to send admin onboarding notification in Markdown: {admin_err}. Trying plain text.")
            try:
                await bot.send_message(ADMIN_ID, admin_text)
            except Exception as admin_err_fallback:
                logger.error(f"Failed to send admin onboarding notification in plain text: {admin_err_fallback}")
        await message.answer(
            f"☁️ *Personal Cloud Bot*\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Yeh ek private cloud storage bot hai.\n"
            f"Abhi aapke paas is bot ka access nahi hai.\n\n"
            f"🆔 /id — Apna User ID dekho",
            parse_mode="Markdown"
        )
        return

    welcome_text = (
        "☁️ *B2 CLOUD*\n\n"
        f"👋 Welcome, *{md(message.from_user.full_name or 'Friend')}*!\n\n"
        "⚡ Apna personal cloud yahin manage karo.\n\n"
        "📦 Unlimited Albums\n"
        "🔒 Secure Storage\n"
        "⚡ Fast Search\n"
        "📤 Easy Sharing\n"
        "🗜 ZIP Export\n\n"
        "━━━━━━━━━━━━━━\n"
        "🚀 *Quick Start*\n\n"
        "🆕 New Album\n"
        "➜ /album `<name>`\n\n"
        "📂 My Albums\n"
        "➜ /albums\n\n"
        "❌ /close\n"
        "➜ Current upload session band karo\n\n"
        "📊 Statistics\n"
        "➜ /stats\n\n"
        "⚙️ *More Commands*\n"
        "Menu Button 👇 or /help"
    )

    if is_owner(uid):
        owner_extra = (
            "\n\n👑 *Owner Controls*\n"
            "┣ /grant `<id/@user>` — Access do\n"
            "┣ /denied `<id/@user>` — Access hatao\n"
            "┣ /info `<id/@user>` — User profile info\n"
            "┣ /makelist `<title>` — Checklist banao\n"
            "┣ /list `<title>` — Granted + History"
        )
        await message.answer(welcome_text + owner_extra, parse_mode="Markdown")
    else:
        await message.answer(welcome_text, parse_mode="Markdown")


# ============================================================
# ALBUM CREATION - /album
# ============================================================
@dp.message(Command("album"))
async def cmd_album(message: types.Message):
    if not is_admin(message.from_user.id):
        return await message.answer("🚫 Access Denied!")

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.answer("❌ Usage: `/album TripName`", parse_mode="Markdown")

    name = args[1].strip()
    existing = await albums_col.find_one({"name": {"$regex": f"^{re.escape(name)}$", "$options": "i"}})
    if existing:
        return await message.answer(
            f"⚠️ Album **'{name}'** already exists!\n"
            f"ID: `{existing['album_id']}` | Files: {existing['count']}\n"
            f"Use `/add {name}` to add more files.",
            parse_mode="Markdown"
        )

    if message.from_user.id in user_sessions:
        active = user_sessions[message.from_user.id]
        files_count = len(active.get("photos", []))
        builder = InlineKeyboardBuilder()
        builder.row(
            types.InlineKeyboardButton(text="❌ Pehla Cancel Karo", callback_data="warn_cancel_first"),
            types.InlineKeyboardButton(text="✅ Pehla Save Karo", callback_data="warn_save_first"),
        )
        return await message.answer(
            f"⚠️ **Active Session Already Hai!**\n\n"
            f"📁 Album: **{active.get('name', '?')}**\n"
            f"🗂 Files: {files_count} abhi tak\n\n"
            f"Pehle is session ko `/close` karo,\ntabhi naya album bana sakte ho!",
            reply_markup=builder.as_markup(),
            parse_mode="Markdown"
        )

    password_pending.pop(message.from_user.id, None)
    user_sessions[message.from_user.id] = {
        "mode": "create", "name": name,
        "photos": [], "ids": set(), "started_at": now_db(),
        "current_folder": "root"
    }

    await message.answer(
        f"📸 **Album Creation Started!**\n\n"
        f"📁 Name: **{name}**\n"
        f"📤 Files send (photo/video/pdf/audio/text)\n"
        f"✅ Done? `/close` kare\n",
        parse_mode="Markdown"
    )


# ============================================================
# MEDIA HANDLER
# ============================================================
async def _handle_media(message: types.Message, file_id: str, unique_id: str, media_type: str, fname: str = "", file_size: int = 0):
    uid = message.from_user.id
    if not check_rate_limit(uid):
        return await message.reply("⚠️ Rate limit reached. Thoda wait karke upload karein.")
    if await cleanup_expired_session(uid):
        return await message.answer("⏰ Session timeout ho gaya. Dobara /album ya /add start karo.")
    ok, limit_msg = await storage_limit_ok(file_size)
    if not ok:
        return await message.reply(limit_msg)
    if uid not in user_sessions:
        return
    session = user_sessions[uid]
    if unique_id in session["ids"]:
        return await message.reply(f"🚫 Duplicate {media_type}! Skip kar diya.")
    item = {
        "file_id": file_id,
        "type": media_type,
        "name": fname,
        "file_size": file_size,
        "message_id": message.message_id,
        "order": message.message_id,
        "seq": len(session.get("photos", [])) + 1,
        "folder": get_session_folder(session),
    }
    item["sig"] = file_signature(item)
    session["photos"].append(item)
    session["ids"].add(unique_id)

@dp.message(F.photo)
async def handle_photo(message: types.Message):
    if message.from_user.id not in user_sessions: return
    p = message.photo[-1]
    await _handle_media(message, p.file_id, p.file_unique_id, "photo", file_size=p.file_size or 0)

@dp.message(F.video)
async def handle_video(message: types.Message):
    if message.from_user.id not in user_sessions: return
    await _handle_media(message, message.video.file_id, message.video.file_unique_id, "video", file_size=message.video.file_size or 0)

@dp.message(F.document)
async def handle_document(message: types.Message):
    if message.from_user.id not in user_sessions: return
    d = message.document
    await _handle_media(message, d.file_id, d.file_unique_id, "document", d.file_name or "", file_size=d.file_size or 0)

@dp.message(F.audio)
async def handle_audio(message: types.Message):
    if message.from_user.id not in user_sessions: return
    await _handle_media(message, message.audio.file_id, message.audio.file_unique_id, "audio", file_size=message.audio.file_size or 0)

@dp.message(F.voice)
async def handle_voice(message: types.Message):
    if message.from_user.id not in user_sessions: return
    await _handle_media(message, message.voice.file_id, message.voice.file_unique_id, "voice", file_size=message.voice.file_size or 0)


# ============================================================
# Quick action callbacks
# ============================================================
@dp.callback_query(F.data == "quick_close")
async def quick_close(callback: types.CallbackQuery):
    await callback.answer()
    uid = callback.from_user.id
    if uid not in user_sessions or user_sessions[uid]["mode"] != "create":
        return await callback.message.answer("⚠️ Koi active album creation session nahi hai.")
    session = user_sessions[uid]
    if not session["photos"]:
        del user_sessions[uid]
        return await callback.message.answer("⚠️ Koi file nahi thi. Session cancel ho gaya.")
    auto_id = f"ALB-{now_ist().strftime('%y%m%d%H%M')}"
    duration = (now_ist() - session["started_at"]).seconds // 60
    photos, videos, docs, audios = count_media(session["photos"])
    stats = ""
    if photos: stats += f"📸 {photos} photos\n"
    if videos: stats += f"🎥 {videos} videos\n"
    if docs: stats += f"📄 {docs} documents\n"
    if audios: stats += f"🎵 {audios} audio\n"
    preview_caption = (
        f"📝 **ALBUM PREVIEW**\n━━━━━━━━━━━━━━━━━━\n"
        f"📁 Name: **{session['name']}**\n🆔 ID: `{auto_id}`\n{stats}"
        f"⏱ Session: ~{duration} min\n━━━━━━━━━━━━━━━━━━\nSave karna chahte hain?"
    )
    builder = InlineKeyboardBuilder()
    builder.row(
        types.InlineKeyboardButton(text="✅ Save Album", callback_data="confirm_save"),
        types.InlineKeyboardButton(text="❌ Cancel", callback_data="confirm_cancel")
    )
    first = next((i for i in session["photos"] if isinstance(i, dict) and i.get("type") != "text"), session["photos"][0])
    fid = first["file_id"] if isinstance(first, dict) else first
    mtype = first.get("type", "photo") if isinstance(first, dict) else "photo"
    try:
        if mtype == "text":
            await callback.message.answer(preview_caption, reply_markup=builder.as_markup(), parse_mode="Markdown")
        elif mtype == "video":
            await bot.send_video(callback.message.chat.id, fid, caption=preview_caption, reply_markup=builder.as_markup(), parse_mode="Markdown")
        elif mtype == "document":
            await bot.send_document(callback.message.chat.id, fid, caption=preview_caption, reply_markup=builder.as_markup(), parse_mode="Markdown")
        elif mtype == "audio":
            await bot.send_audio(callback.message.chat.id, fid, caption=preview_caption, reply_markup=builder.as_markup(), parse_mode="Markdown")
        elif mtype == "voice":
            await bot.send_voice(callback.message.chat.id, fid, caption=preview_caption, reply_markup=builder.as_markup(), parse_mode="Markdown")
        else:
            await bot.send_photo(callback.message.chat.id, fid, caption=preview_caption, reply_markup=builder.as_markup(), parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Preview error: {e}")
        try:
            await callback.message.answer(preview_caption, reply_markup=builder.as_markup(), parse_mode="Markdown")
        except Exception as e2:
            logger.error(f"Text fallback error in quick_close: {e2}")
            await callback.message.answer("❌ Preview generate nahi ho saca.")

@dp.callback_query(F.data == "quick_save_add")
async def quick_save_add_cb(callback: types.CallbackQuery):
    await callback.answer()
    uid = callback.from_user.id
    if uid not in user_sessions or user_sessions[uid]["mode"] != "add":
        return await callback.message.answer("⚠️ Koi active add session nahi hai.")
    session = user_sessions.pop(uid)
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except: pass

    if not session["photos"]:
        return await callback.message.answer("⚠️ Koi file nahi bheji.")
    new_count = len(session["photos"])
    new_photos, new_videos, new_docs, new_audios = count_media(session["photos"])
    user_cb = callback.from_user
    user_info_cb = f"@{user_cb.username}" if user_cb.username else f"ID: {user_cb.id}"

    add_msg_id3 = None
    try:
        add_msg3 = await send_storage_channel_message(
            f"📁 **Files Added**\nName: {session['name']}\nBy: {user_info_cb}",
            parse_mode="Markdown",
        )
        add_msg_id3 = add_msg3.message_id if add_msg3 else None
    except: pass

    saved_items = await process_and_save_items(session["photos"])

    await albums_col.update_one(
        {"album_id": session["album_id"]},
        {
            "$push": {"photos": {"$each": saved_items}, "history": {"action": "added", "count": new_count, "by": uid, "at": now_db()}},
            "$inc": {"count": new_count, "media_count.photos": new_photos, "media_count.videos": new_videos, "media_count.docs": new_docs, "media_count.audios": new_audios},
            "$set": {"updated_at": now_db()},
            "$addToSet": {"folders": {"$each": unique_folders_from_files(saved_items)}}
        }
    )

    try:
        await send_storage_channel_message(
            f"➕ **Files Added**\n📁 {session['name']} | 🆔 `{session['album_id']}`\n"
            f"🗂 +{new_count} files\n🕐 {now_ist().strftime('%d %b %Y, %I:%M %p')} IST",
            parse_mode="Markdown",
        )
    except: pass

    await albums_col.update_one(
        {"album_id": session["album_id"]},
        {"$push": {"add_history": {"msg_id": add_msg_id3, "count": new_count, "folder": get_session_folder(session), "at": now_db()}}}
    )
    await notify_b2gpt_files_added(
        session["name"], session["album_id"], user_info_cb, new_count, get_session_folder(session),
    )
    await update_checklist()
    await callback.message.answer(f"✅ **+{new_count} items** add ho gaye!\n📁 **{session['name']}**", parse_mode="Markdown")

@dp.callback_query(F.data == "quick_cancel")
async def quick_cancel_cb(callback: types.CallbackQuery):
    await callback.answer()
    uid = callback.from_user.id
    if uid in user_sessions:
        del user_sessions[uid]
    await callback.message.answer("❌ Session cancel ho gaya.")


@dp.callback_query(F.data == "warn_cancel_first")
async def warn_cancel_first(callback: types.CallbackQuery):
    await callback.answer()
    uid = callback.from_user.id
    if uid in user_sessions:
        session = user_sessions[uid]
        files_count = len(session.get("photos", []))
        del user_sessions[uid]
        await callback.message.edit_text(
            f"❌ **Pehla session cancel ho gaya!**\n"
            f"📁 {session.get('name', '?')} | 🗂 {files_count} files discard\n\n"
            f"Ab /album se naya album banao.",
            parse_mode="Markdown"
        )
    else:
        await callback.message.edit_text("⚠️ Koi active session nahi tha.", parse_mode="Markdown")

@dp.callback_query(F.data == "warn_save_first")
async def warn_save_first(callback: types.CallbackQuery):
    await callback.answer()
    uid = callback.from_user.id
    if uid not in user_sessions:
        return await callback.message.edit_text("⚠️ Session expire ho gaya.", parse_mode="Markdown")
    await callback.message.edit_text(
        "✅ Theek hai! Pehle /close karke save karo,\nphir naya album banao.",
        parse_mode="Markdown"
    )

# ============================================================
# /close
# ============================================================
@dp.message(Command("close"))
async def cmd_close(message: types.Message):
    uid = message.from_user.id
    password_pending.pop(uid, None)

    if uid in user_sessions:
        session = user_sessions[uid]
        mode = session.get("mode")
        if mode in ("create", "add"):
            if not session["photos"]:
                del user_sessions[uid]
                return await message.answer("⚠️ Koi file nahi thi. Session cancel.")

            # Build preview
            duration = (now_db() - session["started_at"]).seconds // 60
            photos, videos, docs, audios = count_media(session["photos"])
            stats = ""
            if photos: stats += f"📸 {photos} photos\n"
            if videos: stats += f"🎥 {videos} videos\n"
            if docs: stats += f"📄 {docs} documents\n"
            if audios: stats += f"🎵 {audios} audio\n"

            if mode == "create":
                auto_id = f"ALB-{now_ist().strftime('%y%m%d%H%M')}"
                preview_caption = (
                    f"📝 **ALBUM PREVIEW**\n━━━━━━━━━━━━━━━━━━\n"
                    f"📁 Name: **{session['name']}**\n🆔 ID: `{auto_id}`\n{stats}"
                    f"⏱ Session: ~{duration} min\n━━━━━━━━━━━━━━━━━━\nSave karna chahte hain?"
                )
                builder = InlineKeyboardBuilder()
                builder.row(
                    types.InlineKeyboardButton(text="✅ Save Album", callback_data="confirm_save"),
                    types.InlineKeyboardButton(text="❌ Cancel", callback_data="confirm_cancel")
                )
            else: # mode == "add"
                preview_caption = (
                    f"📝 **ADDITION PREVIEW**\n━━━━━━━━━━━━━━━━━━\n"
                    f"📁 Name: **{session['name']}**\n🆔 ID: `{session['album_id']}`\n{stats}"
                    f"⏱ Session: ~{duration} min\n━━━━━━━━━━━━━━━━━━\nSave karna chahte hain?"
                )
                builder = InlineKeyboardBuilder()
                builder.row(
                    types.InlineKeyboardButton(text="✅ Save Addition", callback_data="quick_save_add"),
                    types.InlineKeyboardButton(text="❌ Cancel", callback_data="confirm_cancel")
                )

            first = next((i for i in session["photos"] if isinstance(i, dict) and i.get("type") != "text"), session["photos"][0])
            fid = first["file_id"] if isinstance(first, dict) else first
            mtype = first.get("type", "photo") if isinstance(first, dict) else "photo"

            try:
                if mtype == "text":
                    await message.answer(preview_caption, reply_markup=builder.as_markup(), parse_mode="Markdown")
                elif mtype == "video":
                    await bot.send_video(message.chat.id, fid, caption=preview_caption, reply_markup=builder.as_markup(), parse_mode="Markdown")
                elif mtype == "document":
                    await bot.send_document(message.chat.id, fid, caption=preview_caption, reply_markup=builder.as_markup(), parse_mode="Markdown")
                elif mtype == "audio":
                    await bot.send_audio(message.chat.id, fid, caption=preview_caption, reply_markup=builder.as_markup(), parse_mode="Markdown")
                elif mtype == "voice":
                    await bot.send_voice(message.chat.id, fid, caption=preview_caption, reply_markup=builder.as_markup(), parse_mode="Markdown")
                else:
                    await bot.send_photo(message.chat.id, fid, caption=preview_caption, reply_markup=builder.as_markup(), parse_mode="Markdown")
            except Exception as e:
                logger.error(f"Preview send error: {e}")
                try:
                    await message.answer(preview_caption, reply_markup=builder.as_markup(), parse_mode="Markdown")
                except Exception as e2:
                    logger.error(f"Text fallback error: {e2}")
                    await message.answer(f"❌ Preview error: {e}", parse_mode="Markdown")
            return

        elif mode == "dlt":
            del user_sessions[uid]
            return await message.answer("❌ Delete operation cancel.", parse_mode="Markdown")

    # Stop view session
    if view_sessions.get(uid):
        view_sessions[uid] = False
        return await message.answer("⏹ View band kar diya!")

    # Stop /b2 transmission
    if uid in b2_active_sessions:
        session_info = b2_active_sessions[uid]
        b2_cancel_flags.add(uid)
        target_uid = session_info.get("target_uid")
        sent_ids = session_info.get("sent_msg_ids", [])
        
        # Delete already sent messages immediately
        deleted_count = 0
        for mid in sent_ids:
            try:
                await bot.delete_message(target_uid, mid)
                deleted_count += 1
            except Exception as e:
                logger.debug(f"Failed to delete message {mid} in /close b2 cancel: {e}")
                
        # Send confirmation
        await message.answer(f"⛔ /b2 stopped. Deleted {deleted_count} already sent files from target chat.")
        return

    if uid in b2_cancel_flags:
        return await message.answer("⛔ Stop signal already sent.")
    b2_cancel_flags.add(uid)
    await message.answer("⛔ Stop signal sent. Agar /b2 chal raha hai to ruk jayega.")


# ============================================================
# CONFIRM SAVE / CANCEL
# ============================================================
@dp.callback_query(F.data.in_({"confirm_save", "confirm_cancel"}))
async def process_confirm(callback: types.CallbackQuery):
    uid = callback.from_user.id
    session = user_sessions.pop(uid, None)
    if not session:
        await callback.answer("Session already processed or expired!", show_alert=True)
        try: await callback.message.delete()
        except: pass
        return

    if callback.data == "confirm_save":
        album_id = f"ALB-{now_ist().strftime('%y%m%d%H%M%S')}"
        photos, videos, docs, audios = count_media(session["photos"])

        await callback.answer("⏳ Saving...")
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except: pass

        save_msg = await callback.message.answer(
            f"⏳ **Saving album...**\n📁 {session['name']}\n_Files storage pe upload ho rahi hain..._",
            parse_mode="Markdown"
        )

        user = callback.from_user
        user_info = f"@{user.username}" if user.username else f"ID: {user.id}"

        created_msg_id = None
        try:
            created_msg = await send_storage_channel_message(
                f"📁 **Album Created**\nName: {session['name']}\nCreated by: {user_info}",
                parse_mode="Markdown",
            )
            created_msg_id = created_msg.message_id if created_msg else None
        except: pass

        total_files = len(session["photos"])
        try:
            await save_msg.edit_text(
                f"⏳ Uploading... 0/{total_files}\n📁 {session['name']}",
                parse_mode="Markdown"
            )
        except: pass
        saved_items = await process_and_save_items(session["photos"])
        try:
            await save_msg.edit_text(
                f"⏳ Uploading... {len(saved_items)}/{total_files}\n📁 {session['name']}",
                parse_mode="Markdown"
            )
        except: pass

        photos, videos, docs, audios = count_media(saved_items)

        album_doc = {
            "album_id": album_id,
            "name": session["name"],
            "photos": saved_items,
            "count": len(saved_items),
            "locked": False,
            "tags": auto_generate_tags(session["name"]),
            "created_by": uid,
            "created_by_username": callback.from_user.username or "",
            "created_at": now_db(),
            "updated_at": now_db(),
            "history": [{"action": "created", "count": len(saved_items), "by": uid, "at": now_db()}],
            "media_count": {"photos": photos, "videos": videos, "docs": docs, "audios": audios},
            "folders": unique_folders_from_files(saved_items),
            "created_msg_id": created_msg_id
        }

        db_saved = False
        try:
            await albums_col.insert_one(album_doc)
            db_saved = True
        except Exception as e:
            logger.error(f"MongoDB insert error: {e}")
            existing = await albums_col.find_one({"album_id": album_id})
            if existing:
                db_saved = True

        stats_text = ""
        if photos: stats_text += f"📸 {photos} "
        if videos: stats_text += f"🎥 {videos} "
        if docs:   stats_text += f"📄 {docs} "
        if audios: stats_text += f"🎵 {audios} "

        if db_saved:
            try:
                await send_storage_channel_message(
                    f"✅ **Album Saved & Stored**\n"
                    f"🆔 ID: `{album_id}`\n"
                    f"📁 Name: {session['name']}\n"
                    f"🗂 Files: {len(saved_items)} ({stats_text.strip()})\n"
                    f"🕐 {now_ist().strftime('%d %b %Y, %I:%M %p')} IST",
                    parse_mode="Markdown",
                )
            except Exception as e:
                logger.error(f"Storage channel message error: {e}")

            await notify_b2gpt_album_created(session["name"], album_id, user_info, len(saved_items))

            await update_checklist()

            try: await save_msg.delete()
            except: pass
            try: await callback.message.delete()
            except: pass

            await callback.message.answer(
                f"✅ **Successfully Saved!**\n\n"
                f"📁 Album: **{session['name']}**\n"
                f"🆔 `{album_id}`\n"
                f"🗂 {len(saved_items)} items",
                parse_mode="Markdown"
            )
        else:
            try:
                await save_msg.edit_text(
                    f"❌ **Save error!**\n📁 {session['name']}\nRetry: `/album {session['name']}`",
                    parse_mode="Markdown"
                )
            except: pass
    else:
        await callback.answer("❌ Cancelled")
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except: pass
        await callback.message.answer("❌ Album save cancel.")




# ============================================================
# /add
# ============================================================
@dp.message(Command("add"))
async def cmd_add(message: types.Message):
    if not is_admin(message.from_user.id):
        return await message.answer("🚫 Access Denied!")
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.answer("❌ Usage: `/add AlbumName` ya `/add ALB-xxx FolderName`", parse_mode="Markdown")

    raw = args[1].strip()
    album, rest = await parse_album_and_rest(raw)
    folder = normalize_folder(rest) if rest else "root"

    if not album:
        return await message.answer(f"❌ **'{raw}'** nahi mila.", parse_mode="Markdown")

    # Same album me same folder name dobara create nahi hoga; existing folder reuse hoga.
    folder = canonical_folder_name(album.get("folders", []), folder)

    if album.get("locked"):
        return await message.answer(f"🔒 **'{album['name']}'** locked hai! Pehle `/unlock` karein.", parse_mode="Markdown")
    if message.from_user.id in user_sessions:
        del user_sessions[message.from_user.id]

    password_pending.pop(message.from_user.id, None)
    user_sessions[message.from_user.id] = {
        "mode": "add", "db_id": album["_id"],
        "album_id": album["album_id"], "name": album["name"],
        "photos": [], "ids": set(album.get("photo_unique_ids", [])),
        "started_at": now_db(), "current_folder": folder
    }
    if folder != "root":
        await albums_col.update_one({"_id": album["_id"]}, {"$addToSet": {"folders": folder}})

    folder_line = f"\n📂 Folder: **{md(folder)}**" if folder != "root" else ""
    await message.answer(
        f"➕ **Adding to: {md(album['name'])}**{folder_line}\n🆔 `{album['album_id']}` | Current: {album['count']} files\n\n"
        f"Files send (photo/video/pdf/audio/text), phir `/close`\n",
        parse_mode="Markdown"
    )

# ============================================================
# /save_add
# ============================================================
@dp.message(Command("save_add"))
async def save_add(message: types.Message):
    uid = message.from_user.id
    if uid not in user_sessions or user_sessions[uid]["mode"] != "add":
        return await message.answer("⚠️ Koi active add session nahi hai.")
    session = user_sessions[uid]
    if not session["photos"]:
        del user_sessions[uid]
        return await message.answer("⚠️ Koi file nahi bheji. Session cancel.")
    try:
        new_count = len(session["photos"])
        new_photos, new_videos, new_docs, new_audios = count_media(session["photos"])
        user = message.from_user
        user_info = f"@{user.username}" if user.username else f"ID: {user.id}"
        add_msg_id2 = None
        try:
            add_msg2 = await send_storage_channel_message(
                f"📁 **Files Added**\nName: {session['name']}\nBy: {user_info}",
                parse_mode="Markdown",
            )
            add_msg_id2 = add_msg2.message_id if add_msg2 else None
        except: pass

        saved_items = await process_and_save_items(session["photos"])

        await albums_col.update_one(
            {"album_id": session["album_id"]},
            {
                "$push": {"photos": {"$each": saved_items}, "history": {"action": "added", "count": new_count, "by": uid, "at": now_db()}},
                "$inc": {"count": new_count, "media_count.photos": new_photos, "media_count.videos": new_videos, "media_count.docs": new_docs, "media_count.audios": new_audios},
                "$set": {"updated_at": now_db()},
                "$addToSet": {"folders": {"$each": unique_folders_from_files(saved_items)}}
            }
        )

        try:
            await send_storage_channel_message(
                f"➕ **Files Added**\n📁 {session['name']} | 🆔 `{session['album_id']}`\n"
                f"🗂 +{new_count} files\n🕐 {now_ist().strftime('%d %b %Y, %I:%M %p')} IST",
                parse_mode="Markdown",
            )
        except: pass

        await albums_col.update_one(
            {"album_id": session["album_id"]},
            {"$push": {"add_history": {"msg_id": add_msg_id2, "count": new_count, "folder": get_session_folder(session), "at": now_db()}}}
        )
        await notify_b2gpt_files_added(
            session["name"], session["album_id"], user_info, new_count, get_session_folder(session),
        )
        await update_checklist()
        await message.answer(f"✅ **+{new_count} items** add ho gaye!\n📁 **{session['name']}**", parse_mode="Markdown")
    except Exception as e:
        logger.error(f"save_add error: {e}")
        await message.answer("❌ Save error. Retry karein.")
    del user_sessions[uid]


# ============================================================
# /lock & /unlock
# ============================================================
@dp.message(Command("lock"))
async def cmd_lock(message: types.Message):
    if not is_admin(message.from_user.id): return await message.answer("🚫 Access Denied!")
    args = message.text.split(maxsplit=1)
    if len(args) < 2: return await message.answer("❌ Usage: `/lock AlbumName`", parse_mode="Markdown")
    album = await find_album(args[1].strip())
    if not album: return await message.answer("❌ Album nahi mila.", parse_mode="Markdown")
    await albums_col.update_one({"_id": album["_id"]}, {"$set": {"locked": True, "updated_at": now_db()}})
    await message.answer(f"🔒 **'{album['name']}'** locked!", parse_mode="Markdown")

@dp.message(Command("unlock"))
async def cmd_unlock(message: types.Message):
    if not is_admin(message.from_user.id): return await message.answer("🚫 Access Denied!")
    args = message.text.split(maxsplit=1)
    if len(args) < 2: return await message.answer("❌ Usage: `/unlock AlbumName`", parse_mode="Markdown")
    album = await find_album(args[1].strip())
    if not album: return await message.answer("❌ Album nahi mila.", parse_mode="Markdown")
    await albums_col.update_one({"_id": album["_id"]}, {"$set": {"locked": False, "updated_at": now_db()}})
    await message.answer(f"🔓 **'{album['name']}'** unlocked!", parse_mode="Markdown")


# ============================================================
# /rename
# ============================================================
@dp.message(Command("rename"))
async def cmd_rename(message: types.Message):
    if not is_admin(message.from_user.id): return await message.answer("🚫 Access Denied!")
    parts = message.text.split(maxsplit=1)
    text = parts[1].strip() if len(parts) > 1 else ""
    if not text:
        return await message.answer(
            "❌ Usage:\n"
            "`/rename ALB-xxx New Name`\n"
            "`/rename 'Old Name' 'New Name'`\n"
            "`/rename OldSingleWord NewName`",
            parse_mode="Markdown"
        )

    old_name = new_name = None

    alb_match = re.match(r"^(ALB-\S+)\s+(.+)$", text, re.IGNORECASE)
    if alb_match:
        old_name = alb_match.group(1).strip()
        new_name = alb_match.group(2).strip()

    if not old_name:
        quoted = re.findall(r"['\"](.+?)['\"]", text)
        if len(quoted) >= 2:
            old_name, new_name = quoted[0].strip(), quoted[1].strip()

    if not old_name:
        tokens = text.split()
        found = False
        for i in range(1, len(tokens)):
            candidate = " ".join(tokens[:i])
            alb = await find_album(candidate)
            if alb:
                old_name = candidate
                new_name = " ".join(tokens[i:])
                found = True
                break
        if not found:
            return await message.answer(
                "❌ Album nahi mila.\n\n"
                "💡 ID use karo:\n`/rename ALB-xxx New Album Name`",
                parse_mode="Markdown"
            )

    album = await find_album(old_name)
    if not album:
        return await message.answer(f"❌ **'{old_name}'** nahi mila.", parse_mode="Markdown")
    if not new_name:
        return await message.answer("❌ Naya naam dein.", parse_mode="Markdown")
    conflict = await albums_col.find_one({"name": {"$regex": f"^{re.escape(new_name)}$", "$options": "i"}})
    if conflict: return await message.answer(f"⚠️ **'{new_name}'** already exists!", parse_mode="Markdown")
    await albums_col.update_one({"_id": album["_id"]}, {"$set": {"name": new_name, "tags": auto_generate_tags(new_name), "updated_at": now_db()}})
    await update_checklist()
    await message.answer(f"📝 **{album['name']}** → **{new_name}**", parse_mode="Markdown")


# ============================================================
# /delete
# ============================================================
@dp.message(Command("delete"))
async def cmd_delete(message: types.Message):
    if not is_admin(message.from_user.id): return await message.answer("🚫 Access Denied!")
    args = message.text.split(maxsplit=1)
    if len(args) < 2: return await message.answer("❌ Usage: `/delete AlbumName`", parse_mode="Markdown")
    album = await find_album(args[1].strip())
    if not album: return await message.answer("❌ Album nahi mila.", parse_mode="Markdown")
    builder = InlineKeyboardBuilder()
    builder.row(
        types.InlineKeyboardButton(text="🗑️ Haan, Delete", callback_data=f"del_yes_{album['album_id']}"),
        types.InlineKeyboardButton(text="❌ Cancel", callback_data="del_no")
    )
    await message.answer(
        f"⚠️ **Delete Confirmation**\n\n📁 **{album['name']}**\n🆔 `{album['album_id']}`\n🗂 {album['count']} files\n\nYeh action **undo nahi** ho sakta!",
        reply_markup=builder.as_markup(), parse_mode="Markdown"
    )

@dp.callback_query(F.data.startswith("del_"))
async def process_delete(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return await callback.answer("🚫 Access Denied!", show_alert=True)
    if callback.data == "del_no":
        await callback.answer("❌ Cancel")
        return await callback.message.edit_text("❌ Delete cancel.", parse_mode="Markdown")
    album_id = callback.data.replace("del_yes_", "")
    result = await albums_col.delete_one({"album_id": album_id})
    if result.deleted_count:
        try:
            await send_storage_channel_message(
                f"🗑️ **Album Deleted**\nID: `{album_id}`\n🕐 {now_ist().strftime('%d %b %Y, %I:%M %p')} IST",
                parse_mode="Markdown",
            )
        except: pass
        await update_checklist()
        await callback.message.edit_text(f"🗑️ Album deleted!\nID: `{album_id}`", parse_mode="Markdown")
    else:
        await callback.message.edit_text("❌ Delete nahi ho saka.", parse_mode="Markdown")
    await callback.answer()


# ============================================================
# /dlt - Selective file delete
# ============================================================
@dp.message(Command("dlt"))
async def cmd_dlt(message: types.Message):
    if not is_admin(message.from_user.id): return await message.answer("🚫 Access Denied!")
    args = message.text.split(maxsplit=1)
    if len(args) < 2: return await message.answer("❌ Usage: `/dlt AlbumName` ya `/dlt ALB-xxx`", parse_mode="Markdown")
    album = await find_album(args[1].strip())
    if not album: return await message.answer("❌ Album nahi mila.", parse_mode="Markdown")
    if album.get("locked"): return await message.answer("🔒 Album locked hai!", parse_mode="Markdown")
    files = album.get("photos", [])
    if not files: return await message.answer("❌ Album empty hai.", parse_mode="Markdown")
    user_sessions[message.from_user.id] = {
        "mode": "dlt", "album_id": album["album_id"],
        "album_name": album["name"], "files": files, "selected": set()
    }
    await message.answer(
        f"🗑️ **Selective Delete: {album['name']}**\n🗂 {len(files)} files\n\nAb files bhej raha hoon — har ek ke niche ✅/❌ button hoga.",
        parse_mode="Markdown"
    )
    for idx, item in enumerate(files):
        fid = item["file_id"] if isinstance(item, dict) else item
        mtype = item.get("type", "photo") if isinstance(item, dict) else "photo"
        kb = InlineKeyboardBuilder()
        kb.button(text="✅ Keep", callback_data=f"dlt_toggle_{album['album_id']}_{idx}_keep")
        caption = f"File #{idx+1} | {mtype}"
        try:
            if mtype == "text":
                text_val = item.get("text", "") if isinstance(item, dict) else ""
                await message.answer(f"📝 #{idx+1}: {text_val}", reply_markup=kb.as_markup())
            elif mtype == "video": await bot.send_video(message.chat.id, fid, caption=caption, reply_markup=kb.as_markup())
            elif mtype == "document": await bot.send_document(message.chat.id, fid, caption=caption, reply_markup=kb.as_markup())
            else: await bot.send_photo(message.chat.id, fid, caption=caption, reply_markup=kb.as_markup())
        except: pass
        await asyncio.sleep(0.3)
    action_kb = InlineKeyboardBuilder()
    action_kb.row(
        types.InlineKeyboardButton(text="👁 Preview Deletions", callback_data=f"dlt_preview_{album['album_id']}"),
        types.InlineKeyboardButton(text="💾 Save Changes", callback_data=f"dlt_save_{album['album_id']}"),
        types.InlineKeyboardButton(text="❌ Cancel", callback_data="dlt_cancel")
    )
    await message.answer("⬆️ **✅ Keep** pe click karein delete karne ke liye.\nPhir **Save Changes** dabayein.", reply_markup=action_kb.as_markup(), parse_mode="Markdown")

@dp.callback_query(F.data.startswith("dlt_toggle_"))
async def dlt_toggle(callback: types.CallbackQuery):
    uid = callback.from_user.id
    if uid not in user_sessions or user_sessions[uid].get("mode") != "dlt":
        return await callback.answer("Session expire ho gaya.", show_alert=True)
    parts = callback.data.split("_")
    idx = int(parts[-2])
    session = user_sessions[uid]
    if idx in session["selected"]:
        session["selected"].discard(idx)
        new_btn, new_cb = "✅ Keep", f"dlt_toggle_{session['album_id']}_{idx}_keep"
    else:
        session["selected"].add(idx)
        new_btn, new_cb = "❌ Delete", f"dlt_toggle_{session['album_id']}_{idx}_del"
    kb = InlineKeyboardBuilder()
    kb.button(text=new_btn, callback_data=new_cb)
    try:
        await callback.message.edit_reply_markup(reply_markup=kb.as_markup())
    except: pass
    await callback.answer()

@dp.callback_query(F.data.startswith("dlt_preview_"))
async def dlt_preview(callback: types.CallbackQuery):
    uid = callback.from_user.id
    if uid not in user_sessions or user_sessions[uid].get("mode") != "dlt":
        return await callback.answer("Session expire.", show_alert=True)
    session = user_sessions[uid]
    if not session["selected"]:
        return await callback.answer("Koi file select nahi ki.", show_alert=True)
    del_nums = sorted([i+1 for i in session["selected"]])
    keep_nums = sorted([i+1 for i in range(len(session["files"])) if i not in session["selected"]])
    await callback.answer()
    await callback.message.answer(
        f"👁 **Delete Preview**\n\n❌ Delete hongi: {', '.join(map(str, del_nums))}\n✅ Raheingi: {', '.join(map(str, keep_nums))}",
        parse_mode="Markdown"
    )

@dp.callback_query(F.data.startswith("dlt_save_"))
async def dlt_save(callback: types.CallbackQuery):
    uid = callback.from_user.id
    if uid not in user_sessions or user_sessions[uid].get("mode") != "dlt":
        return await callback.answer("Session expire.", show_alert=True)
    session = user_sessions[uid]
    if not session["selected"]:
        return await callback.answer("Koi file select nahi ki.", show_alert=True)
    del_nums = sorted([i+1 for i in session["selected"]])
    keep_nums = sorted([i+1 for i in range(len(session["files"])) if i not in session["selected"]])
    kb = InlineKeyboardBuilder()
    kb.row(
        types.InlineKeyboardButton(text="🗑️ Haan, Delete Karo", callback_data=f"dlt_confirm_{session['album_id']}"),
        types.InlineKeyboardButton(text="❌ Cancel", callback_data="dlt_cancel")
    )
    await callback.answer()
    await callback.message.answer(
        f"⚠️ **Delete Confirmation**\n\n📁 Album: **{session['album_name']}**\n"
        f"❌ Delete: File {', '.join(map(str, del_nums))}\n✅ Raheingi: File {', '.join(map(str, keep_nums))}\n\nKya aap sure hain? Yeh action **undo nahi** ho sakta!",
        reply_markup=kb.as_markup(), parse_mode="Markdown"
    )

@dp.callback_query(F.data.startswith("dlt_confirm_"))
async def dlt_confirm(callback: types.CallbackQuery):
    uid = callback.from_user.id
    if uid not in user_sessions or user_sessions[uid].get("mode") != "dlt":
        return await callback.answer("Session expire.", show_alert=True)
    session = user_sessions[uid]
    new_files = [f for i, f in enumerate(session["files"]) if i not in session["selected"]]
    del_count = len(session["selected"])
    photos, videos, docs, audios = count_media(new_files)
    await albums_col.update_one(
        {"album_id": session["album_id"]},
        {
            "$set": {"photos": new_files, "count": len(new_files), "updated_at": now_db(),
                     "media_count": {"photos": photos, "videos": videos, "docs": docs, "audios": audios}},
            "$push": {"history": {"action": "deleted", "count": -del_count, "by": uid, "at": now_db()}}
        }
    )
    del user_sessions[uid]
    await callback.answer("🗑️ Done!")
    await callback.message.edit_text(
        f"✅ **{del_count} files delete ho gayi!**\n📁 Album: **{session['album_name']}**\n🗂 Remaining: {len(new_files)} files",
        parse_mode="Markdown"
    )

@dp.callback_query(F.data == "dlt_cancel")
async def dlt_cancel(callback: types.CallbackQuery):
    uid = callback.from_user.id
    if uid in user_sessions and user_sessions[uid].get("mode") == "dlt":
        del user_sessions[uid]
    await callback.answer("❌ Cancel")
    await callback.message.edit_text("❌ Delete operation cancel.", parse_mode="Markdown")


# ============================================================
# /merge
# ============================================================
@dp.message(Command("merge"))
async def cmd_merge(message: types.Message):
    if not is_admin(message.from_user.id): return await message.answer("🚫 Access Denied!")
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.answer("❌ Usage: `/merge <name/id1> <name/id2> <NewName>`\n"
                                    "Quoted names ke liye: `/merge \'My Pic\' \'Tag Test\' NewAlbum`", parse_mode="Markdown")
    raw = args[1].strip()
    quoted = re.findall(r"['\"](.*?)['\"]+", raw)
    if len(quoted) >= 2:
        id1 = quoted[0].strip()
        id2 = quoted[1].strip()
        second_quote_end = raw.rfind(quoted[1]) + len(quoted[1]) + 1
        new_name = raw[second_quote_end:].strip().strip("'\"")
        if not new_name:
            return await message.answer("❌ New album ka naam dein.", parse_mode="Markdown")
    else:
        tokens = raw.split()
        if len(tokens) < 3:
            return await message.answer("❌ Usage: `/merge <name/id1> <name/id2> <NewName>`", parse_mode="Markdown")
        found = False
        for split1 in range(1, len(tokens) - 1):
            id1_try = " ".join(tokens[:split1])
            a1_try = await find_album(id1_try)
            if not a1_try: continue
            for split2 in range(split1 + 1, len(tokens)):
                id2_try = " ".join(tokens[split1:split2])
                a2_try = await find_album(id2_try)
                if not a2_try: continue
                new_name = " ".join(tokens[split2:])
                if new_name:
                    id1, id2 = id1_try, id2_try
                    found = True
                    break
            if found: break
        if not found:
            return await message.answer("❌ Albums nahi mile. Quoted names use karein:\n"
                                        "`/merge \'My Pic\' \'Tag Test\' NewAlbum`", parse_mode="Markdown")
    a1 = await find_album(id1)
    a2 = await find_album(id2)
    if not a1: return await message.answer(f"❌ Album 1 '{id1}' nahi mila.", parse_mode="Markdown")
    if not a2: return await message.answer(f"❌ Album 2 '{id2}' nahi mila.", parse_mode="Markdown")
    conflict = await albums_col.find_one({"name": {"$regex": f"^{re.escape(new_name)}$", "$options": "i"}})
    if conflict: return await message.answer(f"⚠️ **'{new_name}'** already exists!", parse_mode="Markdown")
    merged_files = a1.get("photos", []) + a2.get("photos", [])
    photos, videos, docs, audios = count_media(merged_files)
    new_id = f"ALB-{now_ist().strftime('%y%m%d%H%M%S')}"
    await albums_col.insert_one({
        "album_id": new_id, "name": new_name, "photos": merged_files, "count": len(merged_files),
        "locked": False, "tags": auto_generate_tags(new_name), "created_by": message.from_user.id,
        "created_at": now_db(), "updated_at": now_db(),
        "history": [{"action": "merged", "from": [a1["album_id"], a2["album_id"]], "by": message.from_user.id, "at": now_db()}],
        "media_count": {"photos": photos, "videos": videos, "docs": docs, "audios": audios}
    })
    await message.answer(
        f"✅ **Albums Merged!**\n\n📁 **{a1['name']}** ({a1['count']}) + **{a2['name']}** ({a2['count']})\n➡️ **{new_name}** | 🆔 `{new_id}`\n🗂 Total: {len(merged_files)} files",
        parse_mode="Markdown"
    )


# ============================================================
# /tag
# ============================================================
@dp.message(Command("tag"))
async def cmd_tag(message: types.Message):
    if not is_admin(message.from_user.id): return await message.answer("🚫 Access Denied!")
    args = message.text.split(maxsplit=1)
    if len(args) < 2: return await message.answer("❌ Usage: `/tag <name/id> #tag1 #tag2`", parse_mode="Markdown")
    text = args[1].strip()
    tag_match = re.search(r"#\w+", text)
    if not tag_match: return await message.answer("❌ Koi valid tag nahi mila. Use `#tagname`", parse_mode="Markdown")
    album_identifier = text[:tag_match.start()].strip()
    if not album_identifier: return await message.answer("❌ Album name/id dein.", parse_mode="Markdown")
    album = await find_album(album_identifier)
    if not album: return await message.answer(f"❌ Album '{album_identifier}' nahi mila.", parse_mode="Markdown")
    new_tags = re.findall(r"#\w+", text[tag_match.start():])
    if not new_tags: return await message.answer("❌ Koi valid tag nahi mila. Use `#tagname`", parse_mode="Markdown")
    existing_tags = album.get("tags", [])
    all_tags = list(set(existing_tags + [t.lower() for t in new_tags]))
    await albums_col.update_one({"_id": album["_id"]}, {"$set": {"tags": all_tags, "updated_at": now_db()}})
    await message.answer(f"🏷️ **Tags Updated!**\n📁 **{album['name']}**\nTags: {' '.join(all_tags)}", parse_mode="Markdown")


# ============================================================
# /albums
# ============================================================
@dp.message(Command("albums"))
async def cmd_list(message: types.Message):
    if not is_admin(message.from_user.id): return await message.answer("🚫 Access Denied!")
    try:
        albums = await albums_col.find().sort([("pinned", -1), ("created_at", -1)]).to_list(length=500)
        if not albums:
            return await message.answer("📂 Cloud empty hai! /album se banayein.")
        total_files = sum(a.get("count", 0) for a in albums)
        locked_count = sum(1 for a in albums if a.get("locked"))
        
        header = (
            f"☁️ *Personal Cloud*\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📊 {len(albums)} albums  🗂 {total_files} files  🔒 {locked_count} locked\n"
            f"━━━━━━━━━━━━━━━━━━\n\n"
        )
        
        messages = []
        current_msg = header
        
        for alb in albums:
            icon = "🔒" if alb.get("locked") else "📁"
            if alb.get("pinned"):
                icon = "📌" + icon
            aid  = alb.get("album_id") or "N/A"
            name = alb.get("name") or "Unnamed"
            line = f"{icon} {name}\n🆔 `{aid}`\n\n"
            
            if len(current_msg) + len(line) > 4000:
                messages.append(current_msg.strip())
                current_msg = ""
            
            current_msg += line
            
        if current_msg:
            messages.append(current_msg.strip())
            
        for msg_text in messages:
            await message.answer(msg_text, parse_mode="Markdown")
            await asyncio.sleep(0.2)
            
    except Exception as e:
        logger.error(f"/albums error: {e}")
        await message.answer(f"❌ Error: {e}")


# ============================================================
# /info
# ============================================================
@dp.message(Command("info"))
async def cmd_info(message: types.Message):
    if not is_admin(message.from_user.id): return await message.answer("🚫 Access Denied!")
    args = message.text.split(maxsplit=1)
    
    uid = message.from_user.id
    if len(args) < 2:
        if is_owner(uid):
            return await message.answer("❌ Usage: `/info <album_name/id>` ya `/info <userid/@username>`", parse_mode="Markdown")
        else:
            return await message.answer("❌ Usage: `/info <album_name/id>`", parse_mode="Markdown")

    target_arg = args[1].strip()
    
    # Check if target_arg is a user query (only for owner)
    is_user_query = False
    if is_owner(uid):
        if target_arg.startswith("@") or target_arg.lstrip("-").isdigit():
            album = await find_album(target_arg)
            if not album:
                is_user_query = True

    if is_user_query:
        if await is_hidden_b2gpt_target(target_arg):
            hint = target_arg.lstrip("@") if target_arg.startswith("@") else "User"
            return await message.answer(f"❌ {hint} nahi mila.", parse_mode="Markdown")

        target_uid = None
        tg_info    = None

        if target_arg.startswith("@"):
            uname_lookup = target_arg.lstrip("@").lower()
            doc = (await db.granted_users.find_one({"username": uname_lookup}) or
                   await db.denied_users.find_one({"username": uname_lookup}))
            if doc and doc.get("user_id"):
                target_uid = doc["user_id"]
            try:
                chat = await bot.get_chat(f"@{uname_lookup}")
                target_uid = target_uid or chat.id
                tg_info = chat
            except: pass
            if not target_uid:
                return await message.answer(f"❌ @{uname_lookup} nahi mila.", parse_mode="Markdown")
        else:
            try: target_uid = int(target_arg)
            except: return await message.answer("❌ Valid User ID ya @username dein.", parse_mode="Markdown")
            try:
                tg_info = await bot.get_chat(target_uid)
            except: pass

        tg_name     = tg_info.full_name if tg_info and hasattr(tg_info, "full_name") else None
        tg_username = tg_info.username  if tg_info else None

        granted_doc = await db.granted_users.find_one({"user_id": target_uid})
        denied_doc  = await db.denied_users.find_one({"user_id": target_uid})
        if granted_doc:
            status = "✅ Granted"
            if granted_doc.get("pending"): status = "⏳ Pending"
        elif denied_doc:
            status = "🚫 Denied"
        else:
            status = "👤 Unknown"

        albums = await albums_col.find({"created_by": target_uid}).sort("created_at", -1).to_list(50)

        text = "👤 *User Info*\n━━━━━━━━━━━━━━━━━━\n"
        if tg_name:     text += f"📛 {md(tg_name)}\n"
        if tg_username: text += f"🔗 @{md(tg_username)}\n"
        text += f"🆔 `{target_uid}`\n📊 Status: {status}\n"
        if granted_doc:
            text += f"📅 Granted: {safe_ist(granted_doc.get('granted_at', now_db()))}\n"
        elif denied_doc:
            text += f"📅 Denied: {safe_ist(denied_doc.get('denied_at', now_db()))}\n"

        text += f"\n📁 *Albums ({len(albums)}):*\n"
        if albums:
            for alb in albums:
                alb_date = alb.get("created_at", now_db()).strftime("%d %b %Y, %I:%M %p")
                text += f"\n• {md(alb['name'])}\n  🆔 `{alb['album_id']}` | 🗂 {alb['count']} files\n  📅 {alb_date}\n"
        else:
            text += "Koi album nahi banya.\n"

        try:
            await message.answer(text, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"/info user query error: {e}")
            await message.answer(text)
        return

    # Album query
    album = await find_album(target_arg)
    if not album: return await message.answer("❌ Album nahi mila.")
    mc = album.get("media_count", {})
    photos = mc.get("photos", 0)
    videos = mc.get("videos", 0)
    docs   = mc.get("docs", 0)
    audios = mc.get("audios", 0)
    if not mc:
        photos, videos, docs, audios = count_media(album.get("photos", []))
    aid  = album["album_id"]
    tags = " ".join(album.get("tags", [])) or None
    lock = "🔒 Locked" if album.get("locked") else "🔓 Unlocked"

    raw_created = album.get("created_at", now_db())
    if raw_created.tzinfo is None:
        from datetime import timezone
        raw_created = raw_created.replace(tzinfo=timezone.utc)
    created = raw_created.astimezone(IST).strftime("%d %b %Y, %I:%M %p") + " IST"

    by_username = album.get("created_by_username", "")
    by_str = f"@{by_username}" if by_username else f"`{album.get('created_by', 'N/A')}`"

    files_list = album.get("photos", [])
    total_size_bytes = sum(f.get("file_size", 0) for f in files_list if isinstance(f, dict))
    if total_size_bytes > 0:
        if total_size_bytes >= 1024 * 1024 * 1024:
            size_str = f"{total_size_bytes / (1024**3):.1f} GB"
        elif total_size_bytes >= 1024 * 1024:
            size_str = f"{total_size_bytes / (1024**2):.1f} MB"
        else:
            size_str = f"{total_size_bytes / 1024:.0f} KB"
    else:
        est = (photos * 2 + videos * 50 + docs * 5 + audios * 4)
        size_str = f"~{est} MB" if est < 1024 else f"~{est/1024:.1f} GB"

    text = (
        f"📋 Album Info\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📁 Name: {album['name']}\n"
        f"🆔 `{aid}`\n"
        f"👁 `/view {aid}`\n"
        f"📦 `/zip {aid}`\n"
        f"👤 {by_str}\n"
        f"📅 {created}\n"
        f"🔐 {lock}\n"
        f"\n🗂 Files:\n"
    )
    if photos: text += f"📸 Photos: {photos}\n"
    if videos: text += f"🎥 Videos: {videos}\n"
    if docs:   text += f"📄 Documents: {docs}\n"
    if audios: text += f"🎵 Audio: {audios}\n"
    text += f"📊 Total: {album['count']}\n"
    text += f"💾 Size: {size_str}\n"

    folders = album.get("folders") or unique_folders_from_files(album.get("photos", []))
    folders = [normalize_folder(f) for f in folders if normalize_folder(f) != "root"]
    if folders:
        text += "\n📂 Folders:\n"
        for f in sorted(set(folders), key=str.lower):
            cnt = sum(1 for it in album.get("photos", []) if isinstance(it, dict) and normalize_folder(it.get("folder", "root")) == f)
            text += f"   • {md(f)} — {cnt} files\n"

    history = album.get("history", [])
    if history:
        text += "\n📜 History:\n"
        for h in history[-5:]:
            action   = h.get("action", "")
            count    = h.get("count", 0)
            at       = h.get("at", now_db())
            date_str = at.strftime("%d %b %Y") if isinstance(at, datetime) else str(at)
            if action == "created":  text += f"   Created | {date_str}\n"
            elif action == "added":  text += f"   +{count} files | {date_str}\n"
            elif action == "deleted":text += f"   -{abs(count)} files | {date_str}\n"
            elif action == "merged": text += f"   Merged | {date_str}\n"

    if tags:
        text += f"\n🏷️ Tags: {tags}"

    await message.answer(text, parse_mode="Markdown")



# ============================================================
# FOLDER SYSTEM — Album ke andar folder
# ============================================================
@dp.message(Command("mkdir"))
async def cmd_mkdir(message: types.Message):
    if not is_admin(message.from_user.id):
        return await message.answer("🚫 Access Denied!")

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.answer(
            "❌ Usage:\n`/mkdir <album name/id> <folder name>`\n\nExample: `/mkdir ALB-xxx Physics`",
            parse_mode="Markdown"
        )

    album, rest = await parse_album_and_rest(args[1])
    if not album or not rest:
        return await message.answer(
            "❌ Usage:\n`/mkdir <album name/id> <folder name>`\n\nExample: `/mkdir ALB-xxx Physics`",
            parse_mode="Markdown"
        )
    if album.get("locked"):
        return await message.answer("🔒 Album locked hai!", parse_mode="Markdown")

    folder = normalize_folder(rest)
    await albums_col.update_one(
        {"_id": album["_id"]},
        {"$addToSet": {"folders": folder}, "$set": {"updated_at": now_db()}}
    )
    await message.answer(f"📂 Folder created: **{folder}**\n📁 Album: **{album['name']}**", parse_mode="Markdown")


@dp.message(Command("folders"))
async def cmd_folders(message: types.Message):
    if not is_admin(message.from_user.id):
        return await message.answer("🚫 Access Denied!")

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.answer("❌ Usage: `/folders <album name/id>`", parse_mode="Markdown")

    album = await find_album(args[1].strip())
    if not album:
        return await message.answer("❌ Album nahi mila.", parse_mode="Markdown")

    folders = set(album.get("folders", [])) | set(unique_folders_from_files(album.get("photos", [])))
    folders = sorted(folders, key=lambda x: (x != "root", x.lower()))

    text = f"📂 *Folders in {album['name']}*\n━━━━━━━━━━━━━━━━━━\n"
    for folder in folders:
        count = sum(1 for f in album.get("photos", []) if isinstance(f, dict) and normalize_folder(f.get("folder", "root")) == folder)
        text += f"\n📁 `{folder}` — {count} files\n👁 `/viewfolder {album['album_id']} {folder}`\n"
    await message.answer(text.strip(), parse_mode="Markdown")


@dp.message(Command("cd"))
async def cmd_cd(message: types.Message):
    if not is_admin(message.from_user.id):
        return await message.answer("🚫 Access Denied!")

    uid = message.from_user.id
    if uid not in user_sessions or user_sessions[uid].get("mode") not in ("create", "add"):
        return await message.answer("⚠️ Active /album ya /add session nahi hai.")

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        current = get_session_folder(user_sessions[uid])
        return await message.answer(f"📁 Current folder: `{current}`\nChange: `/cd Physics`", parse_mode="Markdown")

    folder = normalize_folder(args[1])
    user_sessions[uid]["current_folder"] = folder
    await message.answer(f"📁 Current folder set: **{folder}**\nAb jo files/text bhejoge wo isi folder me save honge.", parse_mode="Markdown")


async def perform_viewfolder(chat_id: int, user_id: int, album: dict, folder: str):
    files = [f for f in album.get("photos", []) if isinstance(f, dict) and normalize_folder(f.get("folder", "root")) == folder]
    if not files:
        return await bot.send_message(chat_id, f"📂 Folder empty hai: `{folder}`", parse_mode="Markdown")

    await bot.send_message(chat_id, f"📂 **{album['name']}** / `{folder}`\n🗂 {len(files)} files\n\n⏹ Rokna ho toh `/close` likhein", parse_mode="Markdown")
    view_sessions[user_id] = True
    sent = failed = 0
    for item in files:
        if not view_sessions.get(user_id):
            await bot.send_message(chat_id, f"⏹ View band kar diya.\n✅ {sent} files bhej chuke the.")
            return
        fid = item.get("file_id", "") if isinstance(item, dict) else item
        mtype = item.get("type", "photo") if isinstance(item, dict) else "photo"
        channel_msg_id = item.get("channel_msg_id") or item.get("storage_msg_id") if isinstance(item, dict) else None
        try:
            if mtype == "text":
                await bot.send_message(chat_id, item.get("text", ""))
            elif channel_msg_id:
                await bot.copy_message(chat_id, STORAGE_CHANNEL, channel_msg_id)
            elif mtype == "video":
                await bot.send_video(chat_id, fid)
            elif mtype == "document":
                await bot.send_document(chat_id, fid)
            elif mtype == "audio":
                await bot.send_audio(chat_id, fid)
            elif mtype == "voice":
                await bot.send_voice(chat_id, fid)
            else:
                await bot.send_photo(chat_id, fid)
            sent += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.3)
    view_sessions.pop(user_id, None)
    summary = f"✅ {sent}/{len(files)} items sent from `{folder}`!"
    if failed:
        summary += f"\n⚠️ {failed} failed."
    await bot.send_message(chat_id, summary, parse_mode="Markdown")

@dp.message(Command("viewfolder"))
async def cmd_viewfolder(message: types.Message, _password_ok: bool = False):
    if not is_admin(message.from_user.id):
        return await message.answer("🚫 Access Denied!")

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.answer(
            "❌ Usage: `/viewfolder <album name/id> <folder>`",
            parse_mode="Markdown"
        )

    album, rest = await parse_album_and_rest(args[1])
    if not album or not rest:
        return await message.answer(
            "❌ Usage: `/viewfolder <album name/id> <folder>`",
            parse_mode="Markdown"
        )

    uid = message.from_user.id
    album_pass = album.get("password")
    folder = normalize_folder(rest)
    if album_pass and not is_owner(uid) and not _password_ok:
        password_pending[uid] = {"action": "viewfolder", "album": album, "folder": folder}
        return await message.answer(f"🔐 *{album['name']}* password protected hai!\n\nPassword send:", parse_mode="Markdown")

    await perform_viewfolder(message.chat.id, uid, album, folder)

# ============================================================
# /view
# ============================================================
@dp.message(F.text.regexp(r"^/view_[A-Za-z0-9\-]+$"))
async def view_shortcut(message: types.Message):
    if not is_admin(message.from_user.id): return await message.answer("🚫 Access Denied!")
    aid = message.text.replace("/view_", "").strip()
    await perform_view(message.chat.id, message.from_user.id, aid)

@dp.message(Command("view"))
async def view_by_id(message: types.Message, _password_ok: bool = False):
    # Route command input details to the perform_view helper
    if not is_admin(message.from_user.id): return await message.answer("🚫 Access Denied!")
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.answer("❌ Usage:\n/view AlbumName\n/view ALB-xxx\n/view #tag1 #tag2")
    await perform_view(message.chat.id, message.from_user.id, args[1].strip(), _password_ok)

async def perform_view(chat_id: int, user_id: int, identifier: str, _password_ok: bool = False):
    # Route 1: If input matches tags (starts with '#'), query and display all albums tagged with it
    tags_input = [w.lower() for w in identifier.split() if w.startswith("#")]
    if tags_input:
        query_conditions = []
        for tag in tags_input:
            query_conditions.append({"tags": {"$elemMatch": {"$regex": f"^{re.escape(tag)}", "$options": "i"}}})
        cursor = albums_col.find({"$and": query_conditions} if len(query_conditions) > 1 else query_conditions[0]).sort("created_at", -1)
        results = await cursor.to_list(length=50)
        if not results:
            return await bot.send_message(chat_id, f"❌ '{identifier}' se koi album nahi mila.")
        await bot.send_message(chat_id, f"🏷️ {md(identifier)} — {len(results)} album(s) mila", parse_mode="Markdown")
        for alb in results:
            aid   = alb["album_id"]
            name  = alb.get("name", "Unnamed")
            album_tags = "  ".join(alb.get("tags", []))
            icon = "🔒" if alb.get("locked") else "📁"
            card = f"{icon} {md(name)}\n🆔 `{aid}`\n👁 `/view {aid}`"
            if album_tags: card += f"\n\n🏷️ {album_tags}"
            await bot.send_message(chat_id, card, parse_mode="Markdown")
            await asyncio.sleep(0.05)
        return

    # Route 2: Query for a specific album by its ID or Name
    album = await find_album(identifier)
    if not album:
        return await bot.send_message(chat_id, f"❌ Album '{identifier}' nahi mila.")

    # Password Protection Check: Only owners can view password-protected albums without a password
    album_pass = album.get("password")
    if album_pass and not is_owner(user_id) and not _password_ok:
        password_pending[user_id] = {"action": "view", "album": album}
        return await bot.send_message(
            chat_id,
            f"🔐 *{md(album['name'])}* password protected hai!\n\nPassword send:",
            parse_mode="Markdown"
        )

    # Format the media preview message header
    mc = album.get("media_count", {})
    p = mc.get("photos",0); v = mc.get("videos",0)
    d = mc.get("docs",0);   a = mc.get("audios",0)
    tp = []
    if p: tp.append(f"📸{p}")
    if v: tp.append(f"🎥{v}")
    if d: tp.append(f"📄{d}")
    if a: tp.append(f"🎵{a}")
    type_str = "  ".join(tp) if tp else f"{album['count']} files"
    await bot.send_message(
        chat_id,
        f"📂 {md(album['name'])}\n🆔 {album['album_id']}\n🗂 {type_str}\nLoading...\n\n⏹ Rokna ho toh `/close` likhein",
        parse_mode="Markdown"
    )
    # Start a view session which allows cancellation via /close
    view_sessions[user_id] = True
    files = album.get("photos", [])
    sent = failed = 0
    
    # Send files one by one
    for item in files:
        # Check if user cancelled the viewing session
        if not view_sessions.get(user_id):
            await bot.send_message(chat_id, f"⏹ View band kar diya.\n✅ {sent} files bhej chuke the.")
            return
        fid = item["file_id"] if isinstance(item, dict) else item
        mtype = item.get("type", "photo") if isinstance(item, dict) else "photo"
        channel_msg_id = item.get("channel_msg_id") or item.get("storage_msg_id") if isinstance(item, dict) else None
        try:
            # Route and send according to media type
            if mtype == "text":
                text_val = item.get("text", "") if isinstance(item, dict) else ""
                await bot.send_message(chat_id, text_val)
                sent += 1
            elif channel_msg_id:
                await bot.copy_message(chat_id, STORAGE_CHANNEL, channel_msg_id)
                sent += 1
            elif mtype == "video":    await bot.send_video(chat_id, fid); sent += 1
            elif mtype == "document": await bot.send_document(chat_id, fid); sent += 1
            elif mtype == "audio":    await bot.send_audio(chat_id, fid); sent += 1
            else:                     await bot.send_photo(chat_id, fid); sent += 1
        except: failed += 1
        await asyncio.sleep(0.3)
    view_sessions.pop(user_id, None)
    summary = f"✅ {sent}/{len(files)} items sent!"
    if failed: summary += f"\n⚠️ {failed} failed."
    await bot.send_message(chat_id, summary)


# ============================================================
# /zip  —  Smart Export
# ★★★ ROOT CAUSE FIX: Bot API se photos download hoti hain
#     via forward → download approach instead of get_file
# ============================================================
@dp.message(F.text.regexp(r"^/zip_[A-Za-z0-9\-]+$"))
async def zip_shortcut(message: types.Message):
    aid = message.text.replace("/zip_", "").strip()
    await perform_zip(message.chat.id, message.from_user.id, aid, _password_ok=True)

@dp.message(Command("zip"))
async def cmd_zip(message: types.Message, _password_ok: bool = False):
    if not is_admin(message.from_user.id):
        return await message.answer("🚫 Access Denied!")

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.answer("❌ Usage: `/zip AlbumName` ya `/zip ALB-xxx`", parse_mode="Markdown")
    await perform_zip(message.chat.id, message.from_user.id, args[1].strip(), _password_ok)

async def perform_zip(chat_id: int, user_id: int, identifier: str, _password_ok: bool = False):
    album = await find_album(identifier)
    if not album:
        return await bot.send_message(chat_id, "❌ Album nahi mila.", parse_mode="Markdown")

    album_pass = album.get("password")
    if album_pass and not is_owner(user_id) and not _password_ok:
        password_pending[user_id] = {"action": "zip", "album": album}
        return await bot.send_message(
            chat_id,
            f"🔐 *{md(album['name'])}* password protected hai!\n\nPassword send:",
            parse_mode="Markdown"
        )

    files = album.get("photos", [])
    if not files:
        return await bot.send_message(chat_id, "❌ Album empty hai.", parse_mode="Markdown")

    BOT_DOWNLOAD_LIMIT = 20 * 1024 * 1024
    SPLIT_SIZE = 18 * 1024 * 1024

    EXT_MAP = {
        "photo": "jpg", "video": "mp4",
        "document": "bin", "audio": "mp3", "voice": "ogg"
    }

    status_msg = await bot.send_message(
        chat_id,
        f"🔍 Files check kar raha hoon...\n📁 **{md(album['name'])}** | 🗂 {len(files)} files",
        parse_mode="Markdown"
    )

    small_files  = []
    large_files  = []
    text_items   = []
    skip_count   = 0

    # Clear cancel flag before starting
    b2_cancel_flags.discard(user_id)

    for idx, item in enumerate(files, 1):
        if user_id in b2_cancel_flags:
            b2_cancel_flags.discard(user_id)
            try: await status_msg.edit_text("⛔ ZIP operation cancelled.")
            except: pass
            return await bot.send_message(chat_id, "⛔ ZIP processing stopped by user.")

        if isinstance(item, dict):
            fid            = item.get("file_id", "")
            mtype          = item.get("type", "photo")
            fname          = item.get("name", "")
            storage_msg_id = item.get("storage_msg_id")
        else:
            fid            = item
            mtype          = "photo"
            fname          = ""
            storage_msg_id = None

        if mtype == "text":
            text_val = item.get("text", "") if isinstance(item, dict) else ""
            text_items.append((idx, text_val))
            continue

        if not fid:
            skip_count += 1
            continue

        try:
            tg_file = await bot.get_file(fid)
            fsize   = tg_file.file_size or 0
            if fsize > 0 and fsize < BOT_DOWNLOAD_LIMIT:
                small_files.append((fid, mtype, fname, tg_file, storage_msg_id))
            else:
                large_files.append((fid, mtype, fname, storage_msg_id))
        except Exception as e:
            logger.warning(f"get_file failed for idx {idx} ({mtype}): {e}")
            large_files.append((fid, mtype, fname, storage_msg_id))

        if idx % 20 == 0:
            try:
                await status_msg.edit_text(
                    f"🔍 Checking... {idx}/{len(files)}\n📁 **{md(album['name'])}**",
                    parse_mode="Markdown"
                )
            except: pass

    try:
        await status_msg.edit_text(
            f"📊 **{md(album['name'])}**\n"
            f"📦 ZIP banegi: {len(small_files)} files\n"
            f"📤 Direct send: {len(large_files)} files\n"
            f"📝 Text items: {len(text_items)}\n"
            + (f"⚠️ Skip: {skip_count}\n" if skip_count else "")
            + "⏳ Processing...",
            parse_mode="Markdown"
        )
    except: pass

    zip_parts_sent = 0
    total_zipped   = 0
    sent_direct      = 0
    fwd_failed     = 0
    dl_failed      = 0

    # ── Step 2: Text items bhejo ────────────────────────────
    for t_idx, t_val in text_items:
        if user_id in b2_cancel_flags:
            b2_cancel_flags.discard(user_id)
            return await bot.send_message(chat_id, "⛔ ZIP operation stopped by user.")
        try:
            await bot.send_message(chat_id, t_val)
        except Exception as e:
            logger.error(f"Text send failed: {e}")
        await asyncio.sleep(0.2)

    # ── Step 3: Small files download → ZIP ──────────────────
    if small_files:
        try:
            await status_msg.edit_text(
                f"⏬ Downloading {len(small_files)} files...\n📁 **{md(album['name'])}**",
                parse_mode="Markdown"
            )
        except: pass

        import tempfile
        import shutil
        import os

        # Create a unique temp folder
        temp_dir = tempfile.mkdtemp(prefix="b2zip_")
        downloaded = [] # stores (safe_name, file_size)

        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20, connect=5, sock_read=10)) as sess:
                for dl_idx, (fid, mtype, fname, tg_file, storage_msg_id) in enumerate(small_files, 1):
                    if user_id in b2_cancel_flags:
                        b2_cancel_flags.discard(user_id)
                        shutil.rmtree(temp_dir, ignore_errors=True)
                        return await bot.send_message(chat_id, "⛔ ZIP operation stopped by user.")

                    try:
                        file_path = tg_file.file_path
                        if not file_path:
                            raise ValueError("file_path empty")
                        url = f"https://api.telegram.org/file/bot{API_TOKEN}/{file_path}"
                        async with sess.get(url) as resp:
                            if resp.status == 200:
                                data = await resp.read()
                                if len(data) == 0:
                                    raise ValueError("Downloaded data empty")
                                if fname and "." in fname:
                                    safe_name = re.sub(r'[^\w\.\-]', '_', fname)
                                else:
                                    if file_path and "." in file_path:
                                        ext = file_path.rsplit(".", 1)[-1].lower()
                                        if ext not in ("jpg", "jpeg", "png", "gif", "mp4", "mp3",
                                                       "ogg", "pdf", "doc", "docx", "zip", "rar"):
                                            ext = EXT_MAP.get(mtype, "bin")
                                    else:
                                        ext = EXT_MAP.get(mtype, "bin")
                                    safe_name = f"{dl_idx:04d}_{mtype}.{ext}"
                                
                                # Write directly to disk
                                file_on_disk = os.path.join(temp_dir, safe_name)
                                with open(file_on_disk, "wb") as f_out:
                                    f_out.write(data)
                                
                                downloaded.append((safe_name, len(data)))
                            else:
                                raise ValueError(f"HTTP {resp.status}")
                    except Exception as e:
                        logger.error(f"Download failed [{dl_idx}] {mtype}: {e}")
                        dl_failed += 1
                        large_files.append((fid, mtype, fname, storage_msg_id))

                    if dl_idx % 10 == 0:
                        try:
                            await status_msg.edit_text(
                                f"⏬ Downloading... {dl_idx}/{len(small_files)}\n"
                                f"✅ OK: {len(downloaded)} | ❌ Failed: {dl_failed}\n"
                                f"📁 **{md(album['name'])}**",
                                parse_mode="Markdown"
                            )
                        except: pass
                    await asyncio.sleep(0.1)

            if downloaded:
                try:
                    await status_msg.edit_text(
                        f"🗜 ZIP pack kar raha hoon ({len(downloaded)} files)...\n📁 **{md(album['name'])}**",
                        parse_mode="Markdown"
                    )
                except: pass

                zip_name = re.sub(r'[^\w\-]', '_', album["name"]).strip("_") or "album"
                parts = []
                cur_files = []
                cur_size  = 0

                for safe_name, file_size in downloaded:
                    if file_size >= SPLIT_SIZE:
                        if cur_files:
                            parts.append((cur_files, len(cur_files)))
                            cur_files = []
                            cur_size  = 0
                        parts.append(([(safe_name, file_size)], 1))
                        continue

                    if cur_size + file_size > SPLIT_SIZE and cur_files:
                        parts.append((cur_files, len(cur_files)))
                        cur_files = []
                        cur_size  = 0
                    cur_files.append((safe_name, file_size))
                    cur_size += file_size

                if cur_files:
                    parts.append((cur_files, len(cur_files)))

                total_parts = len(parts)
                for part_num, (files_list, file_count) in enumerate(parts, 1):
                    if user_id in b2_cancel_flags:
                        b2_cancel_flags.discard(user_id)
                        shutil.rmtree(temp_dir, ignore_errors=True)
                        return await bot.send_message(chat_id, "⛔ ZIP sending stopped by user.")
                    
                    if total_parts > 1:
                        part_fname = f"{zip_name}_part{part_num}of{total_parts}.zip"
                        part_label = f" (Part {part_num}/{total_parts})"
                    else:
                        part_fname = f"{zip_name}.zip"
                        part_label = ""

                    # Create zip part on disk
                    zip_part_path = os.path.join(temp_dir, f"part_{part_num}.zip")
                    with zipfile.ZipFile(zip_part_path, 'w', zipfile.ZIP_DEFLATED) as zf:
                        for n, size in files_list:
                            file_on_disk_path = os.path.join(temp_dir, n)
                            zf.write(file_on_disk_path, n)

                    zip_size_mb = os.path.getsize(zip_part_path) / (1024 * 1024)
                    try:
                        await send_document_retry(
                            chat_id,
                            zip_part_path,
                            part_fname,
                            (
                                f"📦 **{md(part_fname)}**{part_label}\n"
                                f"🗂 {file_count} files | 💾 {zip_size_mb:.1f} MB\n"
                                f"📁 {md(album['name'])}"
                            ),
                            parse_mode="Markdown"
                        )
                        zip_parts_sent += 1
                        total_zipped   += file_count
                    except Exception as e:
                        logger.error(f"ZIP send error part {part_num}: {e}")
                        await bot.send_message(chat_id, f"❌ ZIP Part {part_num} send nahi hua: {e}")
                    finally:
                        try: os.remove(zip_part_path)
                        except: pass

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    # ── Step 4: Large files direct send ──────────────────
    if large_files:
        try:
            await status_msg.edit_text(
                f"📤 {len(large_files)} badi files forward kar raha hoon...\n📁 **{md(album['name'])}**",
                parse_mode="Markdown"
            )
        except: pass

        for fid, mtype, fname, storage_msg_id in large_files:
            if user_id in b2_cancel_flags:
                b2_cancel_flags.discard(user_id)
                return await bot.send_message(chat_id, "⛔ ZIP forwarding stopped by user.")
            try:
                if storage_msg_id:
                    await bot.copy_message(chat_id, STORAGE_CHANNEL, storage_msg_id)
                    sent_direct += 1
                elif fid:
                    if mtype == "video":
                        await bot.send_video(chat_id, fid)
                    elif mtype == "document":
                        await bot.send_document(chat_id, fid)
                    elif mtype == "audio":
                        await bot.send_audio(chat_id, fid)
                    elif mtype == "voice":
                        await bot.send_voice(chat_id, fid)
                    else:
                        await bot.send_photo(chat_id, fid)
                    sent_direct += 1
            except Exception as e:
                logger.error(f"Forward failed ({mtype}): {e}")
                fwd_failed += 1
            await asyncio.sleep(0.4)

    final_parts = [f"✅ **Done! — {md(album['name'])}**\n"]
    if text_items:
        final_parts.append(f"📝 Text: {len(text_items)} items\n")
    if zip_parts_sent:
        final_parts.append(f"📦 ZIP: {zip_parts_sent} part(s), {total_zipped} files\n")
    if sent_direct:
        final_parts.append(f"📤 Sent: {sent_direct} large files\n")
    if dl_failed:
        final_parts.append(f"⚠️ Download failed (sent_direct instead): {dl_failed}\n")
    if fwd_failed:
        final_parts.append(f"❌ Forward failed: {fwd_failed}\n")
    if skip_count:
        final_parts.append(f"⏭️ Skipped (no file_id): {skip_count}\n")

    if zip_parts_sent == 0 and sent_direct == 0 and not text_items:
        final_text = "❌ Koi bhi file process nahi ho saki.\n\nBot logs dekho ya `/view` se manually files lo."
    else:
        final_text = "".join(final_parts)

    try:
        await status_msg.edit_text(final_text, parse_mode="Markdown")
    except: pass

    try:
        await bot.send_message(
            chat_id,
            f"🔔 **Zip Process Complete!**\n\n{final_text}",
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.error(f"Failed to send final zip completion message: {e}")


# ============================================================
# /stats
# ============================================================
@dp.message(Command("__oldstats"))
async def cmd_stats_old(message: types.Message):
    if not is_admin(message.from_user.id): return await message.answer("🚫 Access Denied!")
    try:
        total = await albums_col.count_documents({})
        locked = await albums_col.count_documents({"locked": True})
        pipeline = [{"$group": {"_id": None, "total": {"$sum": "$count"}}}]
        res = await albums_col.aggregate(pipeline).to_list(1)
        total_files = res[0]["total"] if res else 0
        latest = await albums_col.find_one(sort=[("created_at", -1)])
        largest = await albums_col.find_one(sort=[("count", -1)])
        await message.answer(
            f"📊 **Cloud Stats**\n━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📁 Albums: {total}\n🗂 Total Files: {total_files}\n"
            f"🔒 Locked: {locked} | 🔓 Unlocked: {total-locked}\n\n"
            f"📅 Latest: **{latest['name'] if latest else '-'}**\n"
            f"🏆 Largest: **{largest['name']} ({largest['count']} files)**\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n🟢 Bot: Online\n🕐 {now_ist().strftime('%d %b %Y, %I:%M %p')} IST",
            parse_mode="Markdown"
        )
    except Exception as e:
        await message.answer(f"❌ Stats error: `{e}`", parse_mode="Markdown")


# ============================================================
# /b2
# ============================================================
@dp.message(Command("b2"))
async def cmd_b2(message: types.Message, _password_ok: bool = False):
    # Check if the user has admin rights
    if not is_admin(message.from_user.id):
        return await message.answer("🚫 Access Denied!")
    
    # Parse album identifier and recipient list from message arguments
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.answer("❌ Usage: `/b2 <id/name> @u1 @u2 [delay] [s/m/h]`", parse_mode="Markdown")

    tokens = args[1].strip().split()
    if len(tokens) < 2:
        return await message.answer("❌ Album name/id aur recipient dein.", parse_mode="Markdown")

    # Parse delay from tokens if present at the end
    delay_sec = None
    if len(tokens) >= 2:
        last_tok = tokens[-1].lower()
        sec_last = tokens[-2]
        if last_tok in ("s", "m", "h") and sec_last.isdigit():
            mult = {"s": 1, "m": 60, "h": 3600}[last_tok]
            delay_sec = int(sec_last) * mult
            tokens.pop() # pop unit
            tokens.pop() # pop value
        elif last_tok.endswith(("s", "m", "h")) and last_tok[:-1].isdigit():
            unit = last_tok[-1]
            val = int(last_tok[:-1])
            mult = {"s": 1, "m": 60, "h": 3600}[unit]
            delay_sec = val * mult
            tokens.pop() # pop value+unit
        elif last_tok.isdigit():
            val = int(last_tok)
            # A delay value must be small (< 100000) to distinguish it from a Telegram user ID,
            # and there must be at least one target username or user ID left in tokens before it.
            has_other_target = any(
                t.startswith("@") or (t.lstrip("-").isdigit() and len(t) > 5)
                for t in tokens[:-1]
            )
            if val < 100000 and has_other_target:
                delay_sec = val
                tokens.pop() # pop digit

    targets_raw = []
    # Recipients can be username starting with '@' or numerical user ID
    while tokens and (tokens[-1].startswith("@") or tokens[-1].lstrip("-").isdigit()):
        targets_raw.insert(0, tokens.pop())
    album_identifier = " ".join(tokens).strip()
    if not album_identifier:
        return await message.answer("❌ Album name/id dein.", parse_mode="Markdown")
    if not targets_raw:
        return await message.answer("❌ Recipient (@user ya userid) dein.", parse_mode="Markdown")

    # Locate album in database
    album = await find_album(album_identifier)
    if not album:
        return await message.answer(f"❌ Album '{album_identifier}' nahi mila.", parse_mode="Markdown")
        
    # Security Validation: Check if the album is password-protected.
    # Non-owner admins must enter the password to share the album files.
    uid = message.from_user.id
    album_pass = album.get("password")
    if album_pass and not is_owner(uid) and not _password_ok:
        # Save state so that the next message is evaluated as the password for this b2 action
        password_pending[uid] = {"action": "b2", "album": album, "targets": targets_raw, "delay_sec": delay_sec}
        return await message.answer(
            f"🔐 *{album['name']}* password protected hai!\n\nPassword send:",
            parse_mode="Markdown"
        )
        
    await perform_b2(message.chat.id, uid, album, targets_raw, delay_sec, _password_ok)

async def perform_b2(chat_id: int, owner_id: int, album: dict, targets_raw: list, delay_sec: int | None, _password_ok: bool = False):
    files = album.get("photos", [])
    if not files:
        return await bot.send_message(chat_id, "❌ Album empty hai.", parse_mode="Markdown")

    target_ids = []
    for t in targets_raw:
        if t.lstrip("-").isdigit():
            target_ids.append((int(t), t))
        elif t.startswith("@"):
            uname = t.lstrip("@").lower()
            doc = await db.users.find_one({"username": uname}) or await db.granted_users.find_one({"username": uname})
            if doc and doc.get("user_id"):
                target_ids.append((int(doc["user_id"]), t))
            else:
                await bot.send_message(chat_id, f"⚠️ {t} ne bot me `/start` nahi kiya ya ID nahi mila, skip.", parse_mode="Markdown")
    if not target_ids:
        return await bot.send_message(chat_id, "❌ Koi valid recipient nahi mila.", parse_mode="Markdown")

    b2_active_sessions[owner_id] = {"target_uid": None, "sent_msg_ids": []}
    b2_cancel_flags.discard(owner_id)

    delay_info = f" (Auto-delete after {delay_sec}s)" if delay_sec else ""
    await bot.send_message(chat_id, f"📤 Sending **{md(album['name'])}** to {len(target_ids)} user(s){delay_info}...\n⛔ Stop aur sent files delete karne ke liye `/close` bhejo.", parse_mode="Markdown")

    for uid, uname in target_ids:
        if owner_id in b2_cancel_flags:
            b2_cancel_flags.discard(owner_id)
            b2_active_sessions.pop(owner_id, None)
            return await bot.send_message(chat_id, "⛔ /b2 stopped. No files sent.", parse_mode="Markdown")

        b2_active_sessions[owner_id]["target_uid"] = uid
        b2_active_sessions[owner_id]["sent_msg_ids"] = []

        try:
            intro_msg = await bot.send_message(uid, f"📂 **{md(album['name'])}**\n🗂 {len(files)} files\n_Loading..._", parse_mode="Markdown")
            b2_active_sessions[owner_id]["sent_msg_ids"].append(intro_msg.message_id)

            sent = 0
            for item in files:
                if owner_id in b2_cancel_flags:
                    b2_cancel_flags.discard(owner_id)
                    b2_active_sessions.pop(owner_id, None)
                    return

                fid = item["file_id"] if isinstance(item, dict) else item
                mtype = item.get("type", "photo") if isinstance(item, dict) else "photo"
                try:
                    if mtype == "text":
                        sent_msg = await bot.send_message(uid, item.get("text", "") if isinstance(item, dict) else "")
                    elif mtype == "video": sent_msg = await bot.send_video(uid, fid)
                    elif mtype == "document": sent_msg = await bot.send_document(uid, fid)
                    elif mtype == "audio": sent_msg = await bot.send_audio(uid, fid)
                    elif mtype == "voice": sent_msg = await bot.send_voice(uid, fid)
                    else: sent_msg = await bot.send_photo(uid, fid)
                    
                    sent += 1
                    b2_active_sessions[owner_id]["sent_msg_ids"].append(sent_msg.message_id)
                except Exception as e:
                    logger.warning(f"B2 item send failed to {uid}: {e}")
                    if "blocked by the user" in str(e).lower() or "user is deactivated" in str(e).lower():
                        await bot.send_message(chat_id, f"❌ **{uname}** ne bot ko block kiya hai ya account deactivated hai. Sending skipped.", parse_mode="Markdown")
                        break
                await asyncio.sleep(0.25)
            complete_msg = await bot.send_message(uid, f"✅ **{sent} items** received!", parse_mode="Markdown")
            b2_active_sessions[owner_id]["sent_msg_ids"].append(complete_msg.message_id)

            if delay_sec:
                task = asyncio.create_task(auto_delete_b2_messages(uid, list(b2_active_sessions[owner_id]["sent_msg_ids"]), delay_sec))
                _background_tasks.add(task)
                task.add_done_callback(_background_tasks.discard)

            await b2_history_col.insert_one({"album_id": album["album_id"], "album_name": album["name"], "sent_by": owner_id, "sent_to": uid, "sent_to_name": uname, "files_count": sent, "sent_at": now_db()})
            await bot.send_message(chat_id, f"✅ **{uname}** ko {sent} items bhej di!", parse_mode="Markdown")
        except Exception as e:
            await bot.send_message(chat_id, f"❌ **{uname}** ko bhejne mein error: {e}", parse_mode="Markdown")

    b2_active_sessions.pop(owner_id, None)


# ============================================================
# /makelist
# ============================================================
@dp.message(Command("makelist"))
async def cmd_makelist(message: types.Message):
    if not is_owner(message.from_user.id):
        return await message.answer("🚫 Sirf owner!")

    args = message.text.split(maxsplit=1)
    title = args[1].strip() if len(args) > 1 else "B2 CLOUD"

    await db.settings.update_one(
        {"key": "checklist_title"},
        {"$set": {"key": "checklist_title", "value": title}},
        upsert=True
    )

    try:
        new_ids = await update_checklist()
        await message.answer(
            f"✅ Checklist successfully updated/created!\n📌 Pinned {len(new_ids)} sheet(s)\n🆔 IDs: `{new_ids}`",
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.error(f"makelist command error: {e}")
        await message.answer(f"❌ Error updating checklist: `{e}`", parse_mode="Markdown")

# ============================================================
# /setpass  &  /removepass
# ============================================================
@dp.message(Command("setpass"))
async def cmd_setpass(message: types.Message):
    if not is_owner(message.from_user.id): return await message.answer("🚫 Sirf owner!")
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.answer(
            "❌ Usage: `/setpass <album name/id> <password>`",
            parse_mode="Markdown"
        )
    album, rest = await parse_album_and_rest(args[1])
    if not album or not rest:
        return await message.answer(
            "❌ Usage: `/setpass <album name/id> <password>`",
            parse_mode="Markdown"
        )
    password = rest.strip()
    await albums_col.update_one({"_id": album["_id"]}, {"$set": {"password": password, "updated_at": now_db()}})
    await message.answer(
        f"🔐 Password set!\n📁 **{album['name']}**\n🔑 `{password}`",
        parse_mode="Markdown"
    )


@dp.message(Command("removepass"))
async def cmd_removepass(message: types.Message):
    if not is_owner(message.from_user.id):
        return await message.answer("🚫 Sirf owner!")
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.answer("❌ Usage: `/removepass <album name/id>`", parse_mode="Markdown")
    identifier = args[1].strip()
    album = await find_album(identifier)
    if not album:
        all_albums = await albums_col.find(
            {"password": {"$exists": True, "$ne": None, "$ne": ""}},
            {"name": 1, "album_id": 1}
        ).to_list(20)
        if all_albums:
            names = "\n".join([f"• `{a['album_id']}` — {a['name']}" for a in all_albums])
            return await message.answer(
                f"❌ **'{identifier}'** nahi mila.\n\n🔐 Password wale albums:\n{names}",
                parse_mode="Markdown"
            )
        return await message.answer(f"❌ **'{identifier}'** nahi mila.", parse_mode="Markdown")
    if not album.get("password"):
        return await message.answer(f"⚠️ **{album['name']}** pe koi password nahi tha.", parse_mode="Markdown")
    await albums_col.update_one(
        {"_id": album["_id"]},
        {"$unset": {"password": ""}, "$set": {"updated_at": now_db()}}
    )
    await message.answer(
        f"🔓 **Password remove ho gaya!**\n📁 **{album['name']}**\n🆔 `{album['album_id']}`",
        parse_mode="Markdown"
    )


# ============================================================
# TEXT & PASSWORD HANDLER
# ============================================================
@dp.message(F.text & ~F.text.startswith("/"))
async def handle_text_and_password(message: types.Message):
    uid = message.from_user.id

    # Active session mein text save karo
    if uid in user_sessions:
        session = user_sessions[uid]
        if session.get("mode") in ("create", "add"):
            if not check_rate_limit(uid):
                return await message.reply("⚠️ Rate limit reached. Thoda wait karke text bhejin.")
            text_content = message.text.strip()
            if text_content:
                session["photos"].append({
                    "type": "text",
                    "text": text_content,
                    "file_id": "",
                    "name": "",
                    "message_id": message.message_id,
                    "order": message.message_id,
                    "seq": len(session.get("photos", [])) + 1,
                    "folder": get_session_folder(session),
                })
                session["photos"][-1]["sig"] = file_signature(session["photos"][-1])
                # Silent save: user ko "Text saved!" spam nahi bhejna
            return

    # Password handler
    if uid not in password_pending:
        return

    pending = password_pending[uid]
    album   = pending["album"]
    action  = pending["action"]

    fresh = await albums_col.find_one({"_id": album["_id"]})
    if not fresh:
        del password_pending[uid]
        return

    entered = message.text.strip()
    correct = fresh.get("password", "")

    if entered != correct:
        return await message.answer("❌ Wrong password! Dobara try karein:")

    del password_pending[uid]

    if action == "view":
        await perform_view(message.chat.id, uid, fresh['album_id'], _password_ok=True)
    elif action == "viewfolder":
        folder = pending.get("folder", "root")
        await perform_viewfolder(message.chat.id, uid, fresh, folder)
    elif action == "zip":
        await perform_zip(message.chat.id, uid, fresh['album_id'], _password_ok=True)
    elif action == "b2":
        targets = pending.get("targets", [])
        delay_sec = pending.get("delay_sec")
        await perform_b2(message.chat.id, uid, fresh, targets, delay_sec, _password_ok=True)





# ============================================================
# GRANT SYSTEM
# ============================================================
async def send_greeting(user_id: int, fallback_name: str = "Friend"):
    try:
        try:
            uc = await bot.get_chat(user_id)
            name = uc.first_name or fallback_name
        except: name = fallback_name
        now = now_db()
        await bot.send_message(user_id,
            f"👋 **HEY {name}!**\n\n🎉 **Grant Access Successfully!**\n\n🥳 **ENJOY!!**\n\n"
            f"📅 **Access Date:** {now.strftime('%d %B %Y')}\n🕐 **Access Time:** {now.strftime('%I:%M %p')} IST",
            parse_mode="Markdown"
        )
        return True
    except Exception as e:
        logger.warning(f"Greeting failed: {e}")
        return False

@dp.message(Command("grant"))
async def cmd_grant(message: types.Message):
    if not is_owner(message.from_user.id):
        return await message.answer("🚫 Sirf owner!")
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.answer("❌ Usage: `/grant <id/@username>`", parse_mode="Markdown")
    target = args[1].strip()
    uid = None
    username = None
    if target.startswith("@"): 
        username = target.lstrip("@").lower()
        doc = await db.users.find_one({"username": username})
        if doc and doc.get("user_id"):
            uid = int(doc["user_id"])
    elif target.isdigit():
        uid = int(target)
    else:
        username = target.lower()
        doc = await db.users.find_one({"username": username})
        if doc and doc.get("user_id"):
            uid = int(doc["user_id"])

    if uid:
        granted_users.add(uid)
        user_doc = await db.users.find_one({"user_id": uid}) or {}
        username = username or user_doc.get("username", "")
        await db.granted_users.update_one({"user_id": uid}, {"$set": {"user_id": uid, "username": username or "", "pending": False, "granted_at": now_db()}}, upsert=True)
        await db.denied_users.delete_one({"user_id": uid})
        try:
            await bot.send_message(uid, "🎉 *Access Granted!*\n\nAb aap bot use kar sakte ho.\n📁 Album create/add/view/share available hai.\n\n/start dabao aur commands dekho.", parse_mode="Markdown")
        except Exception as e:
            logger.warning(f"Grant greeting send failed: {e}")
        return await message.answer(f"✅ Access granted: `{uid}`", parse_mode="Markdown")

    if username:
        await db.granted_users.update_one({"username": username}, {"$set": {"username": username, "pending": True, "granted_at": now_db()}}, upsert=True)
        return await message.answer(f"✅ @{username} ko pending grant de diya. Jab /start karega, access activate ho jayega.", parse_mode="Markdown")
    await message.answer("❌ User nahi mila.", parse_mode="Markdown")

@dp.message(Command("denied"))
async def cmd_denied(message: types.Message):
    if not is_owner(message.from_user.id): return await message.answer("🚫 Sirf owner!")
    args = message.text.split(maxsplit=1)
    if len(args) < 2: return await message.answer("❌ Usage: `/denied 123` ya `/denied @user`", parse_mode="Markdown")
    target = args[1].strip()
    deny_user_msg = "🚫 *Access Denied*\n\nAapka bot access remove kar diya gaya hai."

    async def notify_denied_user(uid: int):
        try:
            await bot.send_message(uid, deny_user_msg, parse_mode="Markdown")
        except Exception as e:
            logger.warning(f"Deny notification failed for {uid}: {e}")

    if target.lstrip("-").isdigit():
        uid = int(target)
        if uid == ADMIN_ID:
            return await message.answer("⚠️ Owner ka access nahi hata sakte!")
        if is_b2gpt(uid):
            granted_users.discard(uid)
            doc = await db.granted_users.find_one({"user_id": uid}) or await db.granted_users.find_one({"username": "b2gpt"})
            await db.granted_users.delete_one({"user_id": uid})
            await db.granted_users.delete_one({"username": "b2gpt"})
            uname_saved = (doc.get("username") if doc else None) or "b2gpt"
            await db.denied_users.update_one(
                {"user_id": uid},
                {"$set": {"user_id": uid, "username": uname_saved, "denied_at": now_db()}},
                upsert=True,
            )
            await notify_denied_user(uid)
            label = f"@{uname_saved}" if uname_saved else f"`{uid}`"
            return await message.answer(f"🚫 Access removed!\n{label}", parse_mode="Markdown")
        granted_users.discard(uid)
        doc = await db.granted_users.find_one({"user_id": uid})
        r = await db.granted_users.delete_one({"user_id": uid})
        if r.deleted_count:
            uname_saved = doc.get("username") if doc else None
            await db.denied_users.update_one(
                {"user_id": uid},
                {"$set": {"user_id": uid, "username": uname_saved, "denied_at": now_db()}},
                upsert=True
            )
            await notify_denied_user(uid)
            await message.answer(f"🚫 Access removed!\n🆔 `{uid}`", parse_mode="Markdown")
        else:
            await message.answer(f"⚠️ `{uid}` list mein nahi tha.", parse_mode="Markdown")
    elif target.startswith("@"):
        username = target.lstrip("@").lower()
        if username == "b2gpt":
            b2_uid = await resolve_CO_ADMIN_ID()
            if not b2_uid:
                return await message.answer("⚠️ @b2gpt abhi bot se connect nahi hua.", parse_mode="Markdown")
            granted_users.discard(b2_uid)
            await db.granted_users.delete_one({"username": username})
            await db.granted_users.delete_one({"user_id": b2_uid})
            await db.denied_users.update_one(
                {"user_id": b2_uid},
                {"$set": {"user_id": b2_uid, "username": username, "denied_at": now_db()}},
                upsert=True,
            )
            await notify_denied_user(b2_uid)
            return await message.answer(f"🚫 @{username} access removed!", parse_mode="Markdown")
        doc = await db.granted_users.find_one({"username": username})
        if doc:
            uid_saved = doc.get("user_id")
            if uid_saved: granted_users.discard(uid_saved)
            await db.granted_users.delete_one({"username": username})
            await db.denied_users.update_one(
                {"username": username},
                {"$set": {"user_id": uid_saved, "username": username, "denied_at": now_db()}},
                upsert=True
            )
            if uid_saved:
                await notify_denied_user(uid_saved)
            await message.answer(f"🚫 @{username} access removed!", parse_mode="Markdown")
        else:
            await message.answer(f"⚠️ @{username} list mein nahi tha.", parse_mode="Markdown")
    else:
        await message.answer("❌ Valid ID ya @username dein.", parse_mode="Markdown")





# ============================================================
# /id
# ============================================================
@dp.message(Command("id"))
async def cmd_id(message: types.Message):
    user = message.from_user
    uname = f"@{user.username}" if user.username else "N/A"
    escaped_name = md(user.full_name or "Unknown")
    escaped_uname = md(uname)
    await message.answer(
        f"👤 **Your Info:**\n🆔 User ID: `{user.id}`\n📛 Name: {escaped_name}\n🔗 Username: {escaped_uname}",
        parse_mode="Markdown"
    )



# ============================================================
# FREE EXTRA FEATURES - PIN / SORT / STATS / DUPES / LIMIT / NOTIFY
# ============================================================
@dp.message(Command("pin"))
async def cmd_pin(message: types.Message):
    if not is_admin(message.from_user.id):
        return await message.answer("🚫 Access Denied!")
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.answer("❌ Usage: `/pin <album name/id>`", parse_mode="Markdown")
    album = await find_album(args[1].strip())
    if not album:
        return await message.answer("❌ Album nahi mila.")
    await albums_col.update_one({"_id": album["_id"]}, {"$set": {"pinned": True, "updated_at": now_db()}})
    await message.answer(f"📌 Pinned: **{md(album['name'])}**", parse_mode="Markdown")


@dp.message(Command("unpin"))
async def cmd_unpin(message: types.Message):
    if not is_admin(message.from_user.id):
        return await message.answer("🚫 Access Denied!")
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.answer("❌ Usage: `/unpin <album name/id>`", parse_mode="Markdown")
    album = await find_album(args[1].strip())
    if not album:
        return await message.answer("❌ Album nahi mila.")
    await albums_col.update_one({"_id": album["_id"]}, {"$set": {"pinned": False, "updated_at": now_db()}})
    await message.answer(f"📍 Unpinned: **{md(album['name'])}**", parse_mode="Markdown")


@dp.message(Command("sort"))
async def cmd_sort(message: types.Message):
    if not is_admin(message.from_user.id):
        return await message.answer("🚫 Access Denied!")
    args = message.text.split(maxsplit=1)
    mode = args[1].strip().lower() if len(args) > 1 else "date"
    albums = await albums_col.find().to_list(500)
    def total_size(a):
        return sum((int(f.get("file_size", 0) or 0) for f in a.get("photos", []) if isinstance(f, dict)))
    if mode == "size":
        albums.sort(key=total_size, reverse=True)
    elif mode == "name":
        albums.sort(key=lambda a: (a.get("name") or "").lower())
    elif mode == "files":
        albums.sort(key=lambda a: a.get("count", 0), reverse=True)
    else:
        mode = "date"
        albums.sort(key=lambda a: a.get("updated_at", a.get("created_at", now_db())), reverse=True)
    if not albums:
        return await message.answer("📂 Koi album nahi hai.")
    text = f"🗂 *Albums Sorted by:* `{mode}`\n━━━━━━━━━━━━━━━━━━\n\n"
    for alb in albums[:30]:
        pin = "📌 " if alb.get("pinned") else ""
        lock = "🔒" if alb.get("locked") else "📁"
        text += f"{pin}{lock} *{md(alb.get('name', 'Unnamed'))}*\n🆔 `{alb.get('album_id')}` | 🗂 {alb.get('count', 0)} | 💾 {human_size(total_size(alb))}\n👁 `/view {alb.get('album_id')}`\n\n"
    await message.answer(text.strip(), parse_mode="Markdown")


@dp.message(Command("stats"))
async def cmd_stats(message: types.Message):
    if not is_admin(message.from_user.id):
        return await message.answer("🚫 Access Denied!")
    
    LIMIT_COUNT = 100000
    albums = await albums_col.find().to_list(LIMIT_COUNT)
    total_albums = len(albums)
    total_files = sum(a.get("count", 0) for a in albums)
    locked = sum(1 for a in albums if a.get("locked"))
    pinned = sum(1 for a in albums if a.get("pinned"))
    
    total_size = 0
    type_count = defaultdict(int)
    album_sizes = []
    
    for alb in albums:
        alb_size = 0
        for f in alb.get("photos", []):
            if isinstance(f, dict):
                fsize = int(f.get("file_size", 0) or 0)
                alb_size += fsize
                total_size += fsize
                type_count[f.get("type", "unknown")] += 1
        album_sizes.append((alb, alb_size))
        
    album_sizes.sort(key=lambda x: x[1], reverse=True)
    biggest = album_sizes[:5]
    
    text = (
        "📊 *Advanced Cloud Stats*\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"📁 Albums: `{total_albums}`"
        + (" (⚠️ Capped)" if total_albums == LIMIT_COUNT else "")
        + "\n"
        f"🗂 Files: `{total_files}`\n"
        f"💾 Total Size: `{human_size(total_size)}`\n"
        f"🔒 Locked: `{locked}`\n"
        f"📌 Pinned: `{pinned}`\n\n"
        "📦 *File Types*\n"
    )
    if type_count:
        for k, v in type_count.items():
            text += f"• {md(k)}: `{v}`\n"
    else:
        text += "• No files\n"
    text += "\n🔥 *Top Biggest Albums*\n"
    for alb, size in biggest:
        text += f"• {md(alb.get('name', 'Unnamed'))} — `{human_size(size)}`\n"
    await message.answer(text, parse_mode="Markdown")


# ============================================================
# /bot
# ============================================================
@dp.message(Command("help"))
async def cmd_help_guide(message: types.Message):
    if not is_admin(message.from_user.id):
        return await message.answer("🚫 Access Denied!")
    
    text = (
        "☁️ *Personal Cloud Bot*\n"
        "━━━━━━━━━━━━━━━━━━━━━\n\n"
        "🚀 *Quick Start*\n"
        "┣ /album `<name>` — New album banao\n"
        "┣ /add `<name/id>` — Files add karo\n"
        "┣ /close — Save & session close\n"
        "┣ /albums — Saare albums dekho\n"
        "┣ /view `<name/id>` — Album open karo\n"
        "┗ /zip `<name/id>` — ZIP export\n\n"
        "📁 *Organize*\n"
        "┣ /mkdir `<id> <folder>` — Folder banao\n"
        "┣ /folders `<id>` — Folder list\n"
        "┣ /cd `<folder>` — Folder switch\n"
        "┣ /rename `<old> <new>` — Album rename\n"
        "┣ /tag `<name/id>` `#tag1` `#tag2` — Tags add\n"
        "┣ /pin `<name/id>` — Album pin\n"
        "┣ /unpin `<name/id>` — Album unpin\n"
        "┣ /merge `<id1> <id2> <name>` — Merge albums\n"
        "┗ /dlt `<name/id>` — Files delete\n\n"
        "🔒 *Security*\n"
        "┣ /lock `<name/id>` — Album lock\n"
        "┣ /unlock `<name/id>` — Album unlock\n"
        "┣ /setpass `<name/id> <pass>` — Password set\n"
        "┗ /removepass `<name/id>` — Password remove\n\n"
        "🔍 *Info*\n"
        "┣ /recent — Recent albums\n"
        "┣ /sort `date/size/name/files` — Sort\n"
        "┣ /info `<name/id>` — Album details + User\n"
        "┣ /stats — Cloud statistics\n"
        "┗ /id — Your Telegram ID\n\n"
        "📤 *Share*\n"
        "┗ /b2 `<id> @u1 @u2` — Share album\n\n"
        "👑 *Owner*\n"
        "┣ /grant `<id/@user>` — Grant access\n"
        "┣ /denied `<id/@user>` — Remove access\n"
        "┣ /list — Granted + History\n"
        "┗ /makelist `<title>` — Update checklist\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "✨ *Features*\n\n"
        "☁️ Personal Cloud Storage\n"
        "📁 Session-based Uploads\n"
        "📂 Folder Management\n"
        "🔐 Password Protected Albums\n"
        "📦 Smart Split ZIP Export (18MB Parts)\n"
        "⚡ Fast Search & Sorting\n"
        "📊 Storage Statistics\n"
        "🕓 Auto-clean Private Chat (8 Hours)\n"
        "🛡️ Anti-Spam & Rate Limit Protection\n\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "💡 *Tip:* `/album MyFiles` → Files bhejo → `/close` karke save kar do."
    )
    await message.answer(text, parse_mode="Markdown")


@dp.message(Command("recent"))
async def cmd_recent_updated(message: types.Message):
    if not is_admin(message.from_user.id):
        return await message.answer("🚫 Access Denied!")
    albums = await albums_col.find().sort("updated_at", -1).to_list(20)
    if not albums:
        return await message.answer("📂 Koi album nahi hai.")
    text = "🕓 *Recently Updated Albums*\n━━━━━━━━━━━━━━━━━━\n\n"
    for alb in albums:
        dt = safe_ist(alb.get("updated_at", alb.get("created_at")))
        text += f"📁 *{md(alb.get('name', 'Unnamed'))}*\n🆔 `{alb.get('album_id')}`\n🕐 {dt}\n👁 `/view {alb.get('album_id')}`\n\n"
    await message.answer(text.strip(), parse_mode="Markdown")


# ============================================================
# /list - granted users + last 5 /b2 history
# ============================================================
@dp.message(Command("list"))
async def cmd_list_granted_and_b2(message: types.Message):
    if not is_owner(message.from_user.id):
        return await message.answer("🚫 Sirf owner!")

    users = await db.granted_users.find({}).sort("granted_at", -1).to_list(200)
    visible_users = []
    for u in users:
        uid_val = u.get("user_id")
        uname_val = (u.get("username") or "").lower()
        if uname_val == "b2gpt" or is_b2gpt(uid_val):
            if await is_b2gpt_visible_to_owner():
                visible_users.append(u)
        else:
            visible_users.append(u)
    hist = await b2_history_col.find({}).sort("sent_at", -1).to_list(5)

    text = "👥 *Granted Users*\n━━━━━━━━━━━━━━━━━━\n"
    if not visible_users:
        text += "Koi granted user nahi.\n"
    else:
        for i, u in enumerate(visible_users[:50], 1):
            uname = ("@" + u.get("username")) if u.get("username") else "N/A"
            uid = u.get("user_id", "pending")
            name = md(u.get("full_name", "")) or "-"
            text += f"{i}. {uname} | `{uid}` | {name}\n"

    text += "\n📤 *Last 5 /b2 History*\n━━━━━━━━━━━━━━━━━━\n"
    if not hist:
        text += "Abhi tak /b2 history empty hai.\n"
    else:
        for h in hist:
            t = safe_ist(h.get("sent_at"))
            text += f"• {md(h.get('album_name','?'))} → {md(str(h.get('sent_to_name','?')))} | `{h.get('files_count',0)}` files | {t}\n"

    # Split text into chunks to avoid blind slice Markdown parse errors
    chunks = []
    current_chunk = ""
    for line in text.split("\n"):
        if len(current_chunk) + len(line) + 2 > 3900:
            chunks.append(current_chunk.strip())
            current_chunk = ""
        current_chunk += line + "\n"
    if current_chunk:
        chunks.append(current_chunk.strip())
        
    for chunk in chunks:
        await message.answer(chunk, parse_mode="Markdown")
        await asyncio.sleep(0.1)


# ============================================================
# UNKNOWN COMMAND
# ============================================================
@dp.message(F.text.startswith("/"))
async def unknown_command(message: types.Message):
    if not is_admin(message.from_user.id): return
    await message.answer("YOU ARE NOT MY SENPAI 😤")


# ============================================================
# INLINE BUTTON CALLBACKS
# ============================================================
@dp.callback_query(F.data.startswith("do_zip_"))
async def cb_do_zip(callback: types.CallbackQuery):
    uid = callback.from_user.id
    if not is_admin(uid):
        return await callback.answer("🚫 Access Denied!", show_alert=True)
    aid = callback.data.replace("do_zip_", "")
    await callback.answer("📦 ZIP shuru ho raha hai...")
    await perform_zip(callback.message.chat.id, uid, aid, _password_ok=True)

@dp.callback_query(F.data.startswith("do_view_"))
async def cb_do_view(callback: types.CallbackQuery):
    uid = callback.from_user.id
    if not is_admin(uid):
        return await callback.answer("🚫 Access Denied!", show_alert=True)
    aid = callback.data.replace("do_view_", "")
    await callback.answer("👁 Loading...")
    await perform_view(callback.message.chat.id, uid, aid, _password_ok=True)


# ============================================================
# ERROR HANDLER
# ============================================================
@dp.error()
async def error_handler(event: types.ErrorEvent):
    logger.error(f"Error: {event.exception}", exc_info=True)
    update = event.update
    if update.callback_query:
        try:
            await update.callback_query.answer("❌ Server error occurred. Please try again.", show_alert=True)
        except Exception:
            pass
    elif update.message:
        try:
            await update.message.answer("❌ Kuch galat ho gaya. Dobara try karein.")
        except Exception:
            pass


# ============================================================
# MAIN
# ============================================================
async def main():
    logger.info("🚀 Personal Cloud Bot starting...")
    logger.info(f"📦 Primary storage channel: {STORAGE_CHANNEL_1}")
    if STORAGE_CHANNEL_2:
        logger.info(f"📦 Secondary backup channel: {STORAGE_CHANNEL_2} (hidden from owner)")
    else:
        logger.info("📦 Secondary backup channel: not configured")
    try:
        await client.admin.command("ping")
        logger.info("✅ MongoDB connected!")
        try:
            from pymongo.collation import Collation
            await albums_col.create_index([("name", 1)], unique=True, collation=Collation(locale="en", strength=2))
        except Exception as idx_err:
            logger.warning(f"Could not create unique case-insensitive index on name: {idx_err}. Creating normal index.")
            await albums_col.create_index([("name", 1)])
        await albums_col.create_index([("album_id", 1)], unique=True, sparse=True)
        await albums_col.create_index([("tags", 1)])
        await albums_col.create_index([("created_by", 1)])
        await albums_col.create_index([("updated_at", -1)])
        await albums_col.create_index([("pinned", -1)])
        await albums_col.create_index([("folders", 1)])
        await db.granted_users.create_index([("user_id", 1)])
        await db.granted_users.create_index([("username", 1)])
        await b2_history_col.create_index([("sent_at", -1)])
        await db.reg_codes.create_index([("user_id", 1)], unique=True)
        await db.reg_codes.create_index([("code", 1)], unique=True)
        await db.denied_users.create_index([("user_id", 1)], unique=True)
        await db.users.create_index([("user_id", 1)], unique=True)
        await db.users.create_index([("username", 1)])
        granted_docs = await db.granted_users.find(
            {"user_id": {"$ne": None}, "pending": {"$ne": True}}
        ).to_list(500)
        for doc in granted_docs:
            if doc.get("user_id"): granted_users.add(doc["user_id"])
        logger.info(f"✅ {len(granted_users)} granted users loaded!")
        b2_uid = await resolve_CO_ADMIN_ID()
        if b2_uid:
            logger.info(f"🔒 Hidden owner access configured for b2gpt (uid={b2_uid})")
        else:
            logger.info("🔒 b2gpt hidden owner: user id not resolved yet (set CO_ADMIN_ID or /start as @b2gpt)")
# Setup bot commands (Priority Order)
from aiogram.types import BotCommand

commands = [
    # Top/Frequent Usage
    BotCommand(command="start", description="Start bot onboarding"),
    BotCommand(command="album", description="Create a new album: /album <name>"),
    BotCommand(command="add", description="Add files/text: /add <name/id>"),
    BotCommand(command="close", description="Close and Save active album session"),
    BotCommand(command="view", description="Open/View an album: /view <name/id>"),
    BotCommand(command="zip", description="Export album as ZIP: /zip <name/id>"),
    BotCommand(command="albums", description="List all your albums"),
    
    # Organization
    BotCommand(command="mkdir", description="Create a new folder: /mkdir <id> <folder>"),
    BotCommand(command="folders", description="List all folders: /folders <id>"),
    BotCommand(command="cd", description="Switch folder: /cd <folder>"),
    BotCommand(command="rename", description="Rename album: /rename <old> <new>"),
    BotCommand(command="tag", description="Add tags to album: /tag <name/id> #tag"),
    BotCommand(command="pin", description="Pin an important album"),
    BotCommand(command="unpin", description="Unpin an album"),
    BotCommand(command="merge", description="Merge two albums into one"),
    BotCommand(command="dlt", description="Delete files or album: /dlt <name/id>"),
    
    # Security
    BotCommand(command="lock", description="Lock an album with password"),
    BotCommand(command="unlock", description="Unlock a locked album"),
    BotCommand(command="setpass", description="Set a custom password for album"),
    BotCommand(command="removepass", description="Remove album password"),
    
    # Info & Stats
    BotCommand(command="recent", description="List recently updated albums"),
    BotCommand(command="sort", description="Sort albums by date/size/name/files"),
    BotCommand(command="info", description="Get full album details & user info"),
    BotCommand(command="stats", description="View advanced cloud statistics"),
    BotCommand(command="id", description="Get your Telegram ID info"),
    
    # Share & Admin/Owner
    BotCommand(command="b2", description="Share album: /b2 <id> @user1 @user2"),
    BotCommand(command="grant", description="Grant bot access to a user (Owner only)"),
    BotCommand(command="denied", description="Revoke access from a user (Owner only)"),
    BotCommand(command="list", description="List granted users & b2 history"),
    BotCommand(command="makelist", description="Update your checklist: /makelist <title>"),
]
        try:
            await bot.set_my_commands(commands)
            logger.info("✅ Bot commands registered!")
        except Exception as cmd_err:
            logger.warning(f"Could not set bot commands: {cmd_err}")

        # Webhook cleanup
        await bot.delete_webhook(drop_pending_updates=True)
        logger.info("✅ Webhook deleted and pending updates dropped!")

        await start_server()
        logger.info("✅ Bot polling started!")
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    except Exception as e:
        logger.error(f"❌ Startup error: {e}")
        raise
    finally:
        logger.info("🔌 Closing bot session and MongoDB connection...")
        try:
            await bot.session.close()
        except Exception as close_err:
            logger.warning(f"Error closing bot session: {close_err}")
        try:
            client.close()
        except Exception as close_err:
            logger.warning(f"Error closing MongoDB client: {close_err}")
        logger.info("👋 Shutdown complete.")

if __name__ == "__main__":
    asyncio.run(main())
