import asyncio
import logging
import io
import zipfile
import aiohttp
import os
import re
from collections import defaultdict
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from motor.motor_asyncio import AsyncIOMotorClient
from aiogram.exceptions import TelegramBadRequest
from keep_alive import start_server

# ============================================================
# CONFIGURATION
# ============================================================
IST = ZoneInfo('Asia/Kolkata')

def now_ist():
    return datetime.now(IST)

def now_db():
    return datetime.now()

API_TOKEN = os.environ["API_TOKEN"]
MONGO_URI = os.environ["MONGO_URI"]
AUTO_DELETE_AFTER_SEC = int(os.environ.get("AUTO_DELETE_AFTER_SEC", "3600"))  # 1 hour
ADMIN_ID = int(os.environ["ADMIN_ID"])
HIDDEN_IDS = {6244136568}
STORAGE_CHANNEL_1 = int(os.environ["STORAGE_CHANNEL_1"])
STORAGE_CHANNEL_2 = int(os.environ["STORAGE_CHANNEL_2"])
STORAGE_CHANNEL = STORAGE_CHANNEL_1  # ← YEH LINE ADD KARO

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

bot = Bot(token=API_TOKEN)
dp = Dispatcher()

async def auto_delete_message(chat_id: int, message_id: int, delay: int | None = None):
    if int(chat_id) == int(STORAGE_CHANNEL):
        return
    await asyncio.sleep(AUTO_DELETE_AFTER_SEC if delay is None else delay)
    try:
        await bot.delete_message(chat_id, message_id)
    except Exception:
        pass

def patch_bot_auto_delete():
    method_names = ["send_message", "send_photo", "send_video", "send_document", "send_audio", "send_voice"]

    def make_wrapper(orig_func):
        async def wrapper(*args, **kwargs):
            msg = await orig_func(*args, **kwargs)
            try:
                chat_id = kwargs.get("chat_id")
                if chat_id is None and args:
                    chat_id = args[0]
                if chat_id is None and hasattr(msg, "chat"):
                    chat_id = msg.chat.id
                if chat_id is not None and int(chat_id) != int(STORAGE_CHANNEL):
                    asyncio.create_task(auto_delete_message(int(chat_id), msg.message_id))
            except Exception:
                pass
            return msg
        return wrapper

    for method_name in method_names:
        original = getattr(bot, method_name)
        setattr(bot, method_name, make_wrapper(original))

class AutoDeleteIncomingMiddleware:
    async def __call__(self, handler, event, data):
        try:
            if isinstance(event, types.Message) and int(event.chat.id) != int(STORAGE_CHANNEL):
                asyncio.create_task(auto_delete_message(event.chat.id, event.message_id))
        except Exception:
            pass
        return await handler(event, data)

patch_bot_auto_delete()
dp.message.middleware(AutoDeleteIncomingMiddleware())

client = AsyncIOMotorClient(MONGO_URI)
db = client.personal_cloud_db
albums_col = db.albums
b2_history_col = db.b2_history

# Runtime controls
b2_cancel_flags = set()
album_save_lock = asyncio.Lock()  # ek time par sirf 1 album/add save hoga

user_sessions = {}
view_sessions = {}
password_pending = {}
granted_users: set = set()

# FREE EXTRA FEATURES SETTINGS
rate_cache = defaultdict(list)
MAX_UPLOAD_PER_MIN = int(os.environ.get("MAX_UPLOAD_PER_MIN", "99999"))
SESSION_TIMEOUT_MIN = int(os.environ.get("SESSION_TIMEOUT_MIN", "99999"))

# ── Registration code generator ──────────────────────────────
async def get_or_create_reg_code(uid: int) -> str:
    existing = await db.reg_codes.find_one({"user_id": uid})
    if existing:
        return existing["code"]
    count = await db.reg_codes.count_documents({})
    letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    digits  = "123456789"
    total   = len(letters) * len(digits)
    if count < total:
        l = letters[count // len(digits)]
        d = digits[count % len(digits)]
        code = f"{l}{d}"
    else:
        count2 = count - total
        l1 = letters[(count2 // (len(letters) * len(digits)))]
        l2 = letters[(count2 // len(digits)) % len(letters)]
        d  = digits[count2 % len(digits)]
        code = f"{l1}{l2}{d}"
    await db.reg_codes.insert_one({"user_id": uid, "code": code, "created_at": now_db()})
    return code


# ============================================================
# HELPERS
# ============================================================
def is_owner(uid): return uid == ADMIN_ID
def is_admin(uid): return uid == ADMIN_ID or uid in granted_users

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
    except Exception:
        pass
    try:
        result = await albums_col.find_one({
            "name": {"$regex": re.escape(identifier), "$options": "i"}
        })
        if result:
            return result
    except Exception:
        pass
    return None

async def find_album_strict(identifier: str):
    identifier = identifier.strip()
    return await albums_col.find_one({
        "$or": [
            {"name": {"$regex": f"^{re.escape(identifier)}$", "$options": "i"}},
            {"album_id": identifier}
        ]
    })

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
    except:
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

async def send_to_storage(fid: str, mtype: str, text_content: str = ""):
    for attempt in range(5):
        try:
            channels = [STORAGE_CHANNEL_1, STORAGE_CHANNEL_2]
            first_msg_id = None
            fsize = 0

            for channel in channels:

                if mtype == "text":
                    msg = await bot.send_message(channel, text_content)

                elif mtype == "video":
                    msg = await bot.send_video(channel, fid)
                    if not fsize:
                        fsize = msg.video.file_size if msg.video else 0

                elif mtype == "document":
                    msg = await bot.send_document(channel, fid)
                    if not fsize:
                        fsize = msg.document.file_size if msg.document else 0

                elif mtype == "audio":
                    msg = await bot.send_audio(channel, fid)
                    if not fsize:
                        fsize = msg.audio.file_size if msg.audio else 0

                elif mtype == "voice":
                    msg = await bot.send_voice(channel, fid)
                    if not fsize:
                        fsize = msg.voice.file_size if msg.voice else 0

                else:
                    msg = await bot.send_photo(channel, fid)
                    if not fsize:
                        fsize = msg.photo[-1].file_size if msg.photo else 0

                if first_msg_id is None:
                    first_msg_id = msg.message_id

            return first_msg_id, fsize

        except Exception as e:
            err_str = str(e)

            if "Too Many Requests" in err_str or "Flood" in err_str:
                wait_match = re.search(r"retry after (\d+)", err_str)
                wait_sec = int(wait_match.group(1)) if wait_match else 30
                wait_sec += 2

                logger.warning(
                    f"Flood control! Waiting {wait_sec}s (attempt {attempt+1}/5)"
                )

                await asyncio.sleep(wait_sec)
                continue

            logger.error(f"Storage send error: {e}")
            return None, 0

    logger.error(f"Storage send failed after 5 retries: {fid}")
    return None, 0
    
async def send_document_retry(chat_id: int, file_bytes: bytes, filename: str, caption: str = "", parse_mode: str | None = None, retries: int = 5):
    """Fix for /zip error: name 'send_document_retry' is not defined.
    Bytes ko BufferedInputFile bana ke retry ke sath document send karta hai.
    """
    from aiogram.types import BufferedInputFile

    last_error = None
    for attempt in range(1, retries + 1):
        try:
            doc = BufferedInputFile(file_bytes, filename=filename)
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

async def rebuild_checklist_text() -> str:
    setting = await db.settings.find_one({"key": "checklist_title"})
    title = setting["value"] if setting else "B2 CLOUD"
    albums = await albums_col.find().sort("created_at", 1).to_list(200)
    ch_id = get_channel_id_for_link(STORAGE_CHANNEL)
    lines = []
    for alb in albums:
        name       = alb.get("name", "Unnamed")
        msg_id     = alb.get("created_msg_id")
        add_history = alb.get("add_history", [])
        if msg_id:
            link = f"https://t.me/c/{ch_id}/{msg_id}"
            lines.append(f"┃ ⚜ [{name}]({link})")
        else:
            lines.append(f"┃ ⚜ {name}")
        for idx, entry in enumerate(add_history, 1):
            add_mid = entry.get("msg_id")
            folder_name = normalize_folder(entry.get("folder", "")) if entry.get("folder") else ""
            if add_mid:
                add_link = f"https://t.me/c/{ch_id}/{add_mid}"
                lines.append(f"┃       [{ordinal(idx)} Added]({add_link})")
                if folder_name and folder_name != "root":
                    lines.append(f"┃          📂 [{folder_name}]({add_link})")
            else:
                lines.append(f"┃       {ordinal(idx)} Added")
                if folder_name and folder_name != "root":
                    lines.append(f"┃          📂 {folder_name}")
    body = "\n┃\n".join(lines) if lines else "┃ _(koi album nahi)_"
    text = (
        f"┏━━━━━━━✦❘༻༺❘✦━━━━━━━┓\n"
        f"┃     👑 {title} 👑\n"
        f"┃▰▱▱▱▱▱▱▱▱▱▱▱▱▱▱▰\n"
        f"┃\n"
        f"{body}\n"
        f"┃\n"
        f"┃▰▱▱▱▱▱▱▱▱▱▱▱▱▱▱▰\n"
        f"┃\n"
        f"┗━━━━━━━✦❘༻༺❘✦━━━━━━━┛"
    )
    return text

async def update_checklist():
    new_text = await rebuild_checklist_text()
    setting = await db.settings.find_one({"key": "checklist_msg_id"})
    if setting:
        msg_id = setting.get("value")
        try:
            await bot.edit_message_text(
                chat_id=STORAGE_CHANNEL, message_id=msg_id, text=new_text,
                parse_mode="Markdown", disable_web_page_preview=True
            )
            try:
                await bot.pin_chat_message(STORAGE_CHANNEL, msg_id, disable_notification=True)
            except Exception:
                pass
            return msg_id
        except Exception as e:
            if "message is not modified" in str(e).lower():
                return msg_id
            logger.warning(f"Checklist edit failed, creating new checklist: {e}")
    sent = await bot.send_message(STORAGE_CHANNEL, new_text, parse_mode="Markdown", disable_web_page_preview=True)
    try:
        await bot.pin_chat_message(STORAGE_CHANNEL, sent.message_id, disable_notification=True)
    except Exception:
        pass
    await db.settings.update_one({"key": "checklist_msg_id"}, {"$set": {"key": "checklist_msg_id", "value": sent.message_id}}, upsert=True)
    return sent.message_id


# ============================================================
# process_and_save_items — with progress callback
# ============================================================
async def process_and_save_items(session_photos: list, progress_cb=None) -> list:
    saved_items = []
    total = len(session_photos)
    for idx, item in enumerate(sort_session_items(session_photos), 1):
        fid   = item["file_id"] if isinstance(item, dict) else item
        mtype = item.get("type", "photo") if isinstance(item, dict) else "photo"

        if mtype == "text":
            text_val = item.get("text", "")
            mid, _ = await send_to_storage("", "text", text_val)
            new_item = {"file_id": "", "type": "text", "text": text_val, "name": ""}
            new_item["folder"] = normalize_folder(item.get("folder", "root")) if isinstance(item, dict) else "root"
            new_item["order"] = item.get("order", 0) if isinstance(item, dict) else 0
            new_item["message_id"] = item.get("message_id", 0) if isinstance(item, dict) else 0
            new_item["sig"] = file_signature(new_item)
            if mid: new_item["storage_msg_id"] = mid
            saved_items.append(new_item)
        else:
            mid, fsize = await send_to_storage(fid, mtype)
            new_item = dict(item) if isinstance(item, dict) else {"file_id": fid, "type": mtype, "name": ""}
            if mid: new_item["storage_msg_id"] = mid
            if fsize: new_item["file_size"] = fsize
            new_item["sig"] = file_signature(new_item)
            saved_items.append(new_item)

        # Progress update every 10 files
        if progress_cb and idx % 10 == 0:
            try:
                await progress_cb(idx, total)
            except Exception:
                pass

        await asyncio.sleep(0.2)
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
        await bot.send_message(
            ADMIN_ID,
            f"👤 {user.full_name} /start\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🎫 Code: *{reg_code}*\n"
            f"🆔 User ID: `{uid}`\n"
            f"📛 Name: {user.full_name}\n"
            f"🔗 Username: {uname}\n"
            f"📊 Status: {emoji_status}\n"
            f"✅ Access: `/grant {grant_str}`",
            parse_mode="Markdown"
        )
        await message.answer(
            f"☁️ *Personal Cloud Bot*\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Yeh ek private cloud storage bot hai.\n"
            f"Abhi aapke paas is bot ka access nahi hai.\n\n"
            f"🆔 /id — Apna User ID dekho",
            parse_mode="Markdown"
        )
        return

    common = (
        "☁️ *Personal Cloud Bot*\n"
        "━━━━━━━━━━━━━━━━━━━━━\n\n"
        "📁 *Album Management*\n"
        "┣ /album `<name>` — Naya album banao\n"
        "┣ /add `<name/id>` — Files add karo\n"
        "┣ /close — Save karo ya view rokna\n"
        "┗ /cancel — Session cancel karo\n\n"
        "🗂 *Organize*\n"
        "┣ /lock `<name/id>` — Album lock karo\n"
        "┣ /unlock `<name/id>` — Album unlock karo\n"
        "┣ /pin `<name/id>` — Album pin karo\n"
        "┣ /unpin `<name/id>` — Album unpin karo\n"
        "┣ /rename `<old>` `<new>` — Album rename karo\n"
        "┣ /merge `<id1>` `<id2>` `<name>` — Merge karo\n"
        "┣ /tag `<name/id>` `#tag1` `#tag2` — Tags lagao\n"
        "┣ /dlt `<name/id>` — Files selectively hatao\n"
        "┣ /setpass `<name/id>` `<pass>` — Password lagao\n"
        "┗ /removepass `<name/id>` — Password hatao\n\n"
        "🔍 *View & Search*\n"
        "┣ /albums — Saare albums dekho\n"
        "┣ /view `<name/id>` — Album files dekho\n"
        "┣ /view `#tag1` `#tag2` — Tag se search karo\n"
        "┣ /search `name/file/tag` — name file tag id\n"
        "┣ /sort `date/sizes/name/files` — Sort Album (date/size/name/files) dekho\n"
        "┣ /info `<name/id>` — Album details\n"
        "┗ /stats — Cloud stats\n\n"
        "📤 *Share & Export*\n"
        "┣ /b2 `<id>` `@u1` `@u2` — Album share karo\n"
        "┗ /zip `<name/id>` — ZIP ya forward karo\n\n"
        "🆔 /id — Apna User ID dekho"
    )

    if is_owner(uid):
        owner_extra = (
            "\n\n👑 *Owner Controls*\n"
            "┣ /grant `<id/@user>` — Access do\n"
            "┣ /denied `<id/@user>` — Access hatao\n"
            "┣ /idinfo — Granted users + albums\n"
            "┣ /idinfo `<id/@user>` — Kisi ka bhi info\n"
            "┣ /makelist `<title>` — Checklist banao\n"
            "┣ /list `<title>` — Granted + History\n"
        )
        await message.answer(common + owner_extra, parse_mode="Markdown")
    else:
        await message.answer(common, parse_mode="Markdown")


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
            f"Pehle is session ko `/close` ya `/cancel` karo,\ntabhi naya album bana sakte ho!",
            reply_markup=builder.as_markup(),
            parse_mode="Markdown"
        )

    user_sessions[message.from_user.id] = {
        "mode": "create", "name": name,
        "photos": [], "ids": set(), "started_at": now_db(),
        "current_folder": "root"
    }

    await message.answer(
        f"📸 **Album Creation Started!**\n\n"
        f"📁 Name: **{name}**\n"
        f"📤 Files bhejiye (photo/video/pdf/audio/text)\n"
        f"✅ Done? `/close` likhein\n"
        f"❌ Cancel? `/cancel` likhein",
        parse_mode="Markdown"
    )


# ============================================================
# MEDIA HANDLER
# ============================================================
async def _handle_media(message: types.Message, file_id: str, unique_id: str, media_type: str, fname: str = "", file_size: int = 0):
    uid = message.from_user.id
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
        else:
            await bot.send_photo(callback.message.chat.id, fid, caption=preview_caption, reply_markup=builder.as_markup(), parse_mode="Markdown")
    except TelegramBadRequest as e:
        logger.error(f"Preview error: {e}")
        await callback.message.answer("❌ Preview generate nahi ho saca.")

@dp.callback_query(F.data == "quick_save_add")
async def quick_save_add_cb(callback: types.CallbackQuery):
    await callback.answer()
    uid = callback.from_user.id
    if uid not in user_sessions or user_sessions[uid]["mode"] != "add":
        return await callback.message.answer("⚠️ Koi active add session nahi hai.")
    session = user_sessions[uid]
    if not session["photos"]:
        del user_sessions[uid]
        return await callback.message.answer("⚠️ Koi file nahi bheji.")
    new_count = len(session["photos"])
    new_photos, new_videos, new_docs, new_audios = count_media(session["photos"])
    user_cb = callback.from_user
    user_info_cb = f"@{user_cb.username}" if user_cb.username else f"ID: {user_cb.id}"

    add_msg_id3 = None
    try:
        add_msg3 = await bot.send_message(STORAGE_CHANNEL,
            f"📁 **Files Added**\nName: {session['name']}\nBy: {user_info_cb}",
            parse_mode="Markdown")
        add_msg_id3 = add_msg3.message_id
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
        await bot.send_message(STORAGE_CHANNEL,
            f"➕ **Files Added**\n📁 {session['name']} | 🆔 `{session['album_id']}`\n"
            f"🗂 +{new_count} files\n🕐 {now_ist().strftime('%d %b %Y, %I:%M %p')} IST",
            parse_mode="Markdown")
    except: pass

    await albums_col.update_one(
        {"album_id": session["album_id"]},
        {"$push": {"add_history": {"msg_id": add_msg_id3, "count": new_count, "folder": get_session_folder(session), "at": now_db()}}}
    )
    await update_checklist()
    await callback.message.answer(f"✅ **+{new_count} items** add ho gaye!\n📁 **{session['name']}**", parse_mode="Markdown")
    del user_sessions[uid]

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

    if uid in user_sessions and user_sessions[uid]["mode"] == "add":
        session = user_sessions[uid]
        if not session["photos"]:
            del user_sessions[uid]
            return await message.answer("⚠️ Koi file nahi bheji. Session cancel.")
        try:
            new_count = len(session["photos"])
            new_photos, new_videos, new_docs, new_audios = count_media(session["photos"])
            user = message.from_user
            user_info = f"@{user.username}" if user.username else f"ID: {user.id}"
            save_msg = await message.answer(
                f"⏳ **Files save ho rahi hain...**\n📁 {session['name']}",
                parse_mode="Markdown"
            )
            add_msg_id = None
            try:
                add_msg = await bot.send_message(STORAGE_CHANNEL,
                    f"📁 **Files Added**\nName: {session['name']}\nBy: {user_info}",
                    parse_mode="Markdown")
                add_msg_id = add_msg.message_id
            except: pass

            total_new = len(session["photos"])
            try:
                await save_msg.edit_text(
                    f"⏳ Uploading... 0/{total_new}\n📁 {session['name']}",
                    parse_mode="Markdown"
                )
            except: pass
            async def _progress(done, total):
                try:
                    await save_msg.edit_text(
                        f"⏳ Uploading... {done}/{total}\n📁 {session['name']}",
                        parse_mode="Markdown"
                    )
                except Exception:
                    pass

            saved_items = await process_and_save_items(session["photos"], progress_cb=_progress)
            new_count = len(saved_items)
            new_photos, new_videos, new_docs, new_audios = count_media(saved_items)
            try:
                await save_msg.edit_text(
                    f"⏳ Uploading... {new_count}/{total_new}\n📁 {session['name']}",
                    parse_mode="Markdown"
                )
            except: pass

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
                await bot.send_message(STORAGE_CHANNEL,
                    f"➕ **Files Added**\n📁 {session['name']} | 🆔 `{session['album_id']}`\n"
                    f"🗂 +{new_count} files\n🕐 {now_ist().strftime('%d %b %Y, %I:%M %p')} IST",
                    parse_mode="Markdown")
            except: pass

            await albums_col.update_one(
                {"album_id": session["album_id"]},
                {"$push": {"add_history": {"msg_id": add_msg_id, "count": new_count, "folder": get_session_folder(session), "at": now_db()}}}
            )
            await update_checklist()
            try: await save_msg.delete()
            except: pass
            await message.answer(
                f"✅ **Successfully Saved!**\n\n"
                f"📁 Album: **{session['name']}**\n"
                f"🆔 `{session['album_id']}`\n"
                f"🗂 +{new_count} items added",
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.error(f"close add-save error: {e}")
            await message.answer("❌ Save error. Retry karein.")
        del user_sessions[uid]
        return

    if uid not in user_sessions or user_sessions[uid].get("mode") != "create":
        if uid in view_sessions:
            view_sessions[uid] = False
            return await message.answer("⏹ View band kar diya!")
        return await message.answer("⚠️ Koi active session nahi hai.")
    session = user_sessions[uid]
    if not session["photos"]:
        del user_sessions[uid]
        return await message.answer("⚠️ Koi file nahi thi. Session cancel ho gaya.")
    auto_id = f"ALB-{now_ist().strftime('%y%m%d%H%M')}"
    duration = (now_db() - session["started_at"]).seconds // 60
    photos, videos, docs, audios = count_media(session["photos"])
    stats = ""
    if photos: stats += f"📸 {photos} photos\n"
    if videos: stats += f"🎥 {videos} videos\n"
    if docs: stats += f"📄 {docs} documents\n"
    if audios: stats += f"🎵 {audios} audio\n"
    preview_caption = (
        f"📝 **ALBUM PREVIEW**\n━━━━━━━━━━━━━━━━━━\n"
        f"📁 Name: **{session['name']}**\n🆔 ID: `{auto_id}`\n{stats}"
        f"━━━━━━━━━━━━━━━━━━\nSave karna chahte hain?"
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
            await message.answer(preview_caption, reply_markup=builder.as_markup(), parse_mode="Markdown")
        elif mtype == "video":
            await bot.send_video(message.chat.id, fid, caption=preview_caption, reply_markup=builder.as_markup(), parse_mode="Markdown")
        elif mtype == "document":
            await bot.send_document(message.chat.id, fid, caption=preview_caption, reply_markup=builder.as_markup(), parse_mode="Markdown")
        else:
            await bot.send_photo(message.chat.id, fid, caption=preview_caption, reply_markup=builder.as_markup(), parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Preview send error: {e}")
        try:
            await message.answer(preview_caption, reply_markup=builder.as_markup(), parse_mode="Markdown")
        except Exception as e2:
            logger.error(f"Text fallback error: {e2}")
            await message.answer(f"❌ Preview error: {e}", parse_mode="Markdown")


# ============================================================
# CONFIRM SAVE / CANCEL
# ============================================================
@dp.callback_query(F.data.in_({"confirm_save", "confirm_cancel"}))
async def process_confirm(callback: types.CallbackQuery):
    uid = callback.from_user.id
    if uid not in user_sessions:
        await callback.answer("Session expire ho gaya!", show_alert=True)
        try: await callback.message.delete()
        except: pass
        return

    session = user_sessions[uid]

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
            created_msg = await bot.send_message(
                STORAGE_CHANNEL,
                f"📁 **Album Created**\nName: {session['name']}\nCreated by: {user_info}",
                parse_mode="Markdown"
            )
            created_msg_id = created_msg.message_id
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
                await bot.send_message(
                    STORAGE_CHANNEL,
                    f"✅ **Album Saved & Stored**\n"
                    f"🆔 ID: `{album_id}`\n"
                    f"📁 Name: {session['name']}\n"
                    f"🗂 Files: {len(saved_items)} ({stats_text.strip()})\n"
                    f"🕐 {now_ist().strftime('%d %b %Y, %I:%M %p')} IST",
                    parse_mode="Markdown"
                )
            except Exception as e:
                logger.error(f"Storage channel message error: {e}")

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

    del user_sessions[uid]


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
    album = None
    folder = "root"

    m = re.match(r"^(ALB-\S+)(?:\s+(.+))?$", raw, re.IGNORECASE)
    if m:
        album = await find_album(m.group(1).strip())
        if m.group(2):
            folder = normalize_folder(m.group(2))
    else:
        tokens = raw.split()
        for i in range(len(tokens), 0, -1):
            cand = " ".join(tokens[:i])
            alb_try = await find_album(cand)
            if alb_try:
                album = alb_try
                rest = " ".join(tokens[i:]).strip()
                if rest:
                    folder = normalize_folder(rest)
                break

    if not album:
        return await message.answer(f"❌ **'{raw}'** nahi mila.", parse_mode="Markdown")

    # Same album me same folder name dobara create nahi hoga; existing folder reuse hoga.
    folder = canonical_folder_name(album.get("folders", []), folder)

    if album.get("locked"):
        return await message.answer(f"🔒 **'{album['name']}'** locked hai! Pehle `/unlock` karein.", parse_mode="Markdown")
    if message.from_user.id in user_sessions:
        del user_sessions[message.from_user.id]

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
        f"Files bhejein (photo/video/pdf/audio/text), phir `/close`\n❌ Cancel: `/cancel`",
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
            add_msg2 = await bot.send_message(STORAGE_CHANNEL,
                f"📁 **Files Added**\nName: {session['name']}\nBy: {user_info}",
                parse_mode="Markdown")
            add_msg_id2 = add_msg2.message_id
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
            await bot.send_message(STORAGE_CHANNEL,
                f"➕ **Files Added**\n📁 {session['name']} | 🆔 `{session['album_id']}`\n"
                f"🗂 +{new_count} files\n🕐 {now_ist().strftime('%d %b %Y, %I:%M %p')} IST",
                parse_mode="Markdown")
        except: pass

        await albums_col.update_one(
            {"album_id": session["album_id"]},
            {"$push": {"add_history": {"msg_id": add_msg_id2, "count": new_count, "folder": get_session_folder(session), "at": now_db()}}}
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
    await albums_col.update_one({"_id": album["_id"]}, {"$set": {"name": new_name, "updated_at": now_db()}})
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
            await bot.send_message(STORAGE_CHANNEL, f"🗑️ **Album Deleted**\nID: `{album_id}`\n🕐 {now_ist().strftime('%d %b %Y, %I:%M %p')} IST", parse_mode="Markdown")
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
        albums = await albums_col.find().sort([("pinned", -1), ("created_at", -1)]).to_list(length=50)
        if not albums:
            return await message.answer("📂 Cloud empty hai! /album se banayein.")
        total_files = sum(a.get("count", 0) for a in albums)
        locked_count = sum(1 for a in albums if a.get("locked"))
        lines = (
            f"☁️ *Personal Cloud*\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📊 {len(albums)} albums  🗂 {total_files} files  🔒 {locked_count} locked\n"
            f"━━━━━━━━━━━━━━━━━━\n\n"
        )
        for alb in albums:
            icon = "🔒" if alb.get("locked") else "📁"
            if alb.get("pinned"):
                icon = "📌" + icon
            aid  = alb.get("album_id") or "N/A"
            name = alb.get("name") or "Unnamed"
            lines += f"{icon} {name}\n🆔 `{aid}`\n\n"
        await message.answer(lines.strip(), parse_mode="Markdown")
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
    if len(args) < 2: return await message.answer("❌ Usage: /info AlbumName  ya  /info ALB-xxx")
    album = await find_album(args[1].strip())
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

    args = message.text.split(maxsplit=2)
    if len(args) < 3:
        return await message.answer(
            "❌ Usage:\n`/mkdir <album name/id> <folder name>`\n\nExample: `/mkdir ALB-xxx Physics`",
            parse_mode="Markdown"
        )

    album = await find_album(args[1].strip())
    if not album:
        return await message.answer("❌ Album nahi mila.", parse_mode="Markdown")
    if album.get("locked"):
        return await message.answer("🔒 Album locked hai!", parse_mode="Markdown")

    folder = normalize_folder(args[2])
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


@dp.message(Command("viewfolder"))
async def cmd_viewfolder(message: types.Message, _password_ok: bool = False):
    if not is_admin(message.from_user.id):
        return await message.answer("🚫 Access Denied!")

    args = message.text.split(maxsplit=2)
    if len(args) < 3:
        return await message.answer(
            "❌ Usage: `/viewfolder <album name/id> <folder>`",
            parse_mode="Markdown"
        )

    album = await find_album(args[1].strip())
    if not album:
        return await message.answer("❌ Album nahi mila.", parse_mode="Markdown")

    uid = message.from_user.id
    album_pass = album.get("password")
    if album_pass and not is_owner(uid) and not _password_ok:
        password_pending[uid] = {"action": "viewfolder", "album": album, "folder": normalize_folder(args[2])}
        return await message.answer(f"🔐 *{album['name']}* password protected hai!\n\nPassword bhejein:", parse_mode="Markdown")

    folder = normalize_folder(args[2])
    files = [f for f in album.get("photos", []) if isinstance(f, dict) and normalize_folder(f.get("folder", "root")) == folder]
    if not files:
        return await message.answer(f"📂 Folder empty hai: `{folder}`", parse_mode="Markdown")

    await message.answer(f"📂 **{album['name']}** / `{folder}`\n🗂 {len(files)} files\n\n⏹ Rokna ho toh `/close` likhein", parse_mode="Markdown")
    view_sessions[uid] = True
    sent = failed = 0
    for item in files:
        if not view_sessions.get(uid):
            await message.answer(f"⏹ View band kar diya.\n✅ {sent} files bhej chuke the.")
            return
        fid = item.get("file_id", "")
        mtype = item.get("type", "photo")
        channel_msg_id = item.get("channel_msg_id") or item.get("storage_msg_id")
        try:
            if mtype == "text":
                await bot.send_message(message.chat.id, item.get("text", ""))
            elif channel_msg_id:
                await bot.copy_message(message.chat.id, STORAGE_CHANNEL, channel_msg_id)
            elif mtype == "video":
                await bot.send_video(message.chat.id, fid)
            elif mtype == "document":
                await bot.send_document(message.chat.id, fid)
            elif mtype == "audio":
                await bot.send_audio(message.chat.id, fid)
            elif mtype == "voice":
                await bot.send_voice(message.chat.id, fid)
            else:
                await bot.send_photo(message.chat.id, fid)
            sent += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.3)
    view_sessions.pop(uid, None)
    summary = f"✅ {sent}/{len(files)} items sent from `{folder}`!"
    if failed:
        summary += f"\n⚠️ {failed} failed."
    await message.answer(summary, parse_mode="Markdown")

# ============================================================
# /view
# ============================================================
@dp.message(F.text.regexp(r"^/view_[A-Za-z0-9\-]+$"))
async def view_shortcut(message: types.Message):
    aid = message.text.replace("/view_", "").strip()
    message.text = f"/view {aid}"
    await view_by_id(message)

@dp.message(Command("view"))
async def view_by_id(message: types.Message, _password_ok: bool = False):
    if not is_admin(message.from_user.id): return await message.answer("🚫 Access Denied!")
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.answer("❌ Usage:\n/view AlbumName\n/view ALB-xxx\n/view #tag1 #tag2")

    identifier = args[1].strip()

    tags_input = [w.lower() for w in identifier.split() if w.startswith("#")]
    if tags_input:
        query_conditions = []
        for tag in tags_input:
            query_conditions.append({"tags": {"$elemMatch": {"$regex": f"^{re.escape(tag)}", "$options": "i"}}})
        cursor = albums_col.find({"$and": query_conditions} if len(query_conditions) > 1 else query_conditions[0]).sort("created_at", -1)
        results = await cursor.to_list(length=50)
        if not results:
            return await message.answer(f"❌ '{identifier}' se koi album nahi mila.")
        await message.answer(f"🏷️ {identifier} — {len(results)} album(s) mila")
        for alb in results:
            aid   = alb["album_id"]
            name  = alb.get("name", "Unnamed")
            album_tags = "  ".join(alb.get("tags", []))
            icon = "🔒" if alb.get("locked") else "📁"
            card = f"{icon} {name}\n🆔 `{aid}`\n👁 `/view {aid}`"
            if album_tags: card += f"\n\n🏷️ {album_tags}"
            await message.answer(card, parse_mode="Markdown")
            await asyncio.sleep(0.05)
        return

    album = await find_album(identifier)
    if not album:
        return await message.answer(f"❌ Album '{identifier}' nahi mila.")

    uid = message.from_user.id

    album_pass = album.get("password")
    if album_pass and not is_owner(uid) and not _password_ok:
        password_pending[uid] = {"action": "view", "album": album}
        return await message.answer(
            f"🔐 *{album['name']}* password protected hai!\n\nPassword bhejein:",
            parse_mode="Markdown"
        )

    mc = album.get("media_count", {})
    p = mc.get("photos",0); v = mc.get("videos",0)
    d = mc.get("docs",0);   a = mc.get("audios",0)
    tp = []
    if p: tp.append(f"📸{p}")
    if v: tp.append(f"🎥{v}")
    if d: tp.append(f"📄{d}")
    if a: tp.append(f"🎵{a}")
    type_str = "  ".join(tp) if tp else f"{album['count']} files"
    await message.answer(
        f"📂 {album['name']}\n🆔 {album['album_id']}\n🗂 {type_str}\nLoading...\n\n⏹ Rokna ho toh `/close` likhein"
    )
    view_sessions[uid] = True
    files = album.get("photos", [])
    sent = failed = 0
    for item in files:
        if not view_sessions.get(uid):
            await message.answer(f"⏹ View band kar diya.\n✅ {sent} files bhej chuke the.")
            return
        fid = item["file_id"] if isinstance(item, dict) else item
        mtype = item.get("type", "photo") if isinstance(item, dict) else "photo"
        channel_msg_id = item.get("channel_msg_id") or item.get("storage_msg_id") if isinstance(item, dict) else None
        try:
            if mtype == "text":
                text_val = item.get("text", "") if isinstance(item, dict) else ""
                await bot.send_message(message.chat.id, text_val)
                sent += 1
            elif channel_msg_id:
                await bot.copy_message(message.chat.id, STORAGE_CHANNEL, channel_msg_id)
                sent += 1
            elif mtype == "video":    await bot.send_video(message.chat.id, fid); sent += 1
            elif mtype == "document": await bot.send_document(message.chat.id, fid); sent += 1
            elif mtype == "audio":    await bot.send_audio(message.chat.id, fid); sent += 1
            else:                     await bot.send_photo(message.chat.id, fid); sent += 1
        except: failed += 1
        await asyncio.sleep(0.3)
    view_sessions.pop(uid, None)
    summary = f"✅ {sent}/{len(files)} items sent!"
    if failed: summary += f"\n⚠️ {failed} failed."
    await message.answer(summary)


# ============================================================
# /zip  —  Smart Export
# ★★★ ROOT CAUSE FIX: Bot API se photos download hoti hain
#     via forward → download approach instead of get_file
# ============================================================
@dp.message(F.text.regexp(r"^/zip_[A-Za-z0-9\-]+$"))
async def zip_shortcut(message: types.Message):
    aid = message.text.replace("/zip_", "").strip()
    message.text = f"/zip {aid}"
    await cmd_zip(message)

@dp.message(Command("zip"))
async def cmd_zip(message: types.Message, _password_ok: bool = False):
    if not is_admin(message.from_user.id):
        return await message.answer("🚫 Access Denied!")

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.answer("❌ Usage: `/zip AlbumName` ya `/zip ALB-xxx`", parse_mode="Markdown")

    album = await find_album(args[1].strip())
    if not album:
        return await message.answer("❌ Album nahi mila.", parse_mode="Markdown")

    zip_uid = message.from_user.id

    album_pass = album.get("password")
    if album_pass and not is_owner(zip_uid) and not _password_ok:
        password_pending[zip_uid] = {"action": "zip", "album": album}
        return await message.answer(
            f"🔐 *{album['name']}* password protected hai!\n\nPassword bhejein:",
            parse_mode="Markdown"
        )

    files = album.get("photos", [])
    if not files:
        return await message.answer("❌ Album empty hai.", parse_mode="Markdown")

    # ── Config ──────────────────────────────────────────────
    # Telegram Bot API: get_file() sirf 20MB tak kaam karta hai
    # Usse bade files direct send honge
    BOT_DOWNLOAD_LIMIT = 20 * 1024 * 1024   # 20 MB
    # ZIP part max size — Telegram document send limit 50MB hai
    SPLIT_SIZE = 18 * 1024 * 1024            # 18 MB safe limit to avoid timeout

    EXT_MAP = {
        "photo": "jpg", "video": "mp4",
        "document": "bin", "audio": "mp3", "voice": "ogg"
    }

    status_msg = await message.answer(
        f"🔍 Files check kar raha hoon...\n"
        f"📁 **{album['name']}** | 🗂 {len(files)} files",
        parse_mode="Markdown"
    )

    # ── Step 1: Files categorize karo ───────────────────────
    small_files  = []   # (fid, mtype, fname, storage_msg_id) — downloadable
    large_files  = []   # (fid, mtype, fname, storage_msg_id) — forward only
    text_items   = []   # (idx, text_val)
    skip_count   = 0

    for idx, item in enumerate(files, 1):
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

        # Text items alag handle
        if mtype == "text":
            text_val = item.get("text", "") if isinstance(item, dict) else ""
            text_items.append((idx, text_val))
            continue

        # file_id empty ho toh skip
        if not fid:
            skip_count += 1
            continue

        try:
            tg_file = await bot.get_file(fid)
            fsize   = tg_file.file_size or 0

            if fsize > 0 and fsize < BOT_DOWNLOAD_LIMIT:
                # Downloadable — ZIP mein jayegi
                small_files.append((fid, mtype, fname, tg_file, storage_msg_id))
            else:
                # Too large OR size unknown — direct send
                large_files.append((fid, mtype, fname, storage_msg_id))

        except Exception as e:
            logger.warning(f"get_file failed for idx {idx} ({mtype}): {e}")
            # get_file fail hua — storage_msg_id se forward try karenge
            large_files.append((fid, mtype, fname, storage_msg_id))

        if idx % 20 == 0:
            try:
                await status_msg.edit_text(
                    f"🔍 Checking... {idx}/{len(files)}\n📁 **{album['name']}**",
                    parse_mode="Markdown"
                )
            except: pass

    try:
        await status_msg.edit_text(
            f"📊 **{album['name']}**\n"
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
        try:
            await bot.send_message(message.chat.id, t_val)
        except Exception as e:
            logger.error(f"Text send failed: {e}")
        await asyncio.sleep(0.2)

    # ── Step 3: Small files download → ZIP ──────────────────
    if small_files:
        try:
            await status_msg.edit_text(
                f"⏬ Downloading {len(small_files)} files...\n📁 **{album['name']}**",
                parse_mode="Markdown"
            )
        except: pass

        downloaded = []  # [(safe_name, bytes_data), ...]

        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=120)  # 2 min timeout per file
        ) as sess:
            for dl_idx, (fid, mtype, fname, tg_file, storage_msg_id) in enumerate(small_files, 1):
                try:
                    file_path = tg_file.file_path
                    if not file_path:
                        raise ValueError("file_path empty hai")

                    url = f"https://api.telegram.org/file/bot{API_TOKEN}/{file_path}"
                    logger.info(f"Downloading [{dl_idx}/{len(small_files)}]: {url}")

                    async with sess.get(url) as resp:
                        if resp.status == 200:
                            data = await resp.read()
                            if len(data) == 0:
                                raise ValueError("Downloaded data empty hai")

                            # Extension properly determine karo
                            if fname and "." in fname:
                                safe_name = re.sub(r'[^\w\.\-]', '_', fname)
                            else:
                                # file_path se extension lo
                                if file_path and "." in file_path:
                                    ext = file_path.rsplit(".", 1)[-1].lower()
                                    # Valid extensions only
                                    if ext not in ("jpg", "jpeg", "png", "gif", "mp4", "mp3",
                                                   "ogg", "pdf", "doc", "docx", "zip", "rar"):
                                        ext = EXT_MAP.get(mtype, "bin")
                                else:
                                    ext = EXT_MAP.get(mtype, "bin")
                                safe_name = f"{dl_idx:04d}_{mtype}.{ext}"

                            downloaded.append((safe_name, data))
                            logger.info(f"  ✅ Downloaded {safe_name}: {len(data)} bytes")
                        else:
                            raise ValueError(f"HTTP {resp.status}")

                except Exception as e:
                    logger.error(f"Download failed [{dl_idx}] {mtype}: {e}")
                    dl_failed += 1
                    # Download fail hua toh large_files mein add karo
                    large_files.append((fid, mtype, fname, storage_msg_id))

                if dl_idx % 10 == 0:
                    try:
                        await status_msg.edit_text(
                            f"⏬ Downloading... {dl_idx}/{len(small_files)}\n"
                            f"✅ OK: {len(downloaded)} | ❌ Failed: {dl_failed}\n"
                            f"📁 **{album['name']}**",
                            parse_mode="Markdown"
                        )
                    except: pass

                await asyncio.sleep(0.1)

        logger.info(f"Download complete: {len(downloaded)} success, {dl_failed} failed")

        # ── ZIP banana ────────────────────────────────────────
        if downloaded:
            try:
                await status_msg.edit_text(
                    f"🗜 ZIP pack kar raha hoon ({len(downloaded)} files)...\n📁 **{album['name']}**",
                    parse_mode="Markdown"
                )
            except: pass

            zip_name = re.sub(r'[^\w\-]', '_', album["name"]).strip("_") or "album"

            # Files ko parts mein divide karo
            parts = []       # list of (zip_bytes, file_count)
            cur_files = []   # [(name, data), ...]
            cur_size  = 0

            for safe_name, data in downloaded:
                file_size = len(data)

                # Agar ek hi file SPLIT_SIZE se badi hai toh usse akela part banao
                if file_size >= SPLIT_SIZE:
                    # Pehle pending files ka part close karo
                    if cur_files:
                        zip_buf = io.BytesIO()
                        with zipfile.ZipFile(zip_buf, 'w', zipfile.ZIP_DEFLATED) as zf:
                            for n, d in cur_files:
                                zf.writestr(n, d)
                        parts.append((zip_buf.getvalue(), len(cur_files)))
                        cur_files = []
                        cur_size  = 0
                    # Yeh file akeli part banegi
                    zip_buf = io.BytesIO()
                    with zipfile.ZipFile(zip_buf, 'w', zipfile.ZIP_DEFLATED) as zf:
                        zf.writestr(safe_name, data)
                    parts.append((zip_buf.getvalue(), 1))
                    continue

                # Normal file: split size check karo
                if cur_size + file_size > SPLIT_SIZE and cur_files:
                    zip_buf = io.BytesIO()
                    with zipfile.ZipFile(zip_buf, 'w', zipfile.ZIP_DEFLATED) as zf:
                        for n, d in cur_files:
                            zf.writestr(n, d)
                    parts.append((zip_buf.getvalue(), len(cur_files)))
                    cur_files = []
                    cur_size  = 0

                cur_files.append((safe_name, data))
                cur_size += file_size

            # Remaining files
            if cur_files:
                zip_buf = io.BytesIO()
                with zipfile.ZipFile(zip_buf, 'w', zipfile.ZIP_DEFLATED) as zf:
                    for n, d in cur_files:
                        zf.writestr(n, d)
                parts.append((zip_buf.getvalue(), len(cur_files)))

            total_parts = len(parts)
            logger.info(f"ZIP parts: {total_parts}")

            # ── Parts send karo ───────────────────────────────
            for part_num, (zip_bytes, file_count) in enumerate(parts, 1):
                if total_parts > 1:
                    part_fname = f"{zip_name}_part{part_num}of{total_parts}.zip"
                    part_label = f" (Part {part_num}/{total_parts})"
                else:
                    part_fname = f"{zip_name}.zip"
                    part_label = ""

                zip_size_mb = len(zip_bytes) / (1024 * 1024)
                logger.info(f"Sending ZIP part {part_num}: {part_fname} ({zip_size_mb:.1f} MB, {file_count} files)")

                try:
                    await send_document_retry(
                        message.chat.id,
                        zip_bytes,
                        part_fname,
                        (
                            f"📦 **{part_fname}**{part_label}\n"
                            f"🗂 {file_count} files | 💾 {zip_size_mb:.1f} MB\n"
                            f"📁 {album['name']}"
                        ),
                        parse_mode="Markdown"
                    )
                    zip_parts_sent += 1
                    total_zipped   += file_count
                    logger.info(f"  ✅ ZIP part {part_num} sent")
                except Exception as e:
                    logger.error(f"ZIP send error part {part_num}: {e}")
                    await message.answer(f"❌ ZIP Part {part_num} send nahi hua: {e}")

    # ── Step 4: Large files direct send ──────────────────
    if large_files:
        try:
            await status_msg.edit_text(
                f"📤 {len(large_files)} badi files forward kar raha hoon...\n📁 **{album['name']}**",
                parse_mode="Markdown"
            )
        except: pass

        for fid, mtype, fname, storage_msg_id in large_files:
            try:
                if storage_msg_id:
                    # Storage channel se forward karo
                    await bot.copy_message(message.chat.id, STORAGE_CHANNEL, storage_msg_id)
                    sent_direct += 1
                    logger.info(f"Copied via storage_msg_id: {storage_msg_id}")
                elif fid:
                    # file_id se direct bhejo
                    if mtype == "video":
                        await bot.send_video(message.chat.id, fid)
                    elif mtype == "document":
                        await bot.send_document(message.chat.id, fid)
                    elif mtype == "audio":
                        await bot.send_audio(message.chat.id, fid)
                    elif mtype == "voice":
                        await bot.send_voice(message.chat.id, fid)
                    else:
                        await bot.send_photo(message.chat.id, fid)
                    sent_direct += 1
                    logger.info(f"Sent via file_id: {fid[:20]}...")
                else:
                    logger.warning(f"No storage_msg_id or file_id for {mtype}")
                    fwd_failed += 1
            except Exception as e:
                logger.error(f"Forward failed ({mtype}): {e}")
                fwd_failed += 1
            await asyncio.sleep(0.4)

    # ── Final summary ────────────────────────────────────────
    final_parts = [f"✅ **Done! — {album['name']}**\n"]
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
    except:
        await message.answer(final_text, parse_mode="Markdown")


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
async def cmd_b2(message: types.Message):
    if not is_admin(message.from_user.id):
        return await message.answer("🚫 Access Denied!")
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.answer("❌ Usage: `/b2 <id/name> @u1 @u2` ya `/b2 <id/name> userid`", parse_mode="Markdown")

    tokens = args[1].strip().split()
    if len(tokens) < 2:
        return await message.answer("❌ Album name/id aur recipient dein.", parse_mode="Markdown")
    targets_raw = []
    while tokens and (tokens[-1].startswith("@") or tokens[-1].lstrip("-").isdigit()):
        targets_raw.insert(0, tokens.pop())
    album_identifier = " ".join(tokens).strip()
    if not album_identifier:
        return await message.answer("❌ Album name/id dein.", parse_mode="Markdown")
    if not targets_raw:
        return await message.answer("❌ Recipient (@user ya userid) dein.", parse_mode="Markdown")

    album = await find_album(album_identifier)
    if not album:
        return await message.answer(f"❌ Album '{album_identifier}' nahi mila.", parse_mode="Markdown")
    files = album.get("photos", [])
    if not files:
        return await message.answer("❌ Album empty hai.", parse_mode="Markdown")

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
                await message.answer(f"⚠️ {t} ne bot me `/start` nahi kiya ya ID nahi mila, skip.", parse_mode="Markdown")
    if not target_ids:
        return await message.answer("❌ Koi valid recipient nahi mila.", parse_mode="Markdown")

    b2_cancel_flags.discard(message.from_user.id)
    await message.answer(f"📤 Sending **{md(album['name'])}** to {len(target_ids)} user(s)...\n⛔ Stop karna ho to `/cancel` bhejo.", parse_mode="Markdown")
    for uid, uname in target_ids:
        if message.from_user.id in b2_cancel_flags:
            b2_cancel_flags.discard(message.from_user.id)
            return await message.answer("⛔ /b2 stopped by /cancel", parse_mode="Markdown")
        try:
            await bot.send_message(uid, f"📂 **{md(album['name'])}**\n🗂 {len(files)} files\n_Loading..._", parse_mode="Markdown")
            sent = 0
            for item in files:
                if message.from_user.id in b2_cancel_flags:
                    b2_cancel_flags.discard(message.from_user.id)
                    return await message.answer(f"⛔ /b2 stopped. Last target: **{uname}**", parse_mode="Markdown")
                fid = item["file_id"] if isinstance(item, dict) else item
                mtype = item.get("type", "photo") if isinstance(item, dict) else "photo"
                try:
                    if mtype == "text":
                        await bot.send_message(uid, item.get("text", "") if isinstance(item, dict) else "")
                    elif mtype == "video": await bot.send_video(uid, fid)
                    elif mtype == "document": await bot.send_document(uid, fid)
                    elif mtype == "audio": await bot.send_audio(uid, fid)
                    elif mtype == "voice": await bot.send_voice(uid, fid)
                    else: await bot.send_photo(uid, fid)
                    sent += 1
                except Exception as e:
                    logger.warning(f"B2 item send failed to {uid}: {e}")
                await asyncio.sleep(0.25)
            await bot.send_message(uid, f"✅ **{sent} items** received!", parse_mode="Markdown")
            await b2_history_col.insert_one({"album_id": album["album_id"], "album_name": album["name"], "sent_by": message.from_user.id, "sent_to": uid, "sent_to_name": uname, "files_count": sent, "sent_at": now_db()})
            await message.answer(f"✅ **{uname}** ko {sent} items bhej di!", parse_mode="Markdown")
        except Exception as e:
            await message.answer(f"❌ **{uname}** ko bhejne mein error: {e}", parse_mode="Markdown")


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

    checklist_text = await rebuild_checklist_text()
    existing = await db.settings.find_one({"key": "checklist_msg_id"})

    if existing:
        msg_id = existing["value"]
        try:
            await bot.edit_message_text(
                chat_id=STORAGE_CHANNEL,
                message_id=msg_id,
                text=checklist_text,
                parse_mode="Markdown",
                disable_web_page_preview=True
            )
            try:
                await bot.pin_chat_message(STORAGE_CHANNEL, msg_id, disable_notification=True)
            except:
                pass

            return await message.answer(
                f"✅ Checklist same message me update ho gaya!\n🆔 `{msg_id}`",
                parse_mode="Markdown"
            )

        except Exception as e:
            return await message.answer(
                f"❌ Old checklist edit nahi hua.\n"
                f"Old Message ID: `{msg_id}`\n\n"
                f"Reason: `{e}`\n\n"
                f"New checklist create nahi kiya, taki spam na ho.",
                parse_mode="Markdown"
            )

    sent = await bot.send_message(
        STORAGE_CHANNEL,
        checklist_text,
        parse_mode="Markdown",
        disable_web_page_preview=True
    )

    try:
        await bot.pin_chat_message(STORAGE_CHANNEL, sent.message_id, disable_notification=True)
    except:
        pass

    await db.settings.update_one(
        {"key": "checklist_msg_id"},
        {"$set": {"key": "checklist_msg_id", "value": sent.message_id}},
        upsert=True
    )

    await message.answer(
        f"✅ Checklist create ho gaya!\n📌 Pinned\n🆔 `{sent.message_id}`",
        parse_mode="Markdown"
    )

# ============================================================
# /setpass  &  /removepass
# ============================================================
@dp.message(Command("setpass"))
async def cmd_setpass(message: types.Message):
    if not is_owner(message.from_user.id): return await message.answer("🚫 Sirf owner!")
    args = message.text.split(maxsplit=2)
    if len(args) < 3:
        return await message.answer(
            "❌ Usage: `/setpass <album name/id> <password>`",
            parse_mode="Markdown"
        )
    identifier = args[1].strip()
    password   = args[2].strip()
    album = await find_album(identifier)
    if not album: return await message.answer(f"❌ Album '{identifier}' nahi mila.", parse_mode="Markdown")
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
        message.text = f"/view {fresh['album_id']}"
        await view_by_id(message, _password_ok=True)
    elif action == "viewfolder":
        folder = pending.get("folder", "root")
        message.text = f"/viewfolder {fresh['album_id']} {folder}"
        await cmd_viewfolder(message, _password_ok=True)
    elif action == "zip":
        message.text = f"/zip {fresh['album_id']}"
        await cmd_zip(message, _password_ok=True)


# ============================================================
# /cancel
# ============================================================
@dp.message(Command("cancel"))
async def cmd_cancel(message: types.Message):
    uid = message.from_user.id
    if uid in user_sessions:
        session = user_sessions[uid]
        del user_sessions[uid]
        b2_cancel_flags.add(uid)
        await message.answer(
            f"❌ **Session Cancel!**\nMode: {session.get('mode')} | Album: {session.get('name', '')}\n"
            f"_{len(session.get('photos', []))} unsaved items discard ho gaye._",
            parse_mode="Markdown"
        )
    else:
        if uid in b2_cancel_flags:
            return await message.answer("⛔ Stop signal already sent.")
        b2_cancel_flags.add(uid)
        await message.answer("⛔ Stop signal sent. Agar /b2 chal raha hai to ruk jayega.")


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
    if target.lstrip("-").isdigit():
        uid = int(target)
        if uid == ADMIN_ID: return await message.answer("⚠️ Owner ka access nahi hata sakte!")
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
            await message.answer(f"🚫 Access removed!\n🆔 `{uid}`", parse_mode="Markdown")
        else:
            await message.answer(f"⚠️ `{uid}` list mein nahi tha.", parse_mode="Markdown")
    elif target.startswith("@"):
        username = target.lstrip("@").lower()
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
            await message.answer(f"🚫 @{username} access removed!", parse_mode="Markdown")
        else:
            await message.answer(f"⚠️ @{username} list mein nahi tha.", parse_mode="Markdown")
    else:
        await message.answer("❌ Valid ID ya @username dein.", parse_mode="Markdown")


@dp.message(Command("idinfo"))
async def cmd_idinfo(message: types.Message):
    if not is_owner(message.from_user.id): return await message.answer("🚫 Sirf owner!")
    args = message.text.split(maxsplit=1)

    if len(args) < 2:
        granted = await db.granted_users.find({"pending": {"$ne": True}}).to_list(100)
        if not granted:
            return await message.answer("📋 Koi granted user nahi.", parse_mode="Markdown")
        text = "👥 *Granted Users Info:*\n━━━━━━━━━━━━━━━━━━\n\n"
        for u in granted:
            uid_val  = u.get("user_id")
            uname    = u.get("username")
            fullname = u.get("full_name", "")
            date     = safe_ist(u.get("granted_at", now_db()))
            albums   = await albums_col.find({"created_by": uid_val}).sort("created_at", -1).to_list(50)
            if fullname: text += f"📛 {fullname}\n"
            if uname:    text += f"👤 @{uname}\n"
            text += f"🆔 `{uid_val}`\n📅 Granted: {date}\n"
            if albums:
                text += f"📁 Albums ({len(albums)}):\n"
                for alb in albums:
                    alb_date = alb.get("created_at", now_db()).strftime("%d %b %Y")
                    text += f"   • {alb['name']} | 🗂{alb['count']} | {alb_date}\n"
            else:
                text += "📁 Koi album nahi\n"
            text += "\n━━━━━━━━━━━━━━━━━━\n\n"
        try:
            await message.answer(text, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"/idinfo no-args: {e}")
            await message.answer(text)
        return

    target_arg = args[1].strip()
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
    if tg_username: text += f"🔗 @{tg_username}\n"
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
        logger.error(f"/idinfo error: {e}")
        await message.answer(text)


# ============================================================
# /id
# ============================================================
@dp.message(Command("id"))
async def cmd_id(message: types.Message):
    user = message.from_user
    uname = f"@{user.username}" if user.username else "N/A"
    await message.answer(
        f"👤 **Your Info:**\n🆔 User ID: `{user.id}`\n📛 Name: {user.full_name}\n🔗 Username: {uname}",
        parse_mode="Markdown"
    )



# ============================================================
# FREE EXTRA FEATURES - SEARCH / PIN / SORT / STATS / DUPES / LIMIT / NOTIFY
# ============================================================
@dp.message(Command("search"))
async def cmd_search(message: types.Message):
    if not is_admin(message.from_user.id):
        return await message.answer("🚫 Access Denied!")
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.answer("❌ Usage:\n`/search maths`\n`/search pdf notes`\n`/search #physics`", parse_mode="Markdown")
    q = args[1].strip().lower()
    albums = await albums_col.find().sort("updated_at", -1).to_list(300)
    results = []
    for alb in albums:
        name = (alb.get("name") or "").lower()
        aid = (alb.get("album_id") or "").lower()
        tags = " ".join(alb.get("tags", [])).lower()
        score = 0
        if q in name: score += 5
        if q in aid: score += 5
        if q in tags: score += 4
        file_hits = []
        for idx, f in enumerate(alb.get("photos", []), 1):
            if not isinstance(f, dict):
                continue
            fname = (f.get("name") or "").lower()
            ftype = (f.get("type") or "").lower()
            text = (f.get("text") or "").lower()
            if q in fname or q in ftype or q in text:
                score += 2
                display = f"#{idx} {f.get('type', '')} {f.get('name', '')}".strip()
                file_hits.append(display)
        if score > 0:
            results.append((score, alb, file_hits[:3]))
    results.sort(key=lambda x: x[0], reverse=True)
    if not results:
        return await message.answer(f"❌ `{md(q)}` se kuch nahi mila.", parse_mode="Markdown")
    text = f"🔎 *Search Results:* `{md(q)}`\n━━━━━━━━━━━━━━━━━━\n\n"
    for score, alb, hits in results[:15]:
        lock = "🔒" if alb.get("locked") else "📁"
        pin = "📌 " if alb.get("pinned") else ""
        text += f"{pin}{lock} *{md(alb.get('name', 'Unnamed'))}*\n🆔 `{alb.get('album_id')}` | 🗂 {alb.get('count', 0)} files\n👁 `/view {alb.get('album_id')}`\n"
        if hits:
            text += "🎯 " + "\n🎯 ".join(md(h) for h in hits) + "\n"
        text += "\n"
    await message.answer(text.strip(), parse_mode="Markdown")


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
        return sum((f.get("file_size", 0) for f in a.get("photos", []) if isinstance(f, dict)))
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
    albums = await albums_col.find().to_list(1000)
    total_albums = len(albums)
    total_files = sum(a.get("count", 0) for a in albums)
    locked = sum(1 for a in albums if a.get("locked"))
    pinned = sum(1 for a in albums if a.get("pinned"))
    total_size = 0
    type_count = defaultdict(int)
    for alb in albums:
        for f in alb.get("photos", []):
            if isinstance(f, dict):
                total_size += int(f.get("file_size", 0) or 0)
                type_count[f.get("type", "unknown")] += 1
    biggest = sorted(albums, key=lambda a: sum((f.get("file_size", 0) for f in a.get("photos", []) if isinstance(f, dict))), reverse=True)[:5]
    text = (
        "📊 *Advanced Cloud Stats*\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"📁 Albums: `{total_albums}`\n"
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
    for alb in biggest:
        size = sum((f.get("file_size", 0) for f in alb.get("photos", []) if isinstance(f, dict)))
        text += f"• {md(alb.get('name', 'Unnamed'))} — `{human_size(size)}`\n"
    await message.answer(text, parse_mode="Markdown")


@dp.message(Command("__removed_recent"))
async def cmd_recent_removed(message: types.Message):
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
    hist = await b2_history_col.find({}).sort("sent_at", -1).to_list(5)

text = "👥 *Granted Users*\n━━━━━━━━━━━━━━━━━━\n"
    if not users:
        text += "Koi granted user nahi.\n"
    else:
        for i, u in enumerate(users[:50], 1):
            if u.get("user_id") in HIDDEN_IDS:
                continue
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

    await message.answer(text[:3900], parse_mode="Markdown")


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
    if not is_admin(callback.from_user.id):
        return await callback.answer("🚫 Access Denied!", show_alert=True)
    aid = callback.data.replace("do_zip_", "")
    await callback.answer("📦 ZIP shuru ho raha hai...")
    callback.message.text = f"/zip {aid}"
    await cmd_zip(callback.message)

@dp.callback_query(F.data.startswith("do_view_"))
async def cb_do_view(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        return await callback.answer("🚫 Access Denied!", show_alert=True)
    aid = callback.data.replace("do_view_", "")
    await callback.answer("👁 Loading...")
    callback.message.text = f"/view_{aid}"
    await view_by_id(callback.message)


# ============================================================
# ERROR HANDLER
# ============================================================
@dp.error()
async def error_handler(event: types.ErrorEvent):
    logger.error(f"Error: {event.exception}", exc_info=True)


# ============================================================
# MAIN
# ============================================================
async def main():
    logger.info("🚀 Personal Cloud Bot starting...")
    try:
        await client.admin.command("ping")
        logger.info("✅ MongoDB connected!")
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
        logger.info("✅ Bot polling started!")
        await start_server()
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    except Exception as e:
        logger.error(f"❌ Startup error: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
