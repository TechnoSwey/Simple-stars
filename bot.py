"""
ErneStars Bot — чат-бот.
Зависимости: aiogram, aiosqlite, aiohttp, python-dotenv, apscheduler
"""
import asyncio, logging, os, uuid, traceback
from time import time
from datetime import date, timedelta
from typing import Dict, Optional

from dotenv import load_dotenv
load_dotenv()

from aiogram import Bot, Dispatcher, Router, F, BaseMiddleware
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton, TelegramObject,
)
import aiohttp
import aiosqlite
from apscheduler.schedulers.asyncio import AsyncIOScheduler

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════
# КОНФИГУРАЦИЯ
# ══════════════════════════════════════════════════════════

BOT_TOKEN          = os.getenv("BOT_TOKEN", "")
ADMIN_ID           = int(os.getenv("ADMIN_ID", "0"))
ADMIN_USERNAME     = os.getenv("ADMIN_USERNAME", "@admin")
PRFLOW_API_KEY     = os.getenv("PRFLOW_API_KEY", "")
PRFLOW_API_URL     = os.getenv("PRFLOW_API_URL", "https://piarflow.com/api")
PAYOUT_CHANNEL_ID  = os.getenv("PAYOUT_CHANNEL_ID", "0")
PAYOUT_CHANNEL_URL = os.getenv("PAYOUT_CHANNEL_URL", "https://t.me/")
DB_PATH            = os.getenv("DB_PATH", "bot.db")

MIN_WITHDRAW       = float(os.getenv("MIN_WITHDRAW_STARS", "15"))
DEFAULT_TASK_STARS = float(os.getenv("DEFAULT_TASK_STARS", "5.0"))
DEFAULT_TASK_XP    = int(os.getenv("DEFAULT_TASK_XP", "50"))
PRICE_VIP          = float(os.getenv("PRICE_VIP_STARS", "500"))
PRICE_FREEZE       = float(os.getenv("PRICE_FREEZE_STARS", "50"))

# ══════════════════════════════════════════════════════════
# УРОВНИ
# ══════════════════════════════════════════════════════════

LEVEL_RANKS = [
    (0,    "Новичок"),
    (100,  "Ученик"),
    (300,  "Опытный"),
    (700,  "Мастер"),
    (1500, "Эксперт"),
    (3000, "Ветеран"),
    (6000, "Элита"),
    (10000,"Легенда"),
]
ACHIEVEMENTS = [
    ("first_task",   "🎯 Первое задание"),
    ("tasks_10",     "📋 10 заданий"),
    ("tasks_50",     "💪 50 заданий"),
    ("tasks_100",    "🏆 100 заданий"),
    ("referral_1",   "👥 Первый реферал"),
    ("referral_10",  "🤝 10 рефералов"),
    ("streak_7",     "🔥 7 дней подряд"),
]

def calculate_level_info(xp: int):
    rank, level_num = LEVEL_RANKS[0][1], 1
    for i, (req, r) in enumerate(LEVEL_RANKS):
        if xp >= req:
            rank, level_num = r, i + 1
    next_req = LEVEL_RANKS[level_num][0] if level_num < len(LEVEL_RANKS) else LEVEL_RANKS[-1][0]
    cur_req  = LEVEL_RANKS[level_num - 1][0]
    span     = next_req - cur_req or 1
    pct      = min(100, int((xp - cur_req) / span * 100))
    return rank, level_num, xp, next_req, pct

# ══════════════════════════════════════════════════════════
# БАЗА ДАННЫХ
# ══════════════════════════════════════════════════════════

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT, full_name TEXT,
            stars REAL DEFAULT 0,
            xp INTEGER DEFAULT 0,
            daily_streak INTEGER DEFAULT 0,
            last_daily_date TEXT,
            streak_protection INTEGER DEFAULT 0,
            referrer_id INTEGER,
            is_banned INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS vip (
            user_id INTEGER PRIMARY KEY,
            granted_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS tasks (
            task_hash TEXT PRIMARY KEY,
            channel_link TEXT,
            stars_reward REAL DEFAULT 5,
            xp_reward INTEGER DEFAULT 50
        );
        CREATE TABLE IF NOT EXISTS completions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER, task_hash TEXT,
            channel_link TEXT, stars_earned REAL, xp_earned INTEGER,
            completed_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, task_hash)
        );
        CREATE TABLE IF NOT EXISTS referrals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            referrer_id INTEGER, referred_id INTEGER UNIQUE,
            stars_earned REAL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS withdrawals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER, stars_amount REAL,
            status TEXT DEFAULT 'pending',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS promo_codes (
            code TEXT PRIMARY KEY,
            reward_stars REAL, reward_xp INTEGER,
            activations_left INTEGER
        );
        CREATE TABLE IF NOT EXISTS promo_activations (
            code TEXT, user_id INTEGER, PRIMARY KEY(code, user_id)
        );
        CREATE TABLE IF NOT EXISTS gifts (
            code TEXT PRIMARY KEY,
            reward_stars REAL,
            activations_left INTEGER
        );
        CREATE TABLE IF NOT EXISTS gift_activations (
            code TEXT, user_id INTEGER, PRIMARY KEY(code, user_id)
        );
        CREATE TABLE IF NOT EXISTS achievements (
            user_id INTEGER, key TEXT, PRIMARY KEY(user_id, key)
        );
        CREATE TABLE IF NOT EXISTS required_channels (
            channel_id TEXT PRIMARY KEY, title TEXT, url TEXT
        );
        CREATE TABLE IF NOT EXISTS task_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            requester_id INTEGER, channel_link TEXT,
            completions_total INTEGER, completions_done INTEGER DEFAULT 0,
            price_per_completion REAL, status TEXT DEFAULT 'pending_admin_check',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS monitored_completions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER, task_hash TEXT, channel_link TEXT,
            completed_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY, value TEXT
        );
        """)
        await db.commit()

async def q(sql, params=(), fetch="none"):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(sql, params)
        await db.commit()
        if fetch == "one":  return await cur.fetchone()
        if fetch == "all":  return await cur.fetchall()
        return cur.lastrowid

# shortcuts
async def get_user(uid):             return await q("SELECT * FROM users WHERE user_id=?", (uid,), "one")
async def is_banned(uid):            u = await get_user(uid); return bool(u and u["is_banned"])
async def is_vip(uid):               return bool(await q("SELECT 1 FROM vip WHERE user_id=?", (uid,), "one"))
async def get_setting(k, default=""): r = await q("SELECT value FROM settings WHERE key=?", (k,), "one"); return r["value"] if r else default
async def set_setting(k, v):         await q("INSERT OR REPLACE INTO settings(key,value) VALUES(?,?)", (k, v))

async def get_or_create_user(uid, uname, fname, ref_id=None):
    if not await get_user(uid):
        await q("INSERT OR IGNORE INTO users(user_id,username,full_name,referrer_id) VALUES(?,?,?,?)",
                (uid, uname, fname, ref_id))
        if ref_id and await get_user(ref_id):
            await q("INSERT OR IGNORE INTO referrals(referrer_id,referred_id) VALUES(?,?)", (ref_id, uid))
            vip_ref = await is_vip(ref_id)
            bonus = round(DEFAULT_TASK_STARS * (0.20 if vip_ref else 0.10), 2)
            await q("UPDATE users SET stars=stars+? WHERE user_id=?", (bonus, ref_id))
            await q("UPDATE referrals SET stars_earned=stars_earned+? WHERE referrer_id=? AND referred_id=?",
                    (bonus, ref_id, uid))

async def update_balance(uid, stars=0, xp=0):
    await q("UPDATE users SET stars=stars+?, xp=xp+? WHERE user_id=?", (stars, xp, uid))

async def task_completed(uid, task_hash): return bool(await q("SELECT 1 FROM completions WHERE user_id=? AND task_hash=?", (uid, task_hash), "one"))
async def save_completion(uid, task_hash, link, stars, xp):
    try:
        await q("INSERT INTO completions(user_id,task_hash,channel_link,stars_earned,xp_earned) VALUES(?,?,?,?,?)",
                (uid, task_hash, link, stars, xp))
        await update_balance(uid, stars, xp)
        await check_achievements(uid)
        return True
    except: return False

async def check_achievements(uid):
    unlocked = []
    user = await get_user(uid)
    if not user: return []
    done_count = (await q("SELECT COUNT(*) as c FROM completions WHERE user_id=?", (uid,), "one"))["c"]
    refs_count = (await q("SELECT COUNT(*) as c FROM referrals WHERE referrer_id=?", (uid,), "one"))["c"]
    mapping = {
        "first_task": done_count >= 1,
        "tasks_10":   done_count >= 10,
        "tasks_50":   done_count >= 50,
        "tasks_100":  done_count >= 100,
        "referral_1": refs_count >= 1,
        "referral_10":refs_count >= 10,
        "streak_7":   user["daily_streak"] >= 7,
    }
    existing = {r["key"] for r in (await q("SELECT key FROM achievements WHERE user_id=?", (uid,), "all") or [])}
    for key, cond in mapping.items():
        if cond and key not in existing:
            await q("INSERT OR IGNORE INTO achievements(user_id,key) VALUES(?,?)", (uid, key))
            label = next((l for k, l in ACHIEVEMENTS if k == key), key)
            unlocked.append(label)
    return unlocked

# ══════════════════════════════════════════════════════════
# PIARFLOW API
# ══════════════════════════════════════════════════════════

async def pf_get_tasks(uid, chat_id):
    try:
        async with aiohttp.ClientSession() as s:
            r = await s.get(f"{PRFLOW_API_URL}/tasks",
                            params={"api_key": PRFLOW_API_KEY, "user_id": uid, "chat_id": chat_id},
                            timeout=aiohttp.ClientTimeout(total=10))
            if r.status == 200:
                data = await r.json()
                stars = float(await get_setting("task_stars", str(DEFAULT_TASK_STARS)))
                xp    = int(await get_setting("task_xp", str(DEFAULT_TASK_XP)))
                return [{"link": t.get("link",""), "reward_stars": stars, "reward_xp": xp} for t in (data if isinstance(data, list) else [])]
    except Exception as e:
        log.warning(f"PiarFlow tasks error: {e}")
    return []

async def pf_check_sub(uid, link):
    try:
        async with aiohttp.ClientSession() as s:
            r = await s.post(f"{PRFLOW_API_URL}/check",
                             json={"api_key": PRFLOW_API_KEY, "user_id": uid, "link": link},
                             timeout=aiohttp.ClientTimeout(total=10))
            if r.status == 200:
                data = await r.json()
                return data.get("subscribed", False)
    except Exception as e:
        log.warning(f"PiarFlow check error: {e}")
    return False

# ══════════════════════════════════════════════════════════
# MIDDLEWARES
# ══════════════════════════════════════════════════════════

class FloodMiddleware(BaseMiddleware):
    def __init__(self, limit=0.8):
        self._ts: Dict[int, float] = {}
        self.limit = limit
    async def __call__(self, handler, event: TelegramObject, data):
        uid = getattr(getattr(event, "from_user", None), "id", None)
        if uid:
            now = time()
            if now - self._ts.get(uid, 0) < self.limit:
                if isinstance(event, CallbackQuery): await event.answer("⚠️ Не так быстро!")
                return
            self._ts[uid] = now
        return await handler(event, data)

class BanMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: TelegramObject, data):
        uid = getattr(getattr(event, "from_user", None), "id", None)
        if uid and await is_banned(uid):
            if isinstance(event, Message): await event.answer("🚫 Вы заблокированы.")
            elif isinstance(event, CallbackQuery): await event.answer("🚫 Заблокированы.", show_alert=True)
            return
        return await handler(event, data)

# ══════════════════════════════════════════════════════════
# FSM
# ══════════════════════════════════════════════════════════

class A(StatesGroup):  # Admin states
    search=State(); ban=State(); vip=State()
    bc_content=State(); bc_btn_text=State(); bc_btn_url=State()
    event_name=State(); event_mult=State()
    promo_code=State(); promo_stars=State(); promo_xp=State(); promo_limit=State()
    gift_stars=State(); gift_limit=State()
    task_stars=State(); task_xp=State()
    ch_id=State(); ch_title=State(); ch_url=State()
    add_task=State()

class U(StatesGroup):  # User states
    withdraw=State(); promo=State()
    sub_link=State(); sub_count=State(); sub_price=State()

# ══════════════════════════════════════════════════════════
# КЛАВИАТУРЫ
# ══════════════════════════════════════════════════════════

def main_kb():
    rows = [
        [KeyboardButton(text="💰 Заработать"), KeyboardButton(text="👤 Профиль")],
        [KeyboardButton(text="🛒 Магазин"),    KeyboardButton(text="📊 Рейтинг")],
        [KeyboardButton(text="📝 Свой канал"), KeyboardButton(text="💸 Вывести")],
        [KeyboardButton(text="ℹ️ О боте")],
    ]
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)

def wd_kb(): return InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="⭐ 15",callback_data="wd:15"),InlineKeyboardButton(text="⭐ 25",callback_data="wd:25")],
    [InlineKeyboardButton(text="⭐ 50",callback_data="wd:50"),InlineKeyboardButton(text="⭐ 100",callback_data="wd:100")],
    [InlineKeyboardButton(text="✏️ Своя сумма",callback_data="wd:custom")],
])

def admin_kb(): return InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="📊 Статистика",         callback_data="adm:stats")],
    [InlineKeyboardButton(text="📥 Заявки на вывод",    callback_data="adm:wd_list")],
    [InlineKeyboardButton(text="🎟 Создать гифт-чек",   callback_data="adm:gift")],
    [InlineKeyboardButton(text="🎉 Ивенты",             callback_data="adm:events")],
    [InlineKeyboardButton(text="📢 Обязат. каналы",     callback_data="adm:channels")],
    [InlineKeyboardButton(text="🎟 Создать промокод",   callback_data="adm:promo")],
    [InlineKeyboardButton(text="⚙️ Награды за задание", callback_data="adm:rewards")],
    [InlineKeyboardButton(text="🔍 Поиск пользователя", callback_data="adm:search")],
    [InlineKeyboardButton(text="🚫 Бан / Разбан",       callback_data="adm:ban")],
    [InlineKeyboardButton(text="👑 Выдать VIP",         callback_data="adm:vip")],
    [InlineKeyboardButton(text="📋 Заявки на каналы",   callback_data="adm:requests")],
    [InlineKeyboardButton(text="➕ Добавить задание",   callback_data="adm:add_task")],
    [InlineKeyboardButton(text="📢 Рассылка",           callback_data="adm:broadcast")],
])

# ══════════════════════════════════════════════════════════
# ПОДПИСКИ
# ══════════════════════════════════════════════════════════

async def check_subs(bot: Bot, uid: int):
    channels = await q("SELECT * FROM required_channels", fetch="all") or []
    unsub = []
    for ch in channels:
        cid = ch["channel_id"]
        target = int(cid) if cid.lstrip("-").isdigit() else cid
        try:
            m = await bot.get_chat_member(chat_id=target, user_id=uid)
            if m.status not in ("creator","administrator","member"):
                unsub.append(ch)
        except: unsub.append(ch)
    if not unsub:
        return True, InlineKeyboardMarkup(inline_keyboard=[])
    btns = [[InlineKeyboardButton(text=f"📢 {c['title']}", url=c["url"])] for c in unsub]
    btns.append([InlineKeyboardButton(text="✅ Проверить", callback_data="check_subs")])
    return False, InlineKeyboardMarkup(inline_keyboard=btns)

# ══════════════════════════════════════════════════════════
# РОУТЕР
# ══════════════════════════════════════════════════════════

r = Router()

# ─── /start ───────────────────────────────────────────────
@r.message(CommandStart())
async def start(msg: Message, command: CommandObject, bot: Bot):
    u = msg.from_user
    ref_id, gift_code = None, None
    if command.args:
        if command.args.startswith("gift_"): gift_code = command.args[5:]
        elif command.args.isdigit():          ref_id = int(command.args)
    await get_or_create_user(u.id, u.username, u.full_name, ref_id)
    ok, sub_kb = await check_subs(bot, u.id)
    if not ok:
        return await msg.answer("Подпишитесь на обязательные каналы:", reply_markup=sub_kb)
    if gift_code:
        gift = await q("SELECT * FROM gifts WHERE code=?", (gift_code,), "one")
        already = await q("SELECT 1 FROM gift_activations WHERE code=? AND user_id=?", (gift_code, u.id), "one")
        if not gift: await msg.answer("❌ Гифт-чек не найден.")
        elif gift["activations_left"] <= 0: await msg.answer("❌ Лимит активаций исчерпан.")
        elif already: await msg.answer("❌ Вы уже активировали этот чек.")
        else:
            await q("INSERT INTO gift_activations VALUES(?,?)", (gift_code, u.id))
            await q("UPDATE gifts SET activations_left=activations_left-1 WHERE code=?", (gift_code,))
            await update_balance(u.id, stars=gift["reward_stars"])
            await msg.answer(f"🎉 +{gift['reward_stars']} Stars получено!")
    event_msg = ""
    if await get_setting("event_active", "0") == "1":
        event_msg = f"\n\n🔥 <b>ИВЕНТ: {await get_setting('event_name')} x{await get_setting('event_multiplier')}</b>"
    await msg.answer(
        f"👋 Добро пожаловать, <b>{u.first_name}</b>!\n\n"
        f"⭐ Зарабатывайте Telegram Stars за задания и рефералов.\n"
        f"⚠️ За отписку от спонсоров в течение 7 дней баланс уменьшается!{event_msg}",
        reply_markup=main_kb(), parse_mode="HTML")

@r.callback_query(F.data == "check_subs")
async def cb_check_subs(cb: CallbackQuery, bot: Bot):
    ok, sub_kb = await check_subs(bot, cb.from_user.id)
    if ok:
        await cb.message.delete()
        await cb.message.answer("✅ Все подписки подтверждены!", reply_markup=main_kb())
    else:
        await cb.answer("❌ Подпишитесь на все каналы!", show_alert=True)
        await cb.message.edit_reply_markup(reply_markup=sub_kb)

# ─── О боте ───────────────────────────────────────────────
@r.message(F.text == "ℹ️ О боте")
async def about(msg: Message):
    total = (await q("SELECT COUNT(*) as c FROM users", fetch="one"))["c"]
    paid  = (await q("SELECT COALESCE(SUM(stars_amount),0) as s FROM withdrawals WHERE status='approved'", fetch="one"))["s"]
    await msg.answer(
        f"🤖 <b>ErneStars Bot</b>\n\n"
        f"👥 Пользователей: <code>{total}</code>\n"
        f"⭐ Выплачено: <code>{paid:.2f}</code> Stars\n"
        f"📌 Минимум вывода: <code>{MIN_WITHDRAW}</code> Stars\n\n"
        f"Поддержка: {ADMIN_USERNAME}",
        parse_mode="HTML")

# ─── Профиль ──────────────────────────────────────────────
@r.message(F.text == "👤 Профиль")
async def profile(msg: Message, bot: Bot):
    try:
        log.info(f"Профиль вызван пользователем {msg.from_user.id}")
        ok, sub_kb = await check_subs(bot, msg.from_user.id)
        if not ok: 
            return await msg.answer("Подпишитесь на обязательные каналы!", reply_markup=sub_kb)
        u = await get_user(msg.from_user.id)
        if not u:
            log.warning(f"Пользователь {msg.from_user.id} не найден в БД")
            return await msg.answer("❌ Ошибка: пользователь не найден. Напишите /start")
        rank, lnum, xp, nxp, pct = calculate_level_info(u["xp"])
        refs = (await q("SELECT COUNT(*) as c FROM referrals WHERE referrer_id=?", (u["user_id"],), "one"))["c"]
        achs = len(await q("SELECT key FROM achievements WHERE user_id=?", (u["user_id"],), "all") or [])
        vip_s = "👑 Активен" if await is_vip(u["user_id"]) else "❌ Нет"
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📅 Ежедневный бонус", callback_data="daily")],
            [InlineKeyboardButton(text="🎟 Промокод",          callback_data="promo_enter"),
             InlineKeyboardButton(text="🏆 Достижения",        callback_data="achievements")],
        ])
        await msg.answer(
            f"👤 <b>Профиль</b>\n\n"
            f"🆔 <code>{u['user_id']}</code> | @{u['username'] or '—'}\n"
            f"⭐ Stars: <code>{u['stars']:.2f}</code>\n"
            f"🏆 Ур. {lnum} — {rank}\n"
            f"⚡ XP: <code>{xp}/{nxp}</code> ({pct}%)\n"
            f"👥 Рефералов: <code>{refs}</code>\n"
            f"🎖 Достижений: <code>{achs}</code>\n"
            f"🔥 Streak: <code>{u['daily_streak']}</code> дн.\n"
            f"🛡 Защита: <code>{u['streak_protection'] or 0}</code> шт.\n"
            f"👑 VIP: {vip_s}",
            reply_markup=kb, parse_mode="HTML")
    except Exception as e:
        log.error(f"ОШИБКА В ПРОФИЛЕ: {e}")
        log.error(traceback.format_exc())
        await msg.answer(f"❌ Ошибка: {e}")

# ─── Ежедневный бонус ─────────────────────────────────────
@r.callback_query(F.data == "daily")
async def daily(cb: CallbackQuery):
    uid = cb.from_user.id
    u = await get_user(uid)
    if not u: return
    today = date.today().isoformat()
    if u["last_daily_date"] == today:
        return await cb.answer("❌ Уже забирали сегодня!", show_alert=True)
    streak = u["daily_streak"]
    protect_used = False
    if u["last_daily_date"]:
        prev = date.fromisoformat(u["last_daily_date"])
        if prev == date.today() - timedelta(days=1): streak += 1
        elif u["streak_protection"] > 0:
            protect_used = True
            await q("UPDATE users SET streak_protection=streak_protection-1 WHERE user_id=?", (uid,))
            streak += 1
        else: streak = 1
    else: streak = 1
    stars = round(1.0 + streak * 0.1, 2)
    xp    = 15 + streak * 2
    if await get_setting("event_active", "0") == "1":
        mult = float(await get_setting("event_multiplier", "1"))
        stars = round(stars * mult, 2); xp = int(xp * mult)
    if await is_vip(uid): stars = round(stars * 1.5, 2); xp = int(xp * 1.5)
    await q("UPDATE users SET stars=stars+?,xp=xp+?,daily_streak=?,last_daily_date=? WHERE user_id=?",
            (stars, xp, streak, today, uid))
    protect_msg = "\n🛡 Streak спасён защитой!" if protect_used else ""
    await cb.message.edit_text(
        f"🎁 <b>Ежедневный бонус!</b>\n\n+{stars} Stars | +{xp} XP\n🔥 Streak: {streak} дн.{protect_msg}",
        parse_mode="HTML")
    await cb.answer()

# ─── Достижения ───────────────────────────────────────────
@r.callback_query(F.data == "achievements")
async def achievements(cb: CallbackQuery):
    rows = await q("SELECT key FROM achievements WHERE user_id=?", (cb.from_user.id,), "all") or []
    unlocked = {r["key"] for r in rows}
    text = "🏆 <b>Достижения:</b>\n\n"
    for key, title in ACHIEVEMENTS:
        text += f"{'✅' if key in unlocked else '🔒'} {title}\n"
    await cb.message.answer(text, parse_mode="HTML")
    await cb.answer()

# ─── Промокод (пользователь) ──────────────────────────────
@r.callback_query(F.data == "promo_enter")
async def promo_enter(cb: CallbackQuery, state: FSMContext):
    await state.set_state(U.promo)
    await cb.message.answer("🎟 Введите промокод:")
    await cb.answer()

@r.message(U.promo)
async def promo_use(msg: Message, state: FSMContext):
    await state.clear()
    code = msg.text.strip().upper()
    uid = msg.from_user.id
    promo = await q("SELECT * FROM promo_codes WHERE code=?", (code,), "one")
    if not promo: return await msg.answer("❌ Промокод не найден.")
    if promo["activations_left"] <= 0: return await msg.answer("❌ Лимит исчерпан.")
    if await q("SELECT 1 FROM promo_activations WHERE code=? AND user_id=?", (code, uid), "one"):
        return await msg.answer("❌ Уже активировали.")
    await q("INSERT INTO promo_activations VALUES(?,?)", (code, uid))
    await q("UPDATE promo_codes SET activations_left=activations_left-1 WHERE code=?", (code,))
    await update_balance(uid, promo["reward_stars"], promo["reward_xp"])
    await msg.answer(f"🎉 +{promo['reward_stars']} Stars | +{promo['reward_xp']} XP", parse_mode="HTML")

# ─── Заработок ────────────────────────────────────────────
@r.message(F.text == "💰 Заработать")
async def earn(msg: Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 Задания",       callback_data="tasks")],
        [InlineKeyboardButton(text="👥 Рефералы",      callback_data="refs")],
    ])
    await msg.answer("💰 <b>Выберите способ заработка:</b>", reply_markup=kb, parse_mode="HTML")

@r.callback_query(F.data == "tasks")
async def cb_tasks(cb: CallbackQuery, bot: Bot):
    ok, sub_kb = await check_subs(bot, cb.from_user.id)
    if not ok: await cb.message.answer("Подпишитесь на каналы!", reply_markup=sub_kb); return await cb.answer()
    uid = cb.from_user.id
    tasks = await pf_get_tasks(uid, cb.message.chat.id)
    avail = [t for t in tasks if not await task_completed(uid, str(hash(t["link"])))]
    if not avail: return await cb.message.answer("🎉 Все задания выполнены! Заходите позже.")
    t = avail[0]
    th = str(hash(t["link"]))
    await q("INSERT OR IGNORE INTO tasks(task_hash,channel_link,stars_reward,xp_reward) VALUES(?,?,?,?)",
            (th, t["link"], t["reward_stars"], t["reward_xp"]))
    event_txt = ""
    if await get_setting("event_active", "0") == "1":
        event_txt = f" (🔥 x{await get_setting('event_multiplier')})"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔗 Перейти", url=t["link"])],
        [InlineKeyboardButton(text="✅ Проверить", callback_data=f"ct:{th}:{t['reward_stars']}:{t['reward_xp']}")],
    ])
    await cb.message.answer(
        f"📢 Подпишитесь на канал:{event_txt}\n\n⭐ <code>{t['reward_stars']}</code> Stars | ⚡ <code>{t['reward_xp']}</code> XP",
        reply_markup=kb, parse_mode="HTML")
    await cb.answer()

@r.callback_query(F.data.startswith("ct:"))
async def check_task(cb: CallbackQuery):
    _, th, stars_s, xp_s = cb.data.split(":")
    uid = cb.from_user.id
    if await task_completed(uid, th):
        return await cb.answer("❌ Уже получили награду!", show_alert=True)
    task = await q("SELECT * FROM tasks WHERE task_hash=?", (th,), "one")
    if not task: return await cb.answer("❌ Задание не найдено.", show_alert=True)
    stars = float(stars_s); xp = int(xp_s)
    if await get_setting("event_active", "0") == "1":
        mult = float(await get_setting("event_multiplier", "1"))
        stars = round(stars * mult, 2); xp = int(xp * mult)
    if await is_vip(uid): stars = round(stars * 1.5, 2); xp = int(xp * 1.5)
    ok = await pf_check_sub(uid, task["channel_link"])
    if ok:
        unlocked = await save_completion(uid, th, task["channel_link"], stars, xp) and await check_achievements(uid)
        msg = f"🎉 Выполнено! +{stars} Stars | +{xp} XP"
        if unlocked: msg += f"\n\n🏆 {', '.join(unlocked)}"
        await cb.message.edit_text(msg)
    else:
        await cb.answer("❌ Не подписаны. Подпишитесь и попробуйте снова.", show_alert=True)

# ─── Рефералы ─────────────────────────────────────────────
@r.callback_query(F.data == "refs")
async def refs(cb: CallbackQuery, bot: Bot):
    info = await bot.get_me()
    link = f"https://t.me/{info.username}?start={cb.from_user.id}"
    cnt  = (await q("SELECT COUNT(*) as c FROM referrals WHERE referrer_id=?", (cb.from_user.id,), "one"))["c"]
    earn = (await q("SELECT COALESCE(SUM(stars_earned),0) as s FROM referrals WHERE referrer_id=?", (cb.from_user.id,), "one"))["s"]
    pct = "20%" if await is_vip(cb.from_user.id) else "10%"
    await cb.message.answer(
        f"👥 <b>Реферальная программа</b>\n\n"
        f"🔗 <code>{link}</code>\n\n"
        f"▪ Приглашено: <code>{cnt}</code> | Заработано: <code>{earn:.2f}</code> Stars\n"
        f"▪ Ваш %: <b>{pct}</b>\n\n"
        f"10 друзей → +10⭐ | 25 → +30⭐ | 50 → +75⭐ | 100 → +200⭐",
        parse_mode="HTML")
    await cb.answer()

# ─── Магазин ──────────────────────────────────────────────
@r.message(F.text == "🛒 Магазин")
async def shop(msg: Message):
    u = await get_user(msg.from_user.id)
    if not u: return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"👑 VIP ({PRICE_VIP} Stars)", callback_data="buy:vip")],
        [InlineKeyboardButton(text=f"🛡 Защита streak ({PRICE_FREEZE} Stars)", callback_data="buy:freeze")],
    ])
    await msg.answer(
        f"🛒 <b>Магазин</b>\n\nВаш баланс: <code>{u['stars']:.2f}</code> Stars\n\n"
        f"👑 VIP: +50% ко всем наградам, 20% с рефералов\n"
        f"🛡 Streak Freeze: защита серии от сброса",
        reply_markup=kb, parse_mode="HTML")

@r.callback_query(F.data.startswith("buy:"))
async def buy(cb: CallbackQuery):
    uid = cb.from_user.id; item = cb.data.split(":")[1]
    u = await get_user(uid)
    if item == "vip":
        if await is_vip(uid): return await cb.answer("👑 У вас уже есть VIP!", show_alert=True)
        if u["stars"] < PRICE_VIP: return await cb.answer(f"❌ Нужно {PRICE_VIP} Stars", show_alert=True)
        await update_balance(uid, -PRICE_VIP)
        await q("INSERT OR IGNORE INTO vip VALUES(?)", (uid,))
        await cb.message.edit_text("🎉 <b>VIP активирован!</b>", parse_mode="HTML")
    elif item == "freeze":
        if u["stars"] < PRICE_FREEZE: return await cb.answer(f"❌ Нужно {PRICE_FREEZE} Stars", show_alert=True)
        await update_balance(uid, -PRICE_FREEZE)
        await q("UPDATE users SET streak_protection=streak_protection+1 WHERE user_id=?", (uid,))
        await cb.message.edit_text("🛡 <b>Защита streak куплена!</b>", parse_mode="HTML")
    await cb.answer()

# ─── Рейтинг ──────────────────────────────────────────────
@r.message(F.text == "📊 Рейтинг")
async def rating(msg: Message):
    rows = await q("SELECT full_name, stars FROM users ORDER BY stars DESC LIMIT 10", fetch="all") or []
    text = "📊 <b>Топ 10 по Stars:</b>\n\n"
    for i, row in enumerate(rows, 1):
        text += f"{i}. {row['full_name'] or 'Аноним'} — <code>{row['stars']:.2f}</code> Stars\n"
    await msg.answer(text, parse_mode="HTML")

# ─── Вывод ────────────────────────────────────────────────
@r.message(F.text == "💸 Вывести")
@r.callback_query(F.data == "open_wd")
async def show_withdraw(event, **_):
    msg = event if isinstance(event, Message) else event.message
    uid = event.from_user.id if isinstance(event, Message) else event.from_user.id
    u = await get_user(uid)
    if not u: return
    await msg.answer(
        f"💸 <b>Вывод Stars</b>\n\nБаланс: <code>{u['stars']:.2f}</code>\nМинимум: <code>{MIN_WITHDRAW}</code>",
        reply_markup=wd_kb(), parse_mode="HTML")
    if isinstance(event, CallbackQuery): await event.answer()

@r.callback_query(F.data.startswith("wd:"))
async def wd_preset(cb: CallbackQuery, state: FSMContext, bot: Bot):
    val = cb.data.split(":")[1]
    if val == "custom":
        await state.set_state(U.withdraw)
        await cb.message.answer(f"✏️ Введите сумму (мин. {MIN_WITHDRAW}):")
        return await cb.answer()
    await do_withdraw(cb.message, cb.from_user, float(val), bot)
    await cb.answer()

@r.message(U.withdraw)
async def wd_custom(msg: Message, state: FSMContext, bot: Bot):
    await state.clear()
    try: amt = float(msg.text.replace(",", "."))
    except: return await msg.answer("❌ Введите число.")
    await do_withdraw(msg, msg.from_user, amt, bot)

async def do_withdraw(msg_obj: Message, user_obj, amount: float, bot: Bot):
    if amount < MIN_WITHDRAW: return await msg_obj.answer(f"❌ Минимум {MIN_WITHDRAW} Stars.")
    u = await get_user(user_obj.id)
    if not u or u["stars"] < amount: return await msg_obj.answer("❌ Недостаточно Stars.")
    await update_balance(user_obj.id, -amount)
    wid = await q("INSERT INTO withdrawals(user_id,stars_amount) VALUES(?,?)", (user_obj.id, amount))
    await msg_obj.answer(f"🟢 Заявка #{wid:06d} создана!\n⭐ {amount} Stars · 🟡 Ожидает подтверждения")
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🟢 Принять", callback_data=f"wa:{wid}"),
        InlineKeyboardButton(text="🔴 Отклонить", callback_data=f"wr:{wid}"),
    ]])
    try: await bot.send_message(ADMIN_ID,
        f"📥 Заявка #{wid:06d}\n👤 {user_obj.full_name} (<code>{user_obj.id}</code>)\n⭐ {amount} Stars",
        reply_markup=kb, parse_mode="HTML")
    except Exception as e: log.error(e)

@r.callback_query(F.data.startswith("wa:"))
async def wd_approve(cb: CallbackQuery, bot: Bot):
    if cb.from_user.id != ADMIN_ID: return
    wid = int(cb.data.split(":")[1])
    wd = await q("SELECT * FROM withdrawals WHERE id=?", (wid,), "one")
    if not wd or wd["status"] != "pending": return await cb.answer("Уже обработано.", show_alert=True)
    await q("UPDATE withdrawals SET status='approved' WHERE id=?", (wid,))
    try: await bot.send_message(wd["user_id"], f"🟢 Заявка #{wid:06d} одобрена! ⭐ {wd['stars_amount']} Stars выплачены.")
    except: pass
    if PAYOUT_CHANNEL_ID and PAYOUT_CHANNEL_ID != "0":
        try:
            u = await get_user(wd["user_id"])
            await bot.send_message(int(PAYOUT_CHANNEL_ID),
                f"🎉 Выплата!\n👤 {u['full_name'] if u else wd['user_id']}\n⭐ {wd['stars_amount']} Stars")
        except Exception as e: log.error(e)
    await cb.message.edit_text(f"🟢 Вывод #{wid:06d} подтверждён.")

@r.callback_query(F.data.startswith("wr:"))
async def wd_reject(cb: CallbackQuery, bot: Bot):
    if cb.from_user.id != ADMIN_ID: return
    wid = int(cb.data.split(":")[1])
    wd = await q("SELECT * FROM withdrawals WHERE id=?", (wid,), "one")
    if not wd or wd["status"] != "pending": return await cb.answer("Уже обработано.", show_alert=True)
    await q("UPDATE withdrawals SET status='rejected' WHERE id=?", (wid,))
    await update_balance(wd["user_id"], wd["stars_amount"])
    try: await bot.send_message(wd["user_id"], f"🔴 Заявка #{wid:06d} отклонена. Stars возвращены.")
    except: pass
    await cb.message.edit_text(f"🔴 Вывод #{wid:06d} отклонён.")

# ─── Добавить свой канал ──────────────────────────────────
@r.message(F.text == "📝 Свой канал")
async def submit_start(msg: Message, state: FSMContext):
    await state.set_state(U.sub_link)
    await msg.answer("🔗 Ссылка на ваш канал (https://t.me/...):")

@r.message(U.sub_link)
async def sub_link(msg: Message, state: FSMContext):
    if not msg.text.startswith(("https://t.me/", "t.me/")):
        return await msg.answer("❌ Введите корректную ссылку:")
    await state.update_data(link=msg.text.strip())
    await state.set_state(U.sub_count)
    await msg.answer(f"✅ {msg.text}\n\nКоличество подписчиков:")

@r.message(U.sub_count)
async def sub_count(msg: Message, state: FSMContext):
    if not msg.text.isdigit() or not (1 <= int(msg.text) <= 10000):
        return await msg.answer("❌ Число от 1 до 10000:")
    await state.update_data(count=int(msg.text))
    await state.set_state(U.sub_price)
    await msg.answer("💰 Stars за одного подписчика (например: 1.0):")

@r.message(U.sub_price)
async def sub_price(msg: Message, state: FSMContext, bot: Bot):
    try:
        price = float(msg.text.replace(",", "."))
        if not (0.1 <= price <= 1000): raise ValueError
    except: return await msg.answer("❌ Число от 0.1 до 1000:")
    data = await state.get_data(); await state.clear()
    total = round(data["count"] * price, 2)
    u = await get_user(msg.from_user.id)
    if not u or u["stars"] < total:
        return await msg.answer(f"❌ Нужно {total} Stars, у вас {u['stars']:.2f}.")
    await update_balance(msg.from_user.id, -total)
    rid = await q("INSERT INTO task_requests(requester_id,channel_link,completions_total,price_per_completion) VALUES(?,?,?,?)",
                  (msg.from_user.id, data["link"], data["count"], price))
    info = await bot.get_me()
    await msg.answer(
        f"📝 Заявка #{rid} создана! Списано {total} ⭐\n\n"
        f"Добавьте @{info.username} администратором в канал для верификации.")
    try:
        await bot.send_message(ADMIN_ID,
            f"📝 Заявка #{rid}\n🔗 {data['link']}\n{data['count']} × {price} ⭐ = {total} ⭐\n👤 {msg.from_user.id}")
    except: pass

# ─── Администратор /admin ──────────────────────────────────
@r.message(Command("admin"))
async def admin_cmd(msg: Message):
    if msg.from_user.id != ADMIN_ID: return
    await msg.answer("🛠 <b>Панель администратора:</b>", reply_markup=admin_kb(), parse_mode="HTML")

@r.callback_query(F.data == "adm:back")
async def adm_back(cb: CallbackQuery):
    if cb.from_user.id != ADMIN_ID: return
    await cb.message.edit_text("🛠 <b>Панель:</b>", reply_markup=admin_kb(), parse_mode="HTML")

@r.callback_query(F.data == "adm:stats")
async def adm_stats(cb: CallbackQuery):
    if cb.from_user.id != ADMIN_ID: return
    total = (await q("SELECT COUNT(*) as c FROM users", fetch="one"))["c"]
    vips  = (await q("SELECT COUNT(*) as c FROM vip", fetch="one"))["c"]
    paid  = (await q("SELECT COALESCE(SUM(stars_amount),0) as s FROM withdrawals WHERE status='approved'", fetch="one"))["s"]
    pend  = (await q("SELECT COUNT(*) as c FROM withdrawals WHERE status='pending'", fetch="one"))["c"]
    ev = "🟢" if await get_setting("event_active","0")=="1" else "🔴"
    await cb.message.edit_text(
        f"📊 <b>Статистика</b>\n\n"
        f"👥 Пользователей: <code>{total}</code>\n👑 VIP: <code>{vips}</code>\n"
        f"⭐ Выплачено: <code>{paid:.2f}</code>\n🟡 Ожидает: <code>{pend}</code>\n"
        f"🎉 Ивент: {ev}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад",callback_data="adm:back")]]),
        parse_mode="HTML")

@r.callback_query(F.data == "adm:wd_list")
async def adm_wd_list(cb: CallbackQuery):
    if cb.from_user.id != ADMIN_ID: return
    items = await q("SELECT w.*,u.full_name FROM withdrawals w LEFT JOIN users u ON w.user_id=u.user_id WHERE w.status='pending' LIMIT 10", fetch="all") or []
    if not items:
        return await cb.message.edit_text("📥 Нет ожидающих заявок.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад",callback_data="adm:back")]]))
    text = "📥 <b>Ожидают:</b>\n\n"; btns = []
    for it in items:
        text += f"#{it['id']:06d} — {it['full_name'] or it['user_id']} | ⭐{it['stars_amount']}\n"
        btns.append([InlineKeyboardButton(text=f"🟢#{it['id']}",callback_data=f"wa:{it['id']}"),
                     InlineKeyboardButton(text=f"🔴#{it['id']}",callback_data=f"wr:{it['id']}")])
    btns.append([InlineKeyboardButton(text="🔙 Назад",callback_data="adm:back")])
    await cb.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=btns), parse_mode="HTML")

# гифт-чек
@r.callback_query(F.data == "adm:gift")
async def adm_gift(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id != ADMIN_ID: return
    await state.set_state(A.gift_stars); await cb.message.answer("⭐ Stars в гифт-чеке:"); await cb.answer()

@r.message(A.gift_stars)
async def adm_gift_stars(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_ID: return
    try: await state.update_data(gs=float(msg.text)); await state.set_state(A.gift_limit); await msg.answer("👥 Лимит активаций:")
    except: await msg.answer("❌ Число!")

@r.message(A.gift_limit)
async def adm_gift_limit(msg: Message, state: FSMContext, bot: Bot):
    if msg.from_user.id != ADMIN_ID: return
    if not msg.text.isdigit(): return await msg.answer("❌ Целое число!")
    data = await state.get_data(); await state.clear()
    code = uuid.uuid4().hex[:8].upper()
    await q("INSERT INTO gifts VALUES(?,?,?)", (code, data["gs"], int(msg.text)))
    info = await bot.get_me()
    await msg.answer(f"🎟 Гифт-чек: <code>{code}</code>\n⭐{data['gs']} | 👥{msg.text}\n"
                     f"🔗 <code>https://t.me/{info.username}?start=gift_{code}</code>", parse_mode="HTML")

# ивенты
@r.callback_query(F.data == "adm:events")
async def adm_events(cb: CallbackQuery):
    if cb.from_user.id != ADMIN_ID: return
    active = await get_setting("event_active","0")=="1"
    name = await get_setting("event_name","—"); mult = await get_setting("event_multiplier","1")
    status = f"🟢 {name} x{mult}" if active else "🔴 Выключен"
    btns = [[InlineKeyboardButton(text="🔴 Остановить" if active else "🚀 Запустить",
             callback_data="adm:event_stop" if active else "adm:event_start")],
            [InlineKeyboardButton(text="🔙 Назад",callback_data="adm:back")]]
    await cb.message.edit_text(f"🎉 <b>Ивенты</b>\n\n{status}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=btns), parse_mode="HTML")

@r.callback_query(F.data == "adm:event_stop")
async def adm_event_stop(cb: CallbackQuery):
    if cb.from_user.id != ADMIN_ID: return
    await set_setting("event_active","0"); await cb.answer("✅ Ивент остановлен!", show_alert=True); await adm_events(cb)

@r.callback_query(F.data == "adm:event_start")
async def adm_event_start(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id != ADMIN_ID: return
    await state.set_state(A.event_name); await cb.message.answer("📌 Название ивента:"); await cb.answer()

@r.message(A.event_name)
async def adm_event_name(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_ID: return
    await state.update_data(en=msg.text); await state.set_state(A.event_mult); await msg.answer("⚡ Множитель (например 2.0):")

@r.message(A.event_mult)
async def adm_event_mult(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_ID: return
    try:
        mult = float(msg.text.replace(",","."))
        data = await state.get_data(); await state.clear()
        await set_setting("event_active","1"); await set_setting("event_name",data["en"]); await set_setting("event_multiplier",str(mult))
        await msg.answer(f"🚀 Ивент запущен! {data['en']} x{mult}")
    except: await msg.answer("❌ Число!")

# обязательные каналы
@r.callback_query(F.data == "adm:channels")
async def adm_channels(cb: CallbackQuery):
    if cb.from_user.id != ADMIN_ID: return
    chs = await q("SELECT * FROM required_channels", fetch="all") or []
    text = "📢 <b>Обязат. каналы:</b>\n\n" + ("\n".join(f"▪ {c['title']} ({c['channel_id']})" for c in chs) or "Пусто")
    btns = [[InlineKeyboardButton(text="➕ Добавить",callback_data="adm:ch_add")]]
    if chs: btns.append([InlineKeyboardButton(text="🗑 Удалить",callback_data="adm:ch_del")])
    btns.append([InlineKeyboardButton(text="🔙 Назад",callback_data="adm:back")])
    await cb.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=btns), parse_mode="HTML")

@r.callback_query(F.data == "adm:ch_add")
async def adm_ch_add(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id != ADMIN_ID: return
    await state.set_state(A.ch_id); await cb.message.answer("ID канала (@username или -100...):\n(Бот должен быть администратором!)"); await cb.answer()

@r.message(A.ch_id)
async def adm_ch_id(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_ID: return
    await state.update_data(cid=msg.text.strip()); await state.set_state(A.ch_title); await msg.answer("📝 Название:")

@r.message(A.ch_title)
async def adm_ch_title(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_ID: return
    await state.update_data(ct=msg.text.strip()); await state.set_state(A.ch_url); await msg.answer("🔗 Ссылка:")

@r.message(A.ch_url)
async def adm_ch_url(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_ID: return
    data = await state.get_data(); await state.clear()
    await q("INSERT OR REPLACE INTO required_channels VALUES(?,?,?)", (data["cid"], data["ct"], msg.text.strip()))
    await msg.answer(f"✅ Канал {data['ct']} добавлен!")

@r.callback_query(F.data == "adm:ch_del")
async def adm_ch_del(cb: CallbackQuery):
    if cb.from_user.id != ADMIN_ID: return
    chs = await q("SELECT * FROM required_channels", fetch="all") or []
    btns = [[InlineKeyboardButton(text=f"❌ {c['title']}", callback_data=f"adm:ch_rm:{c['channel_id']}")] for c in chs]
    btns.append([InlineKeyboardButton(text="🔙 Назад",callback_data="adm:channels")])
    await cb.message.edit_text("🗑 Выберите для удаления:", reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))

@r.callback_query(F.data.startswith("adm:ch_rm:"))
async def adm_ch_rm(cb: CallbackQuery):
    if cb.from_user.id != ADMIN_ID: return
    cid = cb.data.split(":",2)[2]
    await q("DELETE FROM required_channels WHERE channel_id=?", (cid,))
    await cb.answer("✅ Удалено!", show_alert=True); await adm_channels(cb)

# промокоды (admin)
@r.callback_query(F.data == "adm:promo")
async def adm_promo(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id != ADMIN_ID: return
    await state.set_state(A.promo_code); await cb.message.answer("🎟 Текст промокода:"); await cb.answer()

@r.message(A.promo_code)
async def adm_pc(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_ID: return
    await state.update_data(pc=msg.text.strip().upper()); await state.set_state(A.promo_stars); await msg.answer("⭐ Stars:")

@r.message(A.promo_stars)
async def adm_ps(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_ID: return
    try: await state.update_data(ps=float(msg.text)); await state.set_state(A.promo_xp); await msg.answer("⚡ XP:")
    except: await msg.answer("❌ Число!")

@r.message(A.promo_xp)
async def adm_px(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_ID: return
    if not msg.text.isdigit(): return await msg.answer("❌ Целое!")
    await state.update_data(px=int(msg.text)); await state.set_state(A.promo_limit); await msg.answer("👥 Лимит:")

@r.message(A.promo_limit)
async def adm_pl(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_ID: return
    if not msg.text.isdigit(): return await msg.answer("❌ Целое!")
    data = await state.get_data(); await state.clear()
    await q("INSERT OR REPLACE INTO promo_codes VALUES(?,?,?,?)", (data["pc"], data["ps"], data["px"], int(msg.text)))
    await msg.answer(f"✅ Промокод <code>{data['pc']}</code> создан! ⭐{data['ps']} | ⚡{data['px']} | 👥{msg.text}", parse_mode="HTML")

# награды
@r.callback_query(F.data == "adm:rewards")
async def adm_rewards(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id != ADMIN_ID: return
    await state.set_state(A.task_stars); await cb.message.answer("⭐ Stars за задание:"); await cb.answer()

@r.message(A.task_stars)
async def adm_ts(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_ID: return
    try: await state.update_data(ts=float(msg.text)); await state.set_state(A.task_xp); await msg.answer("⚡ XP:")
    except: await msg.answer("❌ Число!")

@r.message(A.task_xp)
async def adm_tx(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_ID: return
    if not msg.text.isdigit(): return await msg.answer("❌ Целое!")
    data = await state.get_data(); await state.clear()
    await set_setting("task_stars", str(data["ts"])); await set_setting("task_xp", msg.text)
    await msg.answer(f"✅ Награды: ⭐{data['ts']} | ⚡{msg.text}")

# добавить задание вручную
@r.callback_query(F.data == "adm:add_task")
async def adm_add_task(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id != ADMIN_ID: return
    await state.set_state(A.add_task); await cb.message.answer("🔗 Ссылка на канал:"); await cb.answer()

@r.message(A.add_task)
async def adm_add_task_link(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_ID: return
    link = msg.text.strip(); await state.clear()
    stars = float(await get_setting("task_stars", str(DEFAULT_TASK_STARS)))
    xp    = int(await get_setting("task_xp", str(DEFAULT_TASK_XP)))
    th = str(hash(link))
    await q("INSERT OR REPLACE INTO tasks VALUES(?,?,?,?)", (th, link, stars, xp))
    await msg.answer(f"✅ Задание добавлено!\n🔗 {link}\n⭐{stars} | ⚡{xp}")

# поиск
@r.callback_query(F.data == "adm:search")
async def adm_search(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id != ADMIN_ID: return
    await state.set_state(A.search); await cb.message.answer("🔍 ID или @username:"); await cb.answer()

@r.message(A.search)
async def adm_search_res(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_ID: return
    await state.clear()
    q_text = msg.text.strip().lstrip("@")
    u = await q("SELECT * FROM users WHERE user_id=? OR username=?", (q_text, q_text), "one")
    if not u: return await msg.answer("❌ Не найден.")
    rank, lnum, xp, _, _ = calculate_level_info(u["xp"])
    await msg.answer(
        f"👤 <code>{u['user_id']}</code> | @{u['username'] or '—'}\n"
        f"⭐{u['stars']:.2f} | Ур.{lnum} {rank}\n"
        f"VIP: {'Да' if await is_vip(u['user_id']) else 'Нет'} | "
        f"Бан: {'Да' if u['is_banned'] else 'Нет'}",
        parse_mode="HTML")

# бан
@r.callback_query(F.data == "adm:ban")
async def adm_ban(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id != ADMIN_ID: return
    await state.set_state(A.ban); await cb.message.answer("🚫 ID для бан/разбан:"); await cb.answer()

@r.message(A.ban)
async def adm_ban_res(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_ID: return
    await state.clear()
    u = await q("SELECT * FROM users WHERE user_id=? OR username=?", (msg.text.strip().lstrip("@"),)*2, "one")
    if not u: return await msg.answer("❌ Не найден.")
    new_ban = 0 if u["is_banned"] else 1
    await q("UPDATE users SET is_banned=? WHERE user_id=?", (new_ban, u["user_id"]))
    await msg.answer(f"✅ {u['full_name']} {'заблокирован' if new_ban else 'разблокирован'}.")

# VIP
@r.callback_query(F.data == "adm:vip")
async def adm_vip(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id != ADMIN_ID: return
    await state.set_state(A.vip); await cb.message.answer("👑 ID для выдачи/снятия VIP:"); await cb.answer()

@r.message(A.vip)
async def adm_vip_res(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_ID: return
    await state.clear()
    u = await q("SELECT * FROM users WHERE user_id=? OR username=?", (msg.text.strip().lstrip("@"),)*2, "one")
    if not u: return await msg.answer("❌ Не найден.")
    if await is_vip(u["user_id"]):
        await q("DELETE FROM vip WHERE user_id=?", (u["user_id"],)); await msg.answer(f"❌ VIP снят с {u['full_name']}.")
    else:
        await q("INSERT OR IGNORE INTO vip VALUES(?)", (u["user_id"],)); await msg.answer(f"👑 VIP выдан {u['full_name']}!")

# заявки на каналы
@r.callback_query(F.data == "adm:requests")
async def adm_requests(cb: CallbackQuery):
    if cb.from_user.id != ADMIN_ID: return
    items = await q("SELECT * FROM task_requests WHERE status='awaiting_approval' OR status='pending_admin_check' LIMIT 10", fetch="all") or []
    if not items:
        return await cb.message.edit_text("📋 Нет заявок.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад",callback_data="adm:back")]]))
    text = "📋 <b>Заявки на каналы:</b>\n\n"; btns = []
    for it in items:
        text += f"#{it['id']} {it['channel_link']}\n{it['completions_total']}×{it['price_per_completion']}⭐ | 👤{it['requester_id']}\n\n"
        btns.append([InlineKeyboardButton(text=f"✅#{it['id']}",callback_data=f"req_ok:{it['id']}"),
                     InlineKeyboardButton(text=f"❌#{it['id']}",callback_data=f"req_no:{it['id']}")])
    btns.append([InlineKeyboardButton(text="🔙 Назад",callback_data="adm:back")])
    await cb.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=btns), parse_mode="HTML")

@r.callback_query(F.data.startswith("req_ok:"))
async def req_ok(cb: CallbackQuery, bot: Bot):
    if cb.from_user.id != ADMIN_ID: return
    rid = int(cb.data.split(":")[1])
    req = await q("SELECT * FROM task_requests WHERE id=?", (rid,), "one")
    if not req: return await cb.answer("Не найдено.", show_alert=True)
    await q("UPDATE task_requests SET status='active' WHERE id=?", (rid,))
    stars = float(await get_setting("task_stars", str(DEFAULT_TASK_STARS)))
    xp    = int(await get_setting("task_xp", str(DEFAULT_TASK_XP)))
    th = f"req{rid}_{hash(req['channel_link'])}"
    await q("INSERT OR REPLACE INTO tasks VALUES(?,?,?,?)", (th, req["channel_link"], stars, xp))
    try: await bot.send_message(req["requester_id"], f"✅ Заявка #{rid} одобрена! Канал добавлен в задания.")
    except: pass
    await cb.answer("✅"); await adm_requests(cb)

@r.callback_query(F.data.startswith("req_no:"))
async def req_no(cb: CallbackQuery, bot: Bot):
    if cb.from_user.id != ADMIN_ID: return
    rid = int(cb.data.split(":")[1])
    req = await q("SELECT * FROM task_requests WHERE id=?", (rid,), "one")
    if not req: return await cb.answer("Не найдено.", show_alert=True)
    await q("UPDATE task_requests SET status='rejected' WHERE id=?", (rid,))
    total = round(req["completions_total"] * req["price_per_completion"], 2)
    await update_balance(req["requester_id"], total)
    try: await bot.send_message(req["requester_id"], f"❌ Заявка #{rid} отклонена. {total}⭐ возвращены.")
    except: pass
    await cb.answer("❌"); await adm_requests(cb)

# рассылка
@r.callback_query(F.data == "adm:broadcast")
async def adm_broadcast(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id != ADMIN_ID: return
    await state.set_state(A.bc_content)
    await cb.message.answer("📢 Отправьте сообщение для рассылки (текст/фото/видео):")
    await cb.answer()

@r.message(A.bc_content)
async def adm_bc_content(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_ID: return
    await state.update_data(bc_cid=msg.chat.id, bc_mid=msg.message_id)
    await state.set_state(A.bc_btn_text)
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⏭ Без кнопки", callback_data="bc_skip_btn")]])
    await msg.answer("🔗 Текст Inline-кнопки или «Без кнопки»:", reply_markup=kb)

@r.callback_query(F.data == "bc_skip_btn")
async def bc_skip(cb: CallbackQuery, state: FSMContext, bot: Bot):
    if cb.from_user.id != ADMIN_ID: return
    data = await state.get_data(); await state.clear()
    await do_broadcast(bot, data["bc_cid"], data["bc_mid"], None)

@r.message(A.bc_btn_text)
async def adm_bc_btn_text(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_ID: return
    await state.update_data(btn_t=msg.text); await state.set_state(A.bc_btn_url); await msg.answer("🔗 URL кнопки:")

@r.message(A.bc_btn_url)
async def adm_bc_btn_url(msg: Message, state: FSMContext, bot: Bot):
    if msg.from_user.id != ADMIN_ID: return
    if not msg.text.startswith("http"): return await msg.answer("❌ Ссылка должна начинаться с http")
    data = await state.get_data(); await state.clear()
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=data["btn_t"], url=msg.text)]])
    await do_broadcast(bot, data["bc_cid"], data["bc_mid"], kb)

async def do_broadcast(bot: Bot, from_chat: int, msg_id: int, kb):
    uids = [r["user_id"] for r in (await q("SELECT user_id FROM users", fetch="all") or [])]
    ok = err = 0
    status = await bot.send_message(from_chat, "⏳ Рассылка...")
    for uid in uids:
        try: await bot.copy_message(uid, from_chat, msg_id, reply_markup=kb); ok += 1; await asyncio.sleep(0.04)
        except: err += 1
    await status.edit_text(f"🎉 Рассылка завершена!\n✅ {ok} | ❌ {err}")

# ══════════════════════════════════════════════════════════
# МОНИТОРИНГ (APScheduler)
# ══════════════════════════════════════════════════════════

async def check_unsubscribes(bot: Bot):
    """Проверяет подписки через PiarFlow и штрафует за отписку."""
    try:
        rows = await q(
            "SELECT mc.user_id, mc.task_hash, mc.channel_link "
            "FROM monitored_completions mc WHERE mc.completed_at > datetime('now','-7 days')",
            fetch="all") or []
        for row in rows:
            still_ok = await pf_check_sub(row["user_id"], row["channel_link"])
            if not still_ok:
                penalty = float(await get_setting("task_stars", str(DEFAULT_TASK_STARS)))
                await update_balance(row["user_id"], -penalty)
                await q("DELETE FROM monitored_completions WHERE user_id=? AND task_hash=?",
                        (row["user_id"], row["task_hash"]))
                try:
                    await bot.send_message(row["user_id"],
                        f"⚠️ Вы отписались от канала и были оштрафованы на {penalty} Stars.")
                except: pass
    except Exception as e:
        log.error(f"check_unsubscribes error: {e}")

async def verify_requests(bot: Bot):
    """Проверяет, добавлен ли бот в каналы по заявкам."""
    try:
        rows = await q("SELECT * FROM task_requests WHERE status='pending_admin_check'", fetch="all") or []
        for req in rows:
            try:
                link = req["channel_link"]
                uname = link.split("t.me/")[-1].strip("/")
                member = await bot.get_chat_member(f"@{uname}", bot.id if bot.id else 0)
                if member.status in ("administrator","creator"):
                    await q("UPDATE task_requests SET status='awaiting_approval' WHERE id=?", (req["id"],))
                    await bot.send_message(ADMIN_ID,
                        f"🔔 Заявка #{req['id']} готова к одобрению!\n🔗 {link}\n"
                        f"Бот добавлен в канал как администратор.")
            except: pass
    except Exception as e:
        log.error(f"verify_requests error: {e}")

# ══════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════

async def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN не задан! Проверьте .env файл.")
    if not ADMIN_ID:
        raise RuntimeError("ADMIN_ID не задан! Проверьте .env файл.")

    await init_db()
    log.info("База данных инициализирована.")

    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp  = Dispatcher(storage=MemoryStorage())

    dp.message.middleware(FloodMiddleware())
    dp.callback_query.middleware(FloodMiddleware())
    dp.message.middleware(BanMiddleware())
    dp.callback_query.middleware(BanMiddleware())
    dp.include_router(r)

    scheduler = AsyncIOScheduler()
    scheduler.add_job(check_unsubscribes, "interval", hours=12, args=[bot])
    scheduler.add_job(verify_requests,    "interval", minutes=5, args=[bot])
    scheduler.start()
    log.info("Планировщик запущен.")

    log.info("Бот запущен!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
