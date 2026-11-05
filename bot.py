import asyncio
import hashlib
import hmac
import json
import logging
import os
import time
from datetime import datetime, timedelta
from urllib.parse import parse_qsl
from zoneinfo import ZoneInfo

import aiohttp
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import (
    ChosenInlineResult,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InlineQuery,
    InlineQueryResultArticle,
    InlineQueryResultsButton,
    InputTextMessageContent,
    LinkPreviewOptions,
    Message,
    WebAppInfo,
)
from aiohttp import web

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ---------- Настройки (Environment на Render) ----------
BOT_TOKEN = os.environ["BOT_TOKEN"]
CHANNEL_ID = int(os.environ["CHANNEL_ID"])
PORT = int(os.environ.get("PORT", "10000"))
# Публичный адрес сервиса на Render (Render сам задаёт RENDER_EXTERNAL_URL для Web Service)
PUBLIC_URL = (os.environ.get("PUBLIC_URL") or os.environ.get("RENDER_EXTERNAL_URL") or "").rstrip("/")

IMAGE_URL = "https://i.imgur.com/IaYKoqn.jpeg"
PREVIEW = LinkPreviewOptions(url=IMAGE_URL, show_above_text=True, prefer_large_media=True)

TZ = ZoneInfo("Europe/Moscow")
POST_EVERY_MINUTES = 30
USER_COOLDOWN = 5 * 60
CACHE_TTL = 60
SHARE_TEXT_MAX_AGE = 10 * 60

COINS = [
    ("USDT", "tether", "5192942020112442148"),
    ("GRAM (ранее Ton)", "the-open-network", "5193179982775476271"),
    ("BTC", "bitcoin", "5193059508942824703"),
    ("ETH", "ethereum", "5195352119535755156"),
    ("SOL", "solana", "5193144965907110821"),
    ("BNB", "binancecoin", "5193131612853789713"),
    ("USDC", "usd-coin", "5193004361562745352"),
    ("TRX", "tron", "5193193288584158951"),
]

API_URL = "https://api.coingecko.com/api/v3/simple/price"

# ---------- Состояние ----------
bot_username = ""
last_request: dict[int, float] = {}
last_text: dict[int, tuple[float, str]] = {}
_cache: tuple[float, dict] | None = None
_lock = asyncio.Lock()


def emoji(eid: str, fallback: str) -> str:
    return f"<tg-emoji emoji-id='{eid}'>{fallback}</tg-emoji>"


async def fetch_rates() -> dict:
    global _cache
    async with _lock:
        if _cache and time.monotonic() - _cache[0] < CACHE_TTL:
            return _cache[1]
        params = {
            "ids": ",".join(c[1] for c in COINS),
            "vs_currencies": "usd,rub",
            "include_24hr_change": "true",
        }
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            for attempt in range(3):
                try:
                    async with session.get(API_URL, params=params) as r:
                        r.raise_for_status()
                        data = await r.json()
                    _cache = (time.monotonic(), data)
                    return data
                except Exception as e:
                    logging.warning("CoinGecko error (try %s): %s", attempt + 1, e)
                    await asyncio.sleep(2 * (attempt + 1))
        raise RuntimeError("Не удалось получить курс")


def fmt_price(value: float, symbol: str) -> str:
    digits = 2 if value >= 1 else 4
    s = f"{value:,.{digits}f}".replace(",", " ")
    return f"{symbol}{s}" if symbol == "$" else f"{s}{symbol}"


async def build_message(when: datetime | None = None) -> str:
    data = await fetch_rates()
    now = when or datetime.now(TZ)

    lines = [
        f"{emoji('5363949944048756047', '📡')} <b>Актуальный</b> курс <b>криптовалюты</b> на "
        f"<b>{now:%H:%M}</b> по <b>МСК</b> <b>{now:%d.%m.%Y}</b>:",
        "",
    ]

    for label, cg_id, coin_emoji in COINS:
        info = data.get(cg_id)
        if not info or "usd" not in info:
            continue
        usd = info["usd"]
        rub = info.get("rub")
        change = info.get("usd_24h_change") or 0.0

        if change >= 0:
            arrow = emoji("5206401524200145033", "🔼")
        else:
            arrow = emoji("5206510891247371052", "🔽")

        price = f"{emoji('5255933397750014894', '💱')} {fmt_price(usd, '$')}"
        if rub is not None:
            price += f" &amp; {emoji('5255806447106679302', '💱')} {fmt_price(rub, '₽')}"

        lines.append(
            f"{emoji(coin_emoji, '🪙')} <b>1 {label}</b>: {price} | {arrow} <b>{abs(change):.2f}%</b>"
        )

    lines += ["", '<a href="https://t.me/cryptoexzo">Курс криптовалюты CryptoExzo</a>']
    return "\n".join(lines)


# ---------- Клавиатуры ----------
def view_button() -> InlineKeyboardButton:
    return InlineKeyboardButton(
        text="🕓 Просмотреть курс на данный момент",
        url=f"https://t.me/{bot_username}?start=rates",
    )


def share_button() -> InlineKeyboardButton:
    text = "🔗 Поделиться этим курсом"
    if PUBLIC_URL:
        # Mini App, который сразу открывает окно «via @bot + предпросмотр + кому отправить»
        return InlineKeyboardButton(text=text, web_app=WebAppInfo(url=f"{PUBLIC_URL}/share"))
    # запасной вариант, если публичного адреса нет
    return InlineKeyboardButton(text=text, switch_inline_query="")


def kb_public() -> InlineKeyboardMarkup:
    """Для канала, inline-сообщений и пересылаемых сообщений."""
    return InlineKeyboardMarkup(inline_keyboard=[[view_button()]])


def kb_private() -> InlineKeyboardMarkup:
    """Для ответа в личке бота."""
    return InlineKeyboardMarkup(inline_keyboard=[[view_button()], [share_button()]])


def make_result(text: str) -> InlineQueryResultArticle:
    return InlineQueryResultArticle(
        id="rates",
        title="Показать текущий курс",
        description="Отправить актуальный курс криптовалюты",
        thumbnail_url=IMAGE_URL,
        input_message_content=InputTextMessageContent(
            message_text=text,
            parse_mode=ParseMode.HTML,
            link_preview_options=PREVIEW,
        ),
        reply_markup=kb_public(),
    )


def cooldown_left(uid: int) -> float:
    return USER_COOLDOWN - (time.monotonic() - last_request.get(uid, -1e9))


def fmt_left(left: float) -> str:
    m, s = divmod(int(left) + 1, 60)
    return f"{m} мин {s} сек"


# ---------- Хендлеры ----------
dp = Dispatcher()


@dp.message(CommandStart())
async def on_start(message: Message):
    uid = message.from_user.id
    left = cooldown_left(uid)
    if left > 0:
        await message.answer(
            f"⏳ Курс можно просматривать раз в 5 минут. Попробуйте снова через {fmt_left(left)}."
        )
        return
    try:
        text = await build_message()
    except Exception:
        logging.exception("build_message failed")
        await message.answer("⚠️ Не удалось получить курс, попробуйте чуть позже.")
        return
    last_request[uid] = time.monotonic()
    last_text[uid] = (time.monotonic(), text)
    await message.answer(text, reply_markup=kb_private(), link_preview_options=PREVIEW)


@dp.inline_query()
async def on_inline(query: InlineQuery):
    uid = query.from_user.id
    left = cooldown_left(uid)
    if left > 0:
        await query.answer(
            [],
            cache_time=0,
            is_personal=True,
            button=InlineQueryResultsButton(
                text=f"⏳ Следующий курс через {fmt_left(left)}", start_parameter="wait"
            ),
        )
        return
    try:
        text = await build_message()
    except Exception:
        logging.exception("inline build failed")
        await query.answer(
            [],
            cache_time=0,
            is_personal=True,
            button=InlineQueryResultsButton(
                text="⚠️ Не удалось получить курс", start_parameter="error"
            ),
        )
        return
    await query.answer([make_result(text)], cache_time=0, is_personal=True)


@dp.chosen_inline_result()
async def on_chosen(chosen: ChosenInlineResult):
    # лимит 5 минут включается, когда человек реально отправил курс
    # (нужно включить /setinlinefeedback в BotFather)
    last_request[chosen.from_user.id] = time.monotonic()


# ---------- Автопост в канал ----------
def next_slot(after: datetime) -> datetime:
    """Ближайшее время строго после `after`: 00:00, 00:30, 01:00 ... 23:30 по МСК."""
    slot = after.replace(minute=0, second=0, microsecond=0)
    while slot <= after:
        slot += timedelta(minutes=POST_EVERY_MINUTES)
    return slot


async def scheduler(bot: Bot):
    target = next_slot(datetime.now(TZ))
    logging.info("Next channel post at %s MSK", target.strftime("%H:%M"))
    while True:
        # ждём короткими шагами, чтобы пост не вышел чуть раньше времени
        while True:
            delay = (target - datetime.now(TZ)).total_seconds()
            if delay <= 0:
                break
            await asyncio.sleep(min(delay, 30))
        try:
            # в сообщении всегда стоит ровно время слота (например 12:30), а не 12:29
            text = await build_message(when=target)
            await bot.send_message(
                CHANNEL_ID, text, reply_markup=kb_public(), link_preview_options=PREVIEW
            )
            logging.info("Posted to channel at %s MSK", target.strftime("%H:%M"))
        except Exception:
            logging.exception("Channel post failed")
        target = next_slot(max(datetime.now(TZ), target))


# ---------- Web: health + Mini App для кнопки «Поделиться» ----------
SHARE_HTML = """<!doctype html>
<html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<script src="https://telegram.org/js/telegram-web-app.js"></script>
</head>
<body style="font-family:sans-serif;text-align:center;padding:40px;color:#888">
<div id="m">Загрузка…</div>
<script>
const tg = Telegram.WebApp;
tg.ready();
(async () => {
  try {
    const r = await fetch('/api/prepare', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({initData: tg.initData})
    });
    const d = await r.json();
    if (!d.id) throw new Error('no id');
    tg.shareMessage(d.id, () => tg.close());
  } catch (e) {
    document.getElementById('m').textContent = 'Не удалось. Попробуйте позже.';
    setTimeout(() => tg.close(), 1500);
  }
})();
</script>
</body></html>"""


def check_init_data(init_data: str) -> int | None:
    parsed = dict(parse_qsl(init_data, keep_blank_values=True))
    received_hash = parsed.pop("hash", None)
    if not received_hash:
        return None
    check = "\n".join(f"{k}={v}" for k, v in sorted(parsed.items()))
    secret = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
    calc = hmac.new(secret, check.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(calc, received_hash):
        return None
    if time.time() - int(parsed.get("auth_date", "0")) > 3600:
        return None
    return int(json.loads(parsed["user"])["id"])


async def h_health(request: web.Request):
    return web.Response(text="ok")


async def h_share(request: web.Request):
    return web.Response(text=SHARE_HTML, content_type="text/html")


async def h_prepare(request: web.Request):
    bot: Bot = request.app["bot"]
    try:
        body = await request.json()
        uid = check_init_data(body.get("initData", ""))
        if not uid:
            return web.json_response({"error": "auth"}, status=403)

        saved = last_text.get(uid)
        if saved and time.monotonic() - saved[0] < SHARE_TEXT_MAX_AGE:
            text = saved[1]  # делимся именно тем курсом, который человек видел
        else:
            text = await build_message()

        prepared = await bot.save_prepared_inline_message(
            user_id=uid,
            result=make_result(text),
            allow_user_chats=True,
            allow_bot_chats=True,
            allow_group_chats=True,
            allow_channel_chats=True,
        )
        return web.json_response({"id": prepared.id})
    except Exception:
        logging.exception("prepare failed")
        return web.json_response({"error": "fail"}, status=500)


async def start_web(bot: Bot):
    app = web.Application()
    app["bot"] = bot
    app.router.add_get("/", h_health)
    app.router.add_get("/share", h_share)
    app.router.add_post("/api/prepare", h_prepare)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()


async def main():
    global bot_username
    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    bot_username = (await bot.get_me()).username
    await bot.delete_webhook(drop_pending_updates=True)
    await start_web(bot)
    asyncio.create_task(scheduler(bot))
    await dp.start_polling(
        bot, allowed_updates=["message", "inline_query", "chosen_inline_result"]
    )


if __name__ == "__main__":
    asyncio.run(main())
