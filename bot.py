import asyncio
import logging
import os
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import aiohttp
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramNetworkError, TelegramRetryAfter
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
)
from aiohttp import web

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ---------- Настройки (Environment на Render) ----------
BOT_TOKEN = os.environ["BOT_TOKEN"]
CHANNEL_ID = int(os.environ["CHANNEL_ID"])
PORT = int(os.environ.get("PORT", "10000"))
PUBLIC_URL = (os.environ.get("PUBLIC_URL") or os.environ.get("RENDER_EXTERNAL_URL") or "").rstrip("/")
# Премиум-эмодзи в кнопках канала работают, только если у бота куплен доп. юзернейм на Fragment.
# Если купишь — поставь переменную CHANNEL_BUTTON_ICONS=1
CHANNEL_BUTTON_ICONS = os.environ.get("CHANNEL_BUTTON_ICONS", "0") == "1"

IMAGE_URL = "https://cdn.phototourl.com/member/2026-11-05-c7d7d24c-9b58-49cf-b7a6-450335ed6b9e.png"
PREVIEW = LinkPreviewOptions(url=IMAGE_URL, show_above_text=True, prefer_large_media=True)

TZ = ZoneInfo("Europe/Moscow")
POST_EVERY_MINUTES = 30
MAX_POST_LATE = 5 * 60     # если проспали слот больше чем на 5 минут — слот пропускаем
USER_COOLDOWN = 5 * 60     # лимит: бот и инлайн — каждый отдельно, раз в 5 минут
RATES_REFRESH = 60         # как часто обновляем курс в фоне (сек)
RATES_FRESH = 120          # курс младше этого возраста отдаём сразу
RATES_STALE_MAX = 30 * 60  # если API недоступны, можно показать курс не старше 30 минут
KEEPALIVE_EVERY = 240      # самопинг, чтобы бесплатный Render не засыпал

# premium-эмодзи кнопок
BTN_VIEW_EMOJI = "5276412364458059956"   # 🕓
BTN_SHARE_EMOJI = "5278305362703835500"  # 🔗

COINS = [
    {"label": "USDT", "cg": ["tether"], "cc": ["USDT"], "emoji": "5192942020112442148"},
    # TON переименован в GRAM, на CoinGecko id теперь "gram"; старый оставлен про запас
    {"label": "GRAM (ранее Ton)", "cg": ["gram", "the-open-network"], "cc": ["GRAM", "TON"], "emoji": "5193179982775476271"},
    {"label": "BTC", "cg": ["bitcoin"], "cc": ["BTC"], "emoji": "5193059508942824703"},
    {"label": "ETH", "cg": ["ethereum"], "cc": ["ETH"], "emoji": "5195352119535755156"},
    {"label": "SOL", "cg": ["solana"], "cc": ["SOL"], "emoji": "5193144965907110821"},
    {"label": "BNB", "cg": ["binancecoin"], "cc": ["BNB"], "emoji": "5193131612853789713"},
    {"label": "USDC", "cg": ["usd-coin"], "cc": ["USDC"], "emoji": "5193004361562745352"},
    {"label": "TRX", "cg": ["tron"], "cc": ["TRX"], "emoji": "5193193288584158951"},
]

CG_URL = "https://api.coingecko.com/api/v3/simple/price"
CC_URL = "https://min-api.cryptocompare.com/data/pricemultifull"
HTTP_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; CryptoExzoBot/1.0)"}

# ---------- Состояние ----------
bot_username = ""
last_view: dict[int, float] = {}    # лимит просмотра в личке бота
last_inline: dict[int, float] = {}  # лимит инлайна (и «Поделиться»)
rates: dict = {"data": {}, "ts": 0.0}
_rates_lock = asyncio.Lock()
_tasks: set = set()


# ---------- Курсы ----------
async def fetch_coingecko(session: aiohttp.ClientSession) -> dict:
    ids = sorted({i for c in COINS for i in c["cg"]})
    params = {"ids": ",".join(ids), "vs_currencies": "usd,rub", "include_24hr_change": "true"}
    async with session.get(CG_URL, params=params) as r:
        r.raise_for_status()
        raw = await r.json()
    out = {}
    for c in COINS:
        for cid in c["cg"]:
            info = raw.get(cid)
            if info and info.get("usd") is not None:
                out[c["label"]] = {
                    "usd": float(info["usd"]),
                    "rub": float(info["rub"]) if info.get("rub") is not None else None,
                    "chg": float(info.get("usd_24h_change") or 0.0),
                }
                break
    return out


async def fetch_cryptocompare(session: aiohttp.ClientSession) -> dict:
    syms = sorted({s for c in COINS for s in c["cc"]})
    params = {"fsyms": ",".join(syms), "tsyms": "USD,RUB"}
    async with session.get(CC_URL, params=params) as r:
        r.raise_for_status()
        raw = (await r.json()).get("RAW", {})
    out = {}
    for c in COINS:
        for sym in c["cc"]:
            row = raw.get(sym)
            if row and row.get("USD", {}).get("PRICE") is not None:
                rub = row.get("RUB", {}).get("PRICE")
                out[c["label"]] = {
                    "usd": float(row["USD"]["PRICE"]),
                    "rub": float(rub) if rub is not None else None,
                    "chg": float(row["USD"].get("CHANGEPCT24HOUR") or 0.0),
                }
                break
    return out


async def refresh_rates() -> bool:
    """Обновляет кэш курса: CoinGecko, а чего не хватает — добирает из CryptoCompare."""
    async with _rates_lock:
        if time.monotonic() - rates["ts"] < 5:  # только что обновляли
            return True
        data: dict = {}
        timeout = aiohttp.ClientTimeout(total=8)
        async with aiohttp.ClientSession(timeout=timeout, headers=HTTP_HEADERS) as session:
            for name, fn in (("coingecko", fetch_coingecko), ("cryptocompare", fetch_cryptocompare)):
                try:
                    part = await fn(session)
                except Exception as e:
                    logging.warning("%s error: %s", name, e)
                    continue
                for label, value in part.items():
                    data.setdefault(label, value)
                if len(data) == len(COINS):
                    break
        if not data:
            return False
        rates["data"] = {**rates["data"], **data}
        rates["ts"] = time.monotonic()
        return True


async def get_rates() -> dict:
    age = time.monotonic() - rates["ts"]
    if rates["data"] and age < RATES_FRESH:
        return rates["data"]
    await refresh_rates()
    age = time.monotonic() - rates["ts"]
    if rates["data"] and age < RATES_STALE_MAX:
        return rates["data"]
    raise RuntimeError("Курс недоступен")


async def rates_updater():
    while True:
        try:
            await refresh_rates()
        except Exception:
            logging.exception("rates_updater error")
        await asyncio.sleep(RATES_REFRESH)


# ---------- Сообщение ----------
def emoji(eid: str, fallback: str) -> str:
    return f"<tg-emoji emoji-id='{eid}'>{fallback}</tg-emoji>"


def fmt_price(value: float, symbol: str) -> str:
    digits = 2 if value >= 1 else 4
    s = f"{value:,.{digits}f}".replace(",", " ")
    return f"{symbol}{s}" if symbol == "$" else f"{s}{symbol}"


async def build_message(when: datetime | None = None) -> str:
    data = await get_rates()
    now = when or datetime.now(TZ)

    lines = [
        f"{emoji('5363949944048756047', '📡')} <b>Актуальный</b> курс <b>криптовалюты</b> на "
        f"<b>{now:%H:%M}</b> по <b>МСК</b> <b>{now:%d.%m.%Y}</b>:",
        "",
    ]

    for c in COINS:
        info = data.get(c["label"])
        if not info:
            logging.warning("No data for %s", c["label"])
            continue
        change = info["chg"]
        arrow = emoji("5206401524200145033", "🔼") if change >= 0 else emoji("5206510891247371052", "🔽")

        price = f"{emoji('5255933397750014894', '💱')} {fmt_price(info['usd'], '$')}"
        if info["rub"] is not None:
            price += f" &amp; {emoji('5255806447106679302', '💱')} {fmt_price(info['rub'], '₽')}"

        lines.append(
            f"{emoji(c['emoji'], '🪙')} <b>1 {c['label']}</b>: {price} | {arrow} <b>{abs(change):.2f}%</b>"
        )

    lines += ["", '<a href="https://t.me/cryptoexzo">Курс криптовалюты CryptoExzo</a>']
    return "\n".join(lines)


# ---------- Кнопки ----------
def view_button(icons: bool) -> InlineKeyboardButton:
    url = f"https://t.me/{bot_username}?start=rates"
    if icons:
        return InlineKeyboardButton(
            text="Просмотреть курс на данный момент", icon_custom_emoji_id=BTN_VIEW_EMOJI, url=url
        )
    return InlineKeyboardButton(text="🕓 Просмотреть курс на данный момент", url=url)


def share_button(icons: bool) -> InlineKeyboardButton:
    # Выбор чата -> в поле ввода подставляется @бот, внизу появляется «Отправить текущий курс»
    if icons:
        return InlineKeyboardButton(
            text="Поделиться этим курсом", icon_custom_emoji_id=BTN_SHARE_EMOJI, switch_inline_query=""
        )
    return InlineKeyboardButton(text="🔗 Поделиться этим курсом", switch_inline_query="")


def kb_public(icons: bool) -> InlineKeyboardMarkup:
    """Канал и сообщения, отправленные через инлайн."""
    return InlineKeyboardMarkup(inline_keyboard=[[view_button(icons)]])


def kb_private(icons: bool) -> InlineKeyboardMarkup:
    """Ответ в личке бота."""
    return InlineKeyboardMarkup(inline_keyboard=[[view_button(icons)], [share_button(icons)]])


# ---------- Отправка ----------
async def send_rates(bot: Bot, chat_id: int, text: str, private: bool, icons: bool):
    """Отправка с повторами. Если Telegram не принял иконки в кнопках — шлём с обычными эмодзи."""
    make_kb = kb_private if private else kb_public
    last_error: Exception | None = None
    for attempt in range(4):
        try:
            return await bot.send_message(
                chat_id, text, reply_markup=make_kb(icons), link_preview_options=PREVIEW
            )
        except TelegramRetryAfter as e:
            last_error = e
            await asyncio.sleep(e.retry_after + 1)
        except TelegramBadRequest as e:
            last_error = e
            if icons:
                logging.warning("Иконки в кнопках не приняты (%s) — отправляю без них", e)
                icons = False
            else:
                raise
        except TelegramNetworkError as e:
            last_error = e
            await asyncio.sleep(2 * (attempt + 1))
    raise RuntimeError(f"Не удалось отправить сообщение: {last_error}")


def cooldown_left(store: dict, uid: int) -> float:
    return USER_COOLDOWN - (time.monotonic() - store.get(uid, -1e9))


def fmt_left(left: float) -> str:
    m, s = divmod(int(left) + 1, 60)
    return f"{m} мин {s} сек"


def make_result(text: str) -> InlineQueryResultArticle:
    now = datetime.now(TZ)
    return InlineQueryResultArticle(
        id="rates",
        title="Отправить текущий курс",
        description=f"Актуальный курс криптовалюты на {now:%H:%M} по МСК",
        thumbnail_url=IMAGE_URL,
        input_message_content=InputTextMessageContent(
            message_text=text,
            parse_mode=ParseMode.HTML,
            link_preview_options=PREVIEW,
        ),
        reply_markup=kb_public(icons=False),
    )


# ---------- Хендлеры ----------
dp = Dispatcher()


@dp.message(CommandStart())
async def on_start(message: Message):
    uid = message.from_user.id
    left = cooldown_left(last_view, uid)
    if left > 0:
        await message.answer(
            f"⏳ Курс можно просматривать раз в 5 минут. Попробуйте снова через {fmt_left(left)}."
        )
        return
    try:
        text = await build_message()
        await send_rates(message.bot, message.chat.id, text, private=True, icons=True)
    except Exception:
        logging.exception("DM reply failed")
        await message.answer("⚠️ Не удалось получить курс, попробуйте через минуту.")
        return
    last_view[uid] = time.monotonic()


@dp.inline_query()
async def on_inline(query: InlineQuery):
    uid = query.from_user.id
    left = cooldown_left(last_inline, uid)
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
        await query.answer([make_result(text)], cache_time=0, is_personal=True)
    except Exception:
        logging.exception("Inline answer failed")
        try:
            await query.answer(
                [],
                cache_time=0,
                is_personal=True,
                button=InlineQueryResultsButton(
                    text="⚠️ Курс недоступен, попробуйте позже", start_parameter="error"
                ),
            )
        except Exception:
            pass


@dp.chosen_inline_result()
async def on_chosen(chosen: ChosenInlineResult):
    # Лимит включается, когда человек реально отправил курс.
    # Нужно включить /setinlinefeedback (100%) в BotFather.
    last_inline[chosen.from_user.id] = time.monotonic()


# ---------- Автопост в канал ----------
def next_slot(after: datetime) -> datetime:
    """Ближайшее время строго после `after`: 00:00, 00:30, ... 23:30 по МСК."""
    slot = after.replace(minute=0, second=0, microsecond=0)
    while slot <= after:
        slot += timedelta(minutes=POST_EVERY_MINUTES)
    return slot


async def wait_until(target: datetime, early: float = 0.0):
    while True:
        delay = (target - datetime.now(TZ)).total_seconds() - early
        if delay <= 0:
            return
        await asyncio.sleep(min(delay, 20))


async def scheduler(bot: Bot):
    target = next_slot(datetime.now(TZ))
    logging.info("Следующий пост в канал: %s МСК", target.strftime("%H:%M"))
    while True:
        await wait_until(target, early=6)
        try:  # свежий курс прямо перед постом
            await asyncio.wait_for(refresh_rates(), timeout=10)
        except Exception as e:
            logging.warning("Не удалось обновить курс перед постом: %s", e)
        await wait_until(target)

        late = (datetime.now(TZ) - target).total_seconds()
        if late > MAX_POST_LATE:
            logging.warning("Слот %s пропущен: опоздание %.0f сек", target.strftime("%H:%M"), late)
        else:
            try:
                text = await build_message(when=target)
                await send_rates(bot, CHANNEL_ID, text, private=False, icons=CHANNEL_BUTTON_ICONS)
                logging.info("Пост в канал %s МСК отправлен (опоздание %.1f сек)", target.strftime("%H:%M"), late)
            except Exception:
                logging.exception("Пост в канал не отправлен")
        target = next_slot(max(datetime.now(TZ), target))


# ---------- Служебное ----------
async def keepalive():
    """Раз в 4 минуты стучимся на свой публичный адрес, чтобы бесплатный Render не засыпал."""
    await asyncio.sleep(60)
    while True:
        try:
            timeout = aiohttp.ClientTimeout(total=15)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(PUBLIC_URL + "/") as r:
                    await r.read()
        except Exception as e:
            logging.warning("keepalive error: %s", e)
        await asyncio.sleep(KEEPALIVE_EVERY)


async def supervise(name: str, fn, *args):
    """Если фоновая задача упала — перезапускаем, чтобы она никогда не останавливалась."""
    while True:
        try:
            await fn(*args)
            return
        except asyncio.CancelledError:
            raise
        except Exception:
            logging.exception("%s упала, перезапуск через 5 сек", name)
            await asyncio.sleep(5)


def spawn(name: str, fn, *args):
    task = asyncio.create_task(supervise(name, fn, *args))
    _tasks.add(task)
    task.add_done_callback(_tasks.discard)


async def h_health(request: web.Request):
    return web.Response(text="ok")


async def start_web():
    app = web.Application()
    app.router.add_get("/", h_health)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()


async def main():
    global bot_username
    await start_web()  # порт открываем сразу, чтобы Render видел сервис живым

    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    for attempt in range(10):
        try:
            bot_username = (await bot.get_me()).username
            break
        except Exception as e:
            logging.warning("get_me failed (%s), retry", e)
            await asyncio.sleep(3)
    else:
        raise RuntimeError("Не удалось подключиться к Telegram")

    await bot.delete_webhook(drop_pending_updates=True)

    spawn("rates_updater", rates_updater)
    spawn("scheduler", scheduler, bot)
    if PUBLIC_URL:
        spawn("keepalive", keepalive)

    await dp.start_polling(
        bot, allowed_updates=["message", "inline_query", "chosen_inline_result"]
    )


if __name__ == "__main__":
    asyncio.run(main())
