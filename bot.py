import asyncio
import json
import logging
import os
import statistics
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import aiohttp
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramNetworkError, TelegramRetryAfter
from aiogram.filters import Command, CommandStart
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
# Необязательно: бесплатный Demo-ключ CoinGecko (дополнительный источник курса)
COINGECKO_API_KEY = os.environ.get("COINGECKO_API_KEY", "").strip()
# Премиум-эмодзи в кнопках канала работают, только если у бота куплен доп. юзернейм на Fragment.
# Если купишь — поставь переменную CHANNEL_BUTTON_ICONS=1
CHANNEL_BUTTON_ICONS = os.environ.get("CHANNEL_BUTTON_ICONS", "0") == "1"
# Служебный чат (твоя личка с ботом или закрытая группа только для тебя и бота).
# Бот отправляет курс туда (там премиум-эмодзи разрешены), копирует в канал без «Переслано от…»
# и удаляет черновик. Узнать ID: отправь боту /id.
STAGING_CHAT_ID = int(os.environ["STAGING_CHAT_ID"]) if os.environ.get("STAGING_CHAT_ID", "").strip() else None

IMAGE_URL = "https://cdn.phototourl.com/member/2026-11-05-c7d7d24c-9b58-49cf-b7a6-450335ed6b9e.png"
PREVIEW = LinkPreviewOptions(url=IMAGE_URL, show_above_text=True, prefer_large_media=True)

TZ = ZoneInfo("Europe/Moscow")
POST_EVERY_MINUTES = 30
MAX_POST_LATE = 5 * 60     # если проспали слот больше чем на 5 минут — слот пропускаем
USER_COOLDOWN = 5 * 60     # лимит: бот и инлайн — каждый отдельно, раз в 5 минут
RATES_REFRESH = 60         # как часто обновляем курс в фоне (сек)
RATES_FRESH = 150          # курс младше этого возраста отдаём сразу
RATES_STALE_MAX = 30 * 60  # если все источники недоступны, показываем курс не старше 30 минут
FX_REFRESH = 15 * 60       # как часто обновляем курс доллара к рублю
KEEPALIVE_EVERY = 240      # самопинг, чтобы бесплатный Render не засыпал

# premium-эмодзи кнопок
BTN_VIEW_EMOJI = "5276412364458059956"   # 🕓
BTN_SHARE_EMOJI = "5278305362703835500"  # 🔗

# base — тикер на биржах, cg — id на CoinGecko (используется только если задан COINGECKO_API_KEY)
COINS = [
    {"label": "USDT", "base": "USDT", "cg": "tether", "emoji": "5192942020112442148", "stable": True},
    {"label": "GRAM (ранее Ton)", "base": "GRAM", "cg": "gram", "emoji": "5193179982775476271"},
    {"label": "BTC", "base": "BTC", "cg": "bitcoin", "emoji": "5193059508942824703"},
    {"label": "ETH", "base": "ETH", "cg": "ethereum", "emoji": "5195352119535755156"},
    {"label": "SOL", "base": "SOL", "cg": "solana", "emoji": "5193144965907110821"},
    {"label": "BNB", "base": "BNB", "cg": "binancecoin", "emoji": "5193131612853789713"},
    {"label": "USDC", "base": "USDC", "cg": "usd-coin", "emoji": "5193004361562745352", "stable": True},
    {"label": "TRX", "base": "TRX", "cg": "tron", "emoji": "5193193288584158951"},
]

HTTP_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; CryptoExzoBot/1.0)"}

# ---------- Состояние ----------
bot_username = ""
last_view: dict[int, float] = {}    # лимит просмотра в личке бота
last_inline: dict[int, float] = {}  # лимит инлайна (и «Поделиться»)
rates: dict = {"data": {}, "ts": 0.0, "fx": None, "fx_ts": 0.0}
_rates_lock = asyncio.Lock()
_tasks: set = set()


# ---------- Источники курса (всё без API-ключей) ----------
async def _get_json(session: aiohttp.ClientSession, url: str, **kwargs):
    async with session.get(url, **kwargs) as r:
        r.raise_for_status()
        return await r.json(content_type=None)


def from_market(m: dict) -> dict:
    """m: {ТИКЕР: (цена в USDT, изменение за 24ч в %)} -> {подпись монеты: (цена в USD, изменение %)}"""
    out = {}
    for c in COINS:
        base = c["base"]
        if base == "USDT":
            # USDT = 1 / (цена USDC в USDT)
            if "USDC" in m and m["USDC"][0] > 0:
                last, chg = m["USDC"]
                out[c["label"]] = (1.0 / last, -chg)
        elif base in m:
            out[c["label"]] = m[base]
    return out


async def fetch_binance(session):
    syms = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "TRXUSDT", "USDCUSDT", "GRAMUSDT"]
    last_err: Exception = RuntimeError("binance: нет ответа")
    for host in ("https://data-api.binance.vision", "https://api.binance.com"):
        for sset in (syms, syms[:-1]):
            try:
                data = await _get_json(
                    session,
                    f"{host}/api/v3/ticker/24hr",
                    params={"symbols": json.dumps(sset, separators=(",", ":"))},
                )
            except aiohttp.ClientResponseError as e:
                last_err = e
                if e.status == 400:  # какого-то символа нет — пробуем без GRAM
                    continue
                break  # хост недоступен — пробуем следующий
            except Exception as e:
                last_err = e
                break
            m = {t["symbol"][:-4]: (float(t["lastPrice"]), float(t["priceChangePercent"])) for t in data}
            return from_market(m)
    raise last_err


async def fetch_kucoin(session):
    data = await _get_json(session, "https://api.kucoin.com/api/v1/market/allTickers")
    m = {}
    for t in data["data"]["ticker"]:
        sym = t.get("symbol", "")
        if sym.endswith("-USDT") and t.get("last") and t.get("changeRate") is not None:
            m[sym[:-5]] = (float(t["last"]), float(t["changeRate"]) * 100)
    return from_market(m)


async def fetch_okx(session):
    data = await _get_json(
        session, "https://www.okx.com/api/v5/market/tickers", params={"instType": "SPOT"}
    )
    m = {}
    for t in data["data"]:
        inst = t.get("instId", "")
        if inst.endswith("-USDT") and t.get("last") and t.get("open24h"):
            last, opn = float(t["last"]), float(t["open24h"])
            if opn > 0:
                m[inst[:-5]] = (last, (last - opn) / opn * 100)
    return from_market(m)


async def fetch_bybit(session):
    data = await _get_json(
        session, "https://api.bybit.com/v5/market/tickers", params={"category": "spot"}
    )
    m = {}
    for t in data["result"]["list"]:
        sym = t.get("symbol", "")
        if sym.endswith("USDT") and t.get("lastPrice") and t.get("price24hPcnt") not in (None, ""):
            m[sym[:-4]] = (float(t["lastPrice"]), float(t["price24hPcnt"]) * 100)
    return from_market(m)


async def fetch_coingecko(session):
    ids = ",".join(c["cg"] for c in COINS)
    data = await _get_json(
        session,
        "https://api.coingecko.com/api/v3/simple/price",
        params={"ids": ids, "vs_currencies": "usd", "include_24hr_change": "true"},
        headers={"x-cg-demo-api-key": COINGECKO_API_KEY},
    )
    out = {}
    for c in COINS:
        info = data.get(c["cg"])
        if info and info.get("usd") is not None:
            out[c["label"]] = (float(info["usd"]), float(info.get("usd_24h_change") or 0.0))
    return out


async def fetch_fx(session) -> float:
    """Курс доллара к рублю: ЦБ РФ, запасные — open.er-api и Coinbase."""
    try:
        d = await _get_json(session, "https://www.cbr-xml-daily.ru/daily_json.js")
        v = d["Valute"]["USD"]
        rate = float(v["Value"]) / float(v.get("Nominal", 1))
        if 20 < rate < 500:
            return rate
    except Exception as e:
        logging.warning("fx cbr: %r", e)
    try:
        d = await _get_json(session, "https://open.er-api.com/v6/latest/USD")
        rate = float(d["rates"]["RUB"])
        if 20 < rate < 500:
            return rate
    except Exception as e:
        logging.warning("fx er-api: %r", e)
    d = await _get_json(session, "https://api.coinbase.com/v2/exchange-rates", params={"currency": "USD"})
    rate = float(d["data"]["rates"]["RUB"])
    if not 20 < rate < 500:
        raise RuntimeError("fx: странное значение")
    return rate


async def refresh_rates() -> bool:
    """Опрашивает все источники параллельно и берёт медиану — один сбойный источник курс не испортит."""
    async with _rates_lock:
        if time.monotonic() - rates["ts"] < 5:  # только что обновляли
            return True

        providers = [
            ("binance", fetch_binance),
            ("kucoin", fetch_kucoin),
            ("okx", fetch_okx),
            ("bybit", fetch_bybit),
        ]
        if COINGECKO_API_KEY:
            providers.append(("coingecko", fetch_coingecko))
        need_fx = rates["fx"] is None or time.monotonic() - rates["fx_ts"] > FX_REFRESH

        async def run(name, fn, session):
            try:
                return name, await asyncio.wait_for(fn(session), 20)
            except Exception as e:
                logging.warning("%s: %r", name, e)
                return name, None

        timeout = aiohttp.ClientTimeout(total=8)
        async with aiohttp.ClientSession(timeout=timeout, headers=HTTP_HEADERS) as session:
            jobs = [run(n, f, session) for n, f in providers]
            if need_fx:
                jobs.append(run("fx", fetch_fx, session))
            results = await asyncio.gather(*jobs)

        per_coin: dict[str, list] = {}
        ok = []
        for name, part in results:
            if not part:
                continue
            if name == "fx":
                rates["fx"], rates["fx_ts"] = part, time.monotonic()
                continue
            ok.append(name)
            for label, (usd, chg) in part.items():
                if usd > 0:
                    per_coin.setdefault(label, []).append((usd, chg))

        if not per_coin:
            return False

        new = {}
        for label, vals in per_coin.items():
            prices = [v[0] for v in vals]
            new[label] = {"usd": statistics.median(prices), "chg": statistics.median([v[1] for v in vals])}
            if len(prices) > 1 and max(prices) / min(prices) > 1.10:
                logging.warning("Источники расходятся по %s: %s", label, [round(p, 4) for p in prices])

        rates["data"] = {**rates["data"], **new}
        rates["ts"] = time.monotonic()
        logging.info(
            "Курс обновлён: источники=%s монет=%d/%d USD/RUB=%s GRAM=%s",
            ",".join(ok),
            len(new),
            len(COINS),
            f"{rates['fx']:.2f}" if rates["fx"] else "нет",
            f"{new['GRAM (ранее Ton)']['usd']:.4f}" if "GRAM (ранее Ton)" in new else "нет",
        )
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


def fmt_price(value: float, symbol: str, stable: bool = False) -> str:
    digits = 2 if (stable or value >= 1) else 4
    s = f"{value:,.{digits}f}".replace(",", " ")
    return f"{symbol}{s}" if symbol == "$" else f"{s}{symbol}"


async def build_message(when: datetime | None = None) -> str:
    data = await get_rates()
    fx = rates["fx"]
    now = when or datetime.now(TZ)

    lines = [
        f"{emoji('5363949944048756047', '📡')} <b>Актуальный</b> курс <b>криптовалюты</b> на "
        f"<b>{now:%H:%M}</b> по <b>МСК</b> <b>{now:%d.%m.%Y}</b>:",
        "",
    ]

    for c in COINS:
        info = data.get(c["label"])
        if not info:
            logging.warning("Нет данных по %s", c["label"])
            continue
        stable = c.get("stable", False)
        change = info["chg"]
        up = round(change, 2) >= 0
        arrow = emoji("5206401524200145033", "🔼") if up else emoji("5206510891247371052", "🔽")

        price = f"{emoji('5255933397750014894', '💱')} {fmt_price(info['usd'], '$', stable)}"
        if fx:
            price += f" &amp; {emoji('5255806447106679302', '💱')} {fmt_price(info['usd'] * fx, '₽', stable)}"

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


@dp.message(Command("id"))
async def on_id(message: Message):
    await message.answer(
        f"ID этого чата: <code>{message.chat.id}</code>\nВаш ID: <code>{message.from_user.id}</code>"
    )


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


# ---------- Публикация в канал ----------
async def post_to_channel(bot: Bot, text: str):
    """Через служебный чат (с премиум-эмодзи) -> copy_message в канал. Если не вышло — обычная отправка."""
    if STAGING_CHAT_ID:
        staged_id = None
        try:
            staged = await bot.send_message(
                STAGING_CHAT_ID, text, link_preview_options=PREVIEW, disable_notification=True
            )
            staged_id = staged.message_id
            await asyncio.sleep(2)  # даём Telegram собрать превью с картинкой
            icons = CHANNEL_BUTTON_ICONS
            for _ in range(3):
                try:
                    await bot.copy_message(
                        chat_id=CHANNEL_ID,
                        from_chat_id=STAGING_CHAT_ID,
                        message_id=staged_id,
                        reply_markup=kb_public(icons),
                    )
                    return "copy"
                except TelegramRetryAfter as e:
                    await asyncio.sleep(e.retry_after + 1)
                except TelegramBadRequest:
                    if icons:  # иконки в кнопках не приняты — повторяем с обычными эмодзи
                        icons = False
                        continue
                    raise
            raise RuntimeError("copy_message не удался")
        except Exception:
            logging.exception("Публикация через служебный чат не удалась — отправляю напрямую")
        finally:
            if staged_id:
                try:
                    await bot.delete_message(STAGING_CHAT_ID, staged_id)
                except Exception as e:
                    logging.warning("Не удалось удалить черновик: %r", e)
    await send_rates(bot, CHANNEL_ID, text, private=False, icons=CHANNEL_BUTTON_ICONS)
    return "direct"


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
            await asyncio.wait_for(refresh_rates(), timeout=25)
        except Exception as e:
            logging.warning("Не удалось обновить курс перед постом: %r", e)
        await wait_until(target)

        late = (datetime.now(TZ) - target).total_seconds()
        if late > MAX_POST_LATE:
            logging.warning("Слот %s пропущен: опоздание %.0f сек", target.strftime("%H:%M"), late)
        else:
            try:
                text = await build_message(when=target)
                mode = await post_to_channel(bot, text)
                logging.info("Пост в канал %s МСК отправлен (%s, опоздание %.1f сек)", target.strftime("%H:%M"), mode, late)
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
            logging.warning("keepalive error: %r", e)
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
            logging.warning("get_me failed (%r), retry", e)
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
