import asyncio
from datetime import datetime
from decimal import Decimal

from tinkoff.invest import AsyncClient
from tinkoff.invest.exceptions import AioRequestError
from PySide6.QtCore import QObject, Signal
from datetime import datetime, timedelta
from pymongo import InsertOne
from pymongo.errors import BulkWriteError

from db.MongoDB_handler import save_prices, db

class ParserSignals(QObject):
    status = Signal(str)
    error = Signal(str)

async def get_figi_by_ticker(
    client: AsyncClient,
    ticker: str,
    signals: ParserSignals | None = None
) -> str | None:
    try:
        if signals:
            signals.status.emit(f"🔍 Поиск FIGI для {ticker}")

        shares = await client.instruments.shares()
        for instrument in shares.instruments:
            if (
                instrument.ticker.upper() == ticker.upper()
                and instrument.api_trade_available_flag
            ):
                if signals:
                    signals.status.emit(
                        f"✅ Найден {ticker}: {instrument.name}"
                    )
                return instrument.figi

        found = await client.instruments.find_instrument(query=ticker)
        for instr in found.instruments:
            if (
                instr.ticker.upper() == ticker.upper()
                and instr.api_trade_available_flag
            ):
                if signals:
                    signals.status.emit(
                        f"✅ Найден {ticker} через поиск: {instr.name}"
                    )
                return instr.figi

        if signals:
            signals.status.emit(f"❌ {ticker} не найден или не торгуется")
        return None

    except AioRequestError as e:
        if signals:
            signals.error.emit(f"Ошибка поиска {ticker}: {e}")
        return None
    
async def get_prices(
    client: AsyncClient,
    figis: list,
    signals: ParserSignals | None = None
) -> dict:
    try:
        response = await client.market_data.get_last_prices(figi=figis)
        prices = {}

        for lp in response.last_prices:
            price = (
                Decimal(lp.price.units)
                + Decimal(lp.price.nano) / Decimal("1000000000")
            )
            prices[lp.figi] = float(price)

        return prices

    except AioRequestError as e:
        if signals:
            signals.error.emit(f"Ошибка запроса цен: {e}")
        return {}


async def poll_stocks(
    client: AsyncClient,
    stocks: dict,
    interval_sec: int = 10,
    signals: ParserSignals | None = None
):
    last_time = None
    while True:
        now = datetime.now()
        if last_time is not None:
            diff = (now - last_time).total_seconds()
            if signals:
                signals.status.emit(f"Δt = {diff:.1f} сек")
        last_time = now

        timestamp = datetime.now()

        if signals:
            signals.status.emit(f"📡 Обновление цен ({timestamp:%H:%M:%S})")

        figis = list(stocks.values())
        prices = await get_prices(client, figis, signals)

        # ⬇⬇⬇ ФОРМАТ ROW — ТОЧНО КАК В СТАРОМ КОДЕ ⬇⬇⬇
        row = {"date": timestamp}

        for ticker, figi in stocks.items():
            price = prices.get(figi, 0.0)
            row[ticker] = price

            if signals:
                if price == 0:
                    signals.status.emit(
                        f"⚠️ {ticker}: 0 руб."
                    )
                else:
                    signals.status.emit(
                        f"💰 {ticker}: {price:.2f} руб."
                    )

        save_prices(row)

        await asyncio.sleep(interval_sec)

async def downsampler_loop(db, interval_sec=300):
    while True:
        try:
            downsample_1m_to_5m(db)
        except Exception as e:
            print(f"Downsampler error: {e}")
        await asyncio.sleep(interval_sec)

async def main(
    TOKEN: str,
    TICKERS: list,
    signals: ParserSignals | None = None
):
    if not TOKEN:
        raise ValueError("INVEST_TOKEN не задан!")

    async with AsyncClient(TOKEN) as client:
        await client.users.get_accounts()

        if signals:
            signals.status.emit("✅ Аутентификация успешна")

        stocks = {}
        for ticker in TICKERS:
            figi = await get_figi_by_ticker(client, ticker, signals)
            if figi:
                stocks[ticker] = figi

        if not stocks:
            raise ValueError("Нет валидных FIGI!")

        if signals:
            signals.status.emit(
                f"🚀 Запуск polling ({len(stocks)} акций)"
            )
        
        asyncio.create_task(downsampler_loop(db))
        await poll_stocks(client, stocks, 60, signals)


def start_main(TOKEN, TICKERS, signals: ParserSignals | None = None):
    asyncio.run(main(TOKEN, TICKERS, signals))

def downsample_1m_to_5m(
    db,
    tickers=None,
    lookback_minutes=15,
    min_fill_ratio=0.6,
    dry_run=False
):
    prices_1m = db["prices_1m"]
    prices_5m = db["prices_5m"]

    if tickers is None:
        tickers = prices_1m.distinct("meta.ticker")

    print(f"\n=== Downsampler запустился ===")
    print(f"Тикеров для обработки: {len(tickers)}")
    print(f"min_fill_ratio = {min_fill_ratio}")

    sample = prices_1m.find_one(sort=[("timestamp", -1)])
    if sample:
        print("Самый свежий документ в prices_1m:")
        print("  timestamp:", sample["timestamp"])
        print("  тип      :", type(sample["timestamp"]))
        print("  tzinfo   :", sample["timestamp"].tzinfo if hasattr(sample["timestamp"], 'tzinfo') else "naive")
    else:
        print("prices_1m ПУСТАЯ — нет данных для агрегации!")

    inserted_total = 0
    now = datetime.now()   # локальное время

    for ticker in tickers:
        last_5m = prices_5m.find_one(
            {"meta.ticker": ticker, "meta.tf": "5m"},
            sort=[("timestamp", -1)],
            projection={"timestamp": 1}
        )

        after_ts = last_5m["timestamp"] if last_5m else (now - timedelta(minutes=lookback_minutes))

        print(f"\n--- {ticker} ---")
        print(f"  after_ts = {after_ts}   (тип: {type(after_ts)})")

        # Сколько минутных записей после after_ts
        count_new = prices_1m.count_documents({
            "meta.ticker": ticker,
            "timestamp": {"$gt": after_ts}
        })
        print(f"  Новых 1m записей после after_ts: {count_new}")

        if count_new == 0:
            print("  → ничего не найдено, переходим к следующему тикеру")
            continue

        pipeline = [
            {"$match": {
                "meta.ticker": ticker,
                "timestamp": {"$gt": after_ts}
            }},
            {"$sort": {"timestamp": 1}},

            {"$group": {
                "_id": {
                    "$toDate": {
                        "$subtract": [
                            {"$toLong": "$timestamp"},
                            {"$mod": [{"$toLong": "$timestamp"}, 300000]}  # 300000 ms = 5 мин
                        ]
                    }
                },
                "prices": {"$push": "$price"},
                "ts_list": {"$push": "$timestamp"}
            }},

            {"$project": {
                "timestamp": {"$dateAdd": {
                    "startDate": "$_id",
                    "unit": "minute",
                    "amount": 5
                }},
                "prices": 1,
                "first_ts": {"$min": "$ts_list"},
                "last_ts": {"$max": "$ts_list"},
                "filled_count": {"$size": "$prices"}
            }},

            {"$match": {
                "filled_count": {"$gte": int(5 * min_fill_ratio)}
            }},

            {"$project": {
                "timestamp": 1,
                "meta": {"ticker": ticker, "tf": "5m"},
                "open":  {"$arrayElemAt": ["$prices", 0]},
                "high":  {"$max": "$prices"},
                "low":   {"$min": "$prices"},
                "close": {"$arrayElemAt": ["$prices", -1]},
                "filled_count": 1,
                "filled_ratio": {"$round": [{"$divide": ["$filled_count", 5]}, 3]}
            }}
        ]

        try:
            cursor = prices_1m.aggregate(pipeline)
            results = list(cursor)
            print(f"  Pipeline вернул {len(results)} документов")

            if results:
                print("  Пример первой свечи:")
                print(results[0])

            if not results:
                print("  → pipeline ничего не вернул (проблема в группировке или фильтрах)")

            ops = [InsertOne(doc) for doc in results]

            if dry_run:
                print(f"{ticker}: {len(ops)} свечей подготовлено")
                continue

            res = prices_5m.bulk_write(ops, ordered=False)
            inserted = len(res.inserted_ids) if hasattr(res, 'inserted_ids') else len(ops)
            inserted_total += inserted

            print(f"{ticker}: вставлено {inserted} свечей 5m")

        except BulkWriteError as e:
            print(f"Конфликт при вставке {ticker}: {e.details}")

    return inserted_total

