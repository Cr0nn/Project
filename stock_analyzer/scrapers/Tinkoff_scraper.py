import asyncio
from datetime import datetime
from decimal import Decimal

from tinkoff.invest import AsyncClient
from tinkoff.invest.exceptions import AioRequestError
from PySide6.QtCore import QObject, Signal

from db.MongoDB_handler import save_prices

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
    while True:
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

        await poll_stocks(client, stocks, 60, signals)

def start_main(TOKEN, TICKERS, signals: ParserSignals | None = None):
    asyncio.run(main(TOKEN, TICKERS, signals))
