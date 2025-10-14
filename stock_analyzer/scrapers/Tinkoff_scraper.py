import asyncio
import os
from datetime import datetime
from decimal import Decimal
import pandas as pd
from tinkoff.invest import AsyncClient
from tinkoff.invest.exceptions import AioRequestError


TICKERS = ["SBER", "GAZP", "LKOH", "YDEX"]

async def get_figi_by_ticker(client: AsyncClient, ticker: str) -> str | None:
    """Получить FIGI по тикеру из API Tinkoff."""
    try:
        # Ищем по shares (акции)
        shares = await client.instruments.shares()
        for instrument in shares.instruments:
            if instrument.ticker.upper() == ticker.upper() and instrument.api_trade_available_flag:
                print(f"Найден {ticker}: FIGI = {instrument.figi}, Название = {instrument.name}")
                return instrument.figi
        # Если не в shares — попробуем find_instrument (для ETF/других)
        found = await client.instruments.find_instrument(query=ticker)
        for instr in found.instruments:
            if instr.ticker.upper() == ticker.upper() and instr.api_trade_available_flag:
                print(f"Найден {ticker} через поиск: FIGI = {instr.figi}, Название = {instr.name}")
                return instr.figi
        print(f"❌ Тикер {ticker} не найден или не торгуется!")
        return None
    except AioRequestError as e:
        print(f"Ошибка поиска {ticker}: {e}")
        return None

async def get_prices(client: AsyncClient, figis: list) -> dict:
    """Получить последние цены для списка FIGI."""
    try:
        response = await client.market_data.get_last_prices(figi=figis)
        prices = {}
        for lp in response.last_prices:
            if lp.price.units == 0 and lp.price.nano == 0:
                print(f"⚠️ Нулевая цена для FIGI {lp.figi} — возможно, рынок закрыт.")
            price = Decimal(lp.price.units) + Decimal(lp.price.nano) / Decimal('1000000000')
            prices[lp.figi] = float(price)
        return prices
    except AioRequestError as e:
        print(f"Ошибка запроса: {e}")
        return {}

async def poll_stocks(client: AsyncClient, stocks: dict, interval_sec: int = 10):
    """Эмуляция потока: polling каждые N сек."""
    data_log = []
    
    while True:
        timestamp = datetime.now()
        figis = list(stocks.values())
        
        prices = await get_prices(client, figis)
        
        row = {"timestamp": timestamp}
        for ticker, figi in stocks.items():
            price = prices.get(figi, 0.0)
            row[ticker] = price
            if price == 0:
                print(f"  ⚠️ {ticker}: 0 руб. (проверьте FIGI или рынок)")
            else:
                print(f"  💰 {ticker}: {price:.2f} руб.")
        
        data_log.append(row)
        print(f"[{timestamp}] Обновление: {len(stocks)} акций")

        
        await asyncio.sleep(interval_sec)

async def main(TOKEN, TICKERS):
    if not TOKEN:
        raise ValueError("INVEST_TOKEN не задан!")
    
    async with AsyncClient(TOKEN) as client:
        # Тест аутентификации
        accounts = await client.users.get_accounts()
        print(f"Аутентификация OK. Аккаунтов: {len(accounts.accounts)}")
        
        # Получаем FIGI для всех тикеров
        stocks = {}  # ticker: figi
        for ticker in TICKERS:
            figi = await get_figi_by_ticker(client, ticker)
            if figi:
                stocks[ticker] = figi
        
        if not stocks:
            raise ValueError("Нет валидных FIGI! Проверьте тикеры.")
        
        print(f"\nНайдено {len(stocks)} акций: {list(stocks.keys())}")
        
        # Запуск polling
        print("Запуск polling каждые 30 сек... (Ctrl+C для остановки)")
        await poll_stocks(client, stocks, 10)

def start_main(TOKEN, TICKERS):
    asyncio.run(main(TOKEN, TICKERS))