"""
ЗАГРУЗКА РЕАЛЬНЫХ ДАННЫХ MOEX
(через публичный API - БЕЗ авторизации!)
"""

import pandas as pd
import numpy as np
import requests
from datetime import datetime, timedelta
import time
import matplotlib.pyplot as plt

print("🔄 Загрузка реальных данных MOEX через публичный API...\n")

# ===== СПИСОК ЛИКВИДНЫХ АКЦИЙ MOEX =====
# Топ-30 акций по ликвидности (примерно аналог индекса MOEX)
MOEX_TICKERS = [
    'SBER',  # Сбербанк
    'GAZP',  # Газпром
    'LKOH',  # Лукойл
    'GMKN',  # Норникель
    'YNDX',  # Яндекс
    'NVTK',  # Новатэк
    'TATN',  # Татнефть
    'ROSN',  # Роснефть
    'MGNT',  # Магнит
    'MTSS',  # МТС
    'ALRS',  # Алроса
    'SNGS',  # Сургутнефтегаз
    'CHMF',  # Северсталь
    'NLMK',  # НЛМК
    'PLZL',  # Полюс
    'VTBR',  # ВТБ
    'POLY',  # Polymetal
    'FEES',  # ФСК ЕЭС
    'MOEX',  # Московская биржа
    'IRAO',  # Интер РАО
    'AFKS',  # Система
    'RTKM',  # Ростелеком
    'AFLT',  # Аэрофлот
    'MAGN',  # ММК
    'PIKK',  # ПИК
    'TCSG',  # TCS Group
    'OZON',  # Ozon
    'FIVE',  # X5 Retail Group
    'DSKY',  # Детский мир
    'MVID',  # М.Видео
]


def fetch_moex_data(ticker, start_date, end_date):
    """
    Загружает исторические данные с MOEX через публичный API

    API документация: https://iss.moex.com/iss/reference/
    """

    base_url = "https://iss.moex.com/iss/history/engines/stock/markets/shares/securities"
    url = f"{base_url}/{ticker}.json"

    params = {
        'from': start_date,
        'till': end_date,
        'start': 0
    }

    all_data = []

    while True:
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            # Извлекаем данные
            history = data.get('history', {})
            columns = history.get('columns', [])
            rows = history.get('data', [])

            if not rows:
                break

            # Создаем DataFrame
            df = pd.DataFrame(rows, columns=columns)
            all_data.append(df)

            # Проверяем, есть ли еще данные
            cursor = data.get('history.cursor', {})
            cursor_columns = cursor.get('columns', [])
            cursor_data = cursor.get('data', [[]])[0]

            if not cursor_data:
                break

            cursor_dict = dict(zip(cursor_columns, cursor_data))
            total = cursor_dict.get('TOTAL', 0)
            index = cursor_dict.get('INDEX', 0)
            page_size = cursor_dict.get('PAGESIZE', 100)

            if index + page_size >= total:
                break

            params['start'] = index + page_size
            time.sleep(0.2)  # Задержка между запросами

        except Exception as e:
            print(f"   ⚠️ Ошибка загрузки {ticker}: {e}")
            break

    if not all_data:
        return None

    # Объединяем все страницы
    df = pd.concat(all_data, ignore_index=True)

    # Оставляем только нужные колонки
    df = df[['TRADEDATE', 'CLOSE']]
    df.columns = ['date', 'close']

    # Преобразуем типы
    df['date'] = pd.to_datetime(df['date'])
    df['close'] = pd.to_numeric(df['close'], errors='coerce')

    # Удаляем пропуски
    df = df.dropna()

    # Сортируем по дате
    df = df.sort_values('date').reset_index(drop=True)

    return df


# ===== ПАРАМЕТРЫ ЗАГРУЗКИ =====
START_DATE = '2023-06-01'
END_DATE = '2024-10-12'
MAX_STOCKS = 30  # Максимум акций для загрузки

print(f"📋 Параметры:")
print(f"   Период: {START_DATE} - {END_DATE}")
print(f"   Акций для загрузки: {MAX_STOCKS}")
print(f"   Источник: MOEX ISS API (публичный)\n")

# ===== ЗАГРУЗКА ДАННЫХ =====
print("⏳ Загрузка данных (может занять 1-2 минуты)...\n")

prices_data = {}
successful = 0
failed = 0

for i, ticker in enumerate(MOEX_TICKERS[:MAX_STOCKS], 1):
    print(f"   [{i}/{MAX_STOCKS}] Загрузка {ticker}...", end=' ')

    df = fetch_moex_data(ticker, START_DATE, END_DATE)

    if df is not None and len(df) > 100:  # Минимум 100 торговых дней
        prices_data[ticker] = df
        successful += 1
        print(f"✅ ({len(df)} дней)")
    else:
        failed += 1
        print(f"❌ (недостаточно данных)")

    time.sleep(0.3)  # Задержка между запросами

print(f"\n✅ Загрузка завершена!")
print(f"   Успешно: {successful} акций")
print(f"   Ошибок: {failed} акций\n")

if successful < 10:
    print("⚠️ ОШИБКА: Загружено слишком мало акций!")
    print("   Возможные причины:")
    print("   1. Проблемы с сетью / proxy блокирует MOEX API")
    print("   2. API MOEX временно недоступен")
    print("   3. Изменился формат API")
    print("\n💡 Решение: используй Вариант 2 (загрузка CSV вручную)")
    exit(1)

# ===== ФОРМИРОВАНИЕ ЕДИНОЙ ТАБЛИЦЫ ЦЕН =====
print("🔄 Формирование единой таблицы цен...")

# Находим общие даты для всех акций
all_dates = None
for ticker, df in prices_data.items():
    dates = set(df['date'])
    if all_dates is None:
        all_dates = dates
    else:
        all_dates = all_dates.intersection(dates)

common_dates = sorted(list(all_dates))
print(f"   Общих торговых дней: {len(common_dates)}\n")

# Создаем DataFrame с ценами
prices = pd.DataFrame(index=pd.to_datetime(common_dates))

for ticker, df in prices_data.items():
    df_indexed = df.set_index('date')
    prices[ticker] = df_indexed['close']

# Удаляем строки с пропусками
prices = prices.dropna()

print(f"✅ Таблица цен готова:")
print(f"   Акций: {len(prices.columns)}")
print(f"   Торговых дней: {len(prices)}")
print(f"   Период: {prices.index[0].date()} - {prices.index[-1].date()}\n")

# ===== СОХРАНЕНИЕ =====
output_file = 'moex_prices.csv'
prices.to_csv(output_file)
print(f"💾 Данные сохранены в файл: {output_file}\n")

# ===== БЫСТРАЯ СТАТИСТИКА =====
print("📊 Статистика данных:")
returns = prices.pct_change().dropna()
print(f"   Средняя дневная доходность: {returns.mean().mean() * 100:.4f}%")
print(f"   Средняя волатильность: {returns.std().mean() * 100:.2f}%")
print(
    f"   Корреляция между акциями: {returns.corr().values[np.triu_indices_from(returns.corr().values, k=1)].mean():.3f}")

# Топ-5 акций по доходности
total_returns = (prices.iloc[-1] / prices.iloc[0] - 1) * 100
top5 = total_returns.nlargest(5)
print(f"\n   📈 Топ-5 акций по доходности:")
for ticker, ret in top5.items():
    print(f"      {ticker}: {ret:+.1f}%")

bottom5 = total_returns.nsmallest(5)
print(f"\n   📉 Худшие 5 акций:")
for ticker, ret in bottom5.items():
    print(f"      {ticker}: {ret:+.1f}%")

print("\n" + "=" * 60)
print("✅ ГОТОВО! Теперь можно запустить бэктест:")
print("   1. Открой файл 'momentum_backtest_real_data.py'")
print("   2. Запусти его - он автоматически загрузит 'moex_prices.csv'")
print("=" * 60)
