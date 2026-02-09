from PySide6.QtWidgets import QWidget, QVBoxLayout, QSizePolicy
from matplotlib.figure import Figure
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
import datetime
import matplotlib.dates as mdates
import numpy as np


class BaseGraphWidget(QWidget):

    def __init__(self, parent=None):
        super().__init__(parent)

        self.figure = Figure(figsize=(5, 4))
        self.canvas = FigureCanvas(self.figure)
        self.canvas.setSizePolicy(
            QSizePolicy.Policy.Expanding,
            QSizePolicy.Policy.Expanding
        )

        self.ax = self.figure.add_subplot(111)

        layout = QVBoxLayout(self)
        layout.addWidget(self.canvas)

        self.ax.grid(True)

    def redraw(self):
        self.canvas.draw_idle()

class YAxisScalingMixin:

    def nice_step(self, y_min, y_max, target_ticks=8):
        diff = y_max - y_min
        if diff <= 0:
            return 1

        raw_step = diff / target_ticks
        nice_values = [1, 2, 5]

        power = 10 ** np.floor(np.log10(raw_step))
        norm = raw_step / power

        nice_norm = min(nice_values, key=lambda x: abs(x - norm))
        return nice_norm * power

    def y_ticks(self, prices, n=10, padding_ratio=0.1):
        y_min = prices.min()
        y_max = prices.max()

        if y_min == y_max:
            return np.linspace(y_min - 1, y_max + 1, n + 1)

        # === диапазон ===
        diff = y_max - y_min

        # === padding (10%) ===
        padding = diff * padding_ratio
        y_min -= padding
        y_max += padding

        # === шаг ===
        step = (y_max - y_min) / n
        power = 10 ** np.floor(np.log10(step))
        step = power * np.ceil(step / power)

        # === округляем границы ===
        y_min = np.floor(y_min / step) * step
        y_max = np.ceil(y_max / step) * step

        return np.arange(y_min, y_max + step, step)
        
class PriceGraphWidget(BaseGraphWidget, YAxisScalingMixin):

    def __init__(self, parent=None):
        super().__init__(parent)

        self.line_smooth, = self.ax.plot(
            [], [], linewidth=2, label="Цена (сглажено)"
        )
        self.line_raw, = self.ax.plot(
            [], [], alpha=0.3, label="Цена (сырые данные)"
        )

        self.ax.set_title("Динамика цены за последний час")
        self.ax.set_xlabel("Время")
        self.ax.set_ylabel("Цена")
        self.ax.legend(loc="upper left")

        self.ax.xaxis.set_major_locator(mdates.MinuteLocator(interval=5))
        self.ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))

        # Для будущей стабилизации масштаба (как у тебя)
        self._last_y_min = None
        self._last_y_max = None
        self._last_step = None

    def plot_price(self, data):
        if not data:
            return
        
        # Берем только ПОСЛЕДНИЕ 12 точек (или сколько нужно)
        MAX_POINTS = 12
        recent_data = data[-MAX_POINTS:]  # ← ВАЖНО!
        
        prices = np.array([i["price"] for i in recent_data])
        times = [i["_id"] for i in recent_data]
        
        # Далее твой обычный код:
        x = np.arange(len(prices))
        
        self.line_raw.set_data(x, prices)
        self.line_smooth.set_data(x, prices)
        
        self.ax.set_xlim(0, len(x) - 1)
        
        # X axis labels - только для этих 12 точек
        if len(times) <= 12:
            # Показываем все метки
            xticks = x
            xlabels = [t.strftime("%H:%M") for t in times]
        else:
            # Показываем каждую N-ю метку
            step = max(1, len(x) // 6)  # 6 меток максимум
            xticks = x[::step]
            xlabels = [times[i].strftime("%H:%M") for i in range(0, len(times), step)]
        
        self.ax.set_xticks(xticks)
        self.ax.set_xticklabels(xlabels)
        
        # ВРАЩЕНИЕ И ВЫРАВНИВАНИЕ МЕТОК!
        for label in self.ax.get_xticklabels():
            label.set_rotation(45)
            label.set_horizontalalignment("right")
        
        # Ось Y (этого не было в твоем коде!)
        ticks = self.y_ticks(prices, 10)
        self.ax.set_ylim(ticks[0], ticks[-1])
        self.ax.set_yticks(ticks)
        
        self.redraw()  # ← Добавить!



class TechnicalAnalysisGraphWidget(PriceGraphWidget):

    def __init__(self, parent=None):
        super().__init__(parent)

        self.support_lines = []
        self.resistance_lines = []
        self.trend_lines = []

    # ===== Заготовки под TA =====

    def draw_support(self, y):
        line = self.ax.axhline(
            y, linestyle="--", color="green", alpha=0.6, label="Поддержка"
        )
        self.support_lines.append(line)
        self.redraw()

    def draw_resistance(self, y):
        line = self.ax.axhline(
            y, linestyle="--", color="red", alpha=0.6, label="Сопротивление"
        )
        self.resistance_lines.append(line)
        self.redraw()

    def draw_trend(self, x1, y1, x2, y2):
        line, = self.ax.plot(
            [x1, x2], [y1, y2], color="blue", linewidth=1.5, label="Тренд"
        )
        self.trend_lines.append(line)
        self.redraw()

class MultiPanelGraphWidget(QWidget, YAxisScalingMixin):

    def __init__(self, parent=None):
        super().__init__(parent)

        # === Figure / Canvas ===
        self.figure = Figure(figsize=(10, 8))
        self.canvas = FigureCanvas(self.figure)

        self.canvas.setSizePolicy(
            QSizePolicy.Policy.Expanding,
            QSizePolicy.Policy.Expanding
        )

        layout = QVBoxLayout(self)
        layout.addWidget(self.canvas)

        # === GridSpec (пропорции как в терминалах) ===
        gs = self.figure.add_gridspec(
            3, 1,
            height_ratios=[4, 1.5, 1.5],
            hspace=0.15
        )

        self.ax_price = self.figure.add_subplot(gs[0])
        self.ax_adx   = self.figure.add_subplot(gs[1], sharex=self.ax_price)
        self.ax_rsi   = self.figure.add_subplot(gs[2], sharex=self.ax_price)

        # === оформление ===
        self.ax_price.set_title("Цена")
        self.ax_adx.set_ylabel("ADX")
        self.ax_rsi.set_ylabel("RSI")
        self.support_lines = []
        self.resistance_lines = []
        self.trend_lines = []

        for ax in (self.ax_price, self.ax_adx, self.ax_rsi):
            ax.grid(True)

        # скрываем X подписи у верхних панелей
        self.ax_price.tick_params(labelbottom=False)
        self.ax_adx.tick_params(labelbottom=False)
        
        self.ax_rsi.set_autoscale_on(False)
        self.ax_adx.set_autoscale_on(False)

        # === линии (создаются ОДИН РАЗ) ===
        self.price_line, = self.ax_price.plot([], [], linewidth=2, label="Цена")

        self.adx_line, = self.ax_adx.plot([], [], color="orange", label="ADX", clip_on=True)
        self.rsi_line, = self.ax_rsi.plot([], [], color="purple", label="RSI", clip_on=True)

        self.ax_price.legend(loc="upper left")

        # уровни RSI (один раз!)
        self.ax_rsi.axhline(30, linestyle="--", alpha=0.3)
        self.ax_rsi.axhline(70, linestyle="--", alpha=0.3)
        self.rsi_line.set_clip_on(True)

        RSI_MIN = 0
        RSI_MAX = 100
        RSI_PADDING = 5  # визуальный отступ

        self.ax_rsi.set_ylim(
            RSI_MIN - RSI_PADDING,
            RSI_MAX + RSI_PADDING
        )

        self.ax_rsi.set_yticks([0, 30, 50, 70, 100])
        self.ax_rsi.set_autoscale_on(False)

        ADX_MIN = 0
        ADX_MAX = 60
        ADX_PADDING = 3

        self.ax_adx.set_ylim(
            ADX_MIN - ADX_PADDING,
            ADX_MAX + ADX_PADDING
        )

        self.ax_adx.set_yticks([0, 20, 40, 60])
        self.ax_adx.set_autoscale_on(False)


        self.adx_line.set_clip_on(True)


        self.ax_rsi.fill_between(
            [0, 1],
            70, 100,
            color="red",
            alpha=0.05,
            transform=self.ax_rsi.get_yaxis_transform()
        )

        self.ax_rsi.fill_between(
            [0, 1],
            0, 30,
            color="green",
            alpha=0.05,
            transform=self.ax_rsi.get_yaxis_transform()
        )



    def update_price(self, data):
        if data is None or len(data) == 0:
            return
        
        prices = np.array([i["price"] for i in data])
        times = [i["_id"] for i in data]
        
        # Индексы вместо времени на оси X
        x_indices = np.arange(len(prices))
        
        self.price_line.set_data(x_indices, prices)
        self.ax_price.set_xlim(0, len(prices) - 1)
        
        # Устанавливаем 12 меток на нижней панели
        self.set_12_xticks_for_multipanel(x_indices, times)
        
        # Ось Y
        ticks = self.y_ticks(prices, 10)
        self.ax_price.set_ylim(ticks[0], ticks[-1])
        self.ax_price.set_yticks(ticks)

    def update_all(self, data):
        """
        Обновляет ВСЕ панели за один вызов.
        data: list[dict] -> [{ "_id": datetime, "price": float }, ...]
        """
        self.reset_ta()

        if data is None or len(data) == 0:
            return
        
        # Извлекаем данные
        prices = np.array([i["price"] for i in data])
        times = [i["_id"] for i in data]
        
        # 1. Используем ИНДЕКСЫ везде
        x_indices = np.arange(len(prices))
        
        # 2. Обновляем цену с индексами
        self.price_line.set_data(x_indices, prices)
        self.ax_price.set_xlim(0, len(prices) - 1)
        
        # 3. Обновляем RSI с индексами
        self.update_rsi(prices, x_indices)
        
        # 4. Обновляем ADX с индексами
        self.update_adx(prices, x_indices)
        
        # 5. Устанавливаем подписи оси X
        self.set_12_xticks_for_multipanel(x_indices, times)
        
        # 6. Ось Y для цены
        ticks = self.y_ticks(prices, 10)
        self.ax_price.set_ylim(ticks[0], ticks[-1])
        self.ax_price.set_yticks(ticks)
        
        # 7. Перерисовываем
        VISIBLE_POINTS = 60

        prices_visible = prices[-VISIBLE_POINTS:]

        self.update_ta_lines(prices_visible)
        self.redraw()

    def update_rsi(self, prices, x_indices, period=14):
        if len(prices) < period + 1:
            return

        deltas = np.diff(prices)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)

        avg_gain = np.convolve(gains, np.ones(period) / period, mode="valid")
        avg_loss = np.convolve(losses, np.ones(period) / period, mode="valid")

        rs = avg_gain / (avg_loss + 1e-9)
        rsi = 100 - (100 / (1 + rs))

        rsi = np.clip(rsi, 0, 100)

        rsi_indices = x_indices[-len(rsi):]

        self.rsi_line.set_data(rsi_indices, rsi)

    def update_adx(self, prices, x_indices, period=14):
        if len(prices) < 2 * period + 1:
            return

        high = prices
        low = prices
        close = prices

        tr = np.maximum.reduce([
            high[1:] - low[1:],
            np.abs(high[1:] - close[:-1]),
            np.abs(low[1:] - close[:-1]),
        ])

        plus_dm = np.maximum(high[1:] - high[:-1], 0)
        minus_dm = np.maximum(low[:-1] - low[1:], 0)

        plus_dm[plus_dm < minus_dm] = 0
        minus_dm[minus_dm < plus_dm] = 0

        tr_smooth = np.convolve(tr, np.ones(period), mode="valid")
        plus_dm_smooth = np.convolve(plus_dm, np.ones(period), mode="valid")
        minus_dm_smooth = np.convolve(minus_dm, np.ones(period), mode="valid")

        plus_di = 100 * plus_dm_smooth / (tr_smooth + 1e-9)
        minus_di = 100 * minus_dm_smooth / (tr_smooth + 1e-9)

        dx = 100 * np.abs(plus_di - minus_di) / (plus_di + minus_di + 1e-9)

        adx = np.convolve(dx, np.ones(period) / period, mode="valid")

        adx = np.clip(adx, 0, 60)

        adx_indices = x_indices[-len(adx):]

        self.adx_line.set_data(adx_indices, adx)



    def set_12_xticks_for_multipanel(self, x_indices, times):
        """Устанавливает 12 меток для всех панелей"""
        if len(x_indices) == 0:
            return
        
        # Выбираем 12 индексов равномерно
        n_ticks = min(12, len(x_indices))
        
        # Если данных мало - показываем все
        if len(x_indices) <= n_ticks:
            xticks = x_indices
            xlabels = [t.strftime("%H:%M") for t in times]
        else:
            # Равномерно распределяем
            step = len(x_indices) / n_ticks
            xticks = []
            xlabels = []
            
            for i in range(n_ticks):
                idx = int(i * step)
                if idx < len(x_indices):
                    xticks.append(x_indices[idx])
                    xlabels.append(times[idx].strftime("%H:%M"))
        
        # Устанавливаем только на нижней панели
        self.ax_rsi.set_xticks(xticks)
        self.ax_rsi.set_xticklabels(xlabels)
        
        # Устанавливаем те же тики на других панелях (но без подписей)
        self.ax_price.set_xticks(xticks)
        self.ax_adx.set_xticks(xticks)
        
        # Поворот меток
        for label in self.ax_rsi.get_xticklabels():
            label.set_rotation(45)
            label.set_horizontalalignment("right")
            
    def clear_ta_lines(self):
        for line in (
            self.support_lines +
            self.resistance_lines +
            self.trend_lines
        ):
            line.remove()

        self.support_lines.clear()
        self.resistance_lines.clear()
        self.trend_lines.clear()

    def update_ta_lines(self, prices):
        # 1. Удаляем старые линии
        self.clear_ta_lines()

        if len(prices) < 5:
            return

        # 2. Экстремумы
        mins, maxs = find_local_extrema(prices)

        # 3. Поддержка / сопротивление
        supports = filter_levels([p for _, p in mins])
        resistances = filter_levels([p for _, p in maxs])

        current_price = prices[-1]

        supports_below = [s for s in supports if s < current_price]
        resistances_above = [r for r in resistances if r > current_price]

        if supports_below:
            y = max(supports)
            line = self.ax_price.axhline(
                y, linestyle="--", color="green", alpha=0.7
            )
            self.support_lines.append(line)

        if resistances_above:
            y = min(resistances)
            line = self.ax_price.axhline(
                y, linestyle="--", color="red", alpha=0.7
            )
            self.resistance_lines.append(line)

        # 4. Линия тренда (по минимумам)
        trend = calc_trend_from_extrema(mins)
        if trend:
            k, b = trend

            x0 = 0
            x1 = len(prices) - 1

            y0 = k * x0 + b
            y1 = k * x1 + b

            line, = self.ax_price.plot(
                [x0, x1],
                [y0, y1],
                color="blue",
                linewidth=1.8,
                alpha=0.8
            )
            self.trend_lines.append(line)

    def reset_ta(self):
        self.clear_ta_lines()

    def redraw(self):
        self.canvas.draw_idle()

def find_local_extrema(prices, window=3):
    mins = []
    maxs = []

    for i in range(window, len(prices) - window):
        segment = prices[i - window:i + window + 1]
        center = prices[i]

        if center == segment.min():
            mins.append((i, center))

        if center == segment.max():
            maxs.append((i, center))

    return mins, maxs

def filter_levels(levels, tolerance=0.003):
    if not levels:
        return []

    levels = sorted(levels)
    clustered = [[levels[0]]]

    for lvl in levels[1:]:
        if abs(lvl - np.mean(clustered[-1])) / lvl < tolerance:
            clustered[-1].append(lvl)
        else:
            clustered.append([lvl])

    return [np.mean(group) for group in clustered]

def calc_trend_from_extrema(extrema):
    if len(extrema) < 2:
        return None

    x = np.array([i for i, _ in extrema])
    y = np.array([p for _, p in extrema])

    k, b = np.polyfit(x, y, 1)
    return k, b
