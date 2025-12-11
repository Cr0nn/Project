from PySide6.QtWidgets import QWidget, QVBoxLayout
from matplotlib.figure import Figure
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
import datetime
import matplotlib.dates as mdates
import numpy as np


class GraphWidget(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)

        # === Создаём Figure ===
        self.figure = Figure(figsize=(5, 4))
        self.canvas = FigureCanvas(self.figure)
        self.canvas.setFixedSize(1000, 600)
        self.ax = self.figure.add_subplot(111)

        layout = QVBoxLayout(self)
        layout.addWidget(self.canvas)

        # Параметры для стабильности
        self._last_y_min = None
        self._last_y_max = None
        self._last_step = None

    def plot_price(self, data):
        """prices — список из 60 чисел."""
        
        # ---- Подготовка ----
        prices = np.array(data[0]["price"], dtype=float)
        time = data[0]['timestamp']


        # ---- Сглаживание ----
        window = 1 
        if len(prices) > window:
            kernel = np.ones(window) / window
            smoothed = np.convolve(prices, kernel, mode='same')
        else:
            smoothed = prices.copy()

        self.ax.clear()

        # ---- Построение ----
        self.ax.xaxis.set_major_locator(mdates.MinuteLocator(interval=5))
        self.ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
        self.ax.plot(time, smoothed, linewidth=2, label="Цена (сглажено)")
        self.ax.plot(time, prices, alpha=0.3, label="Цена (сырые данные)")

        self.ax.set_title("Динамика цены за последний час")
        self.ax.set_xlabel("Время (минуты)")
        self.ax.set_ylabel("Цена")
        self.ax.grid(True)
        self.ax.legend(loc="upper left")
        
        for label in self.ax.get_xticklabels():
            label.set_rotation(45)
            label.set_horizontalalignment("right")


        y_min_raw = min(prices)
        y_max_raw = max(prices)

        # Нормализуем границы
        step = self.nice_step(y_min_raw, y_max_raw)

        y_min = np.floor(y_min_raw / step) * step
        y_max = np.ceil(y_max_raw / step) * step

        self.ax.set_ylim(y_min, y_max)
        self.ax.set_yticks(np.arange(y_min, y_max + step, step))

        # Если диапазон меньше одного шага — делаем его минимум два шага
        if abs(y_max - y_min) < step:
            y_max = y_min + step * 2

        # ---- Фиксация оси — гарантирует НЕ ломается при ресайзе ----
        self.ax.set_ylim(y_min, y_max)
        self.ax.set_yticks(np.arange(y_min, y_max + step, step))
        self.ax.set_autoscaley_on(False)
        self.ax.set_aspect('auto', adjustable='box')

        # ---- Подгоняем макет ----
        self.figure.tight_layout()

        self.canvas.draw_idle()

        # ---- Сохраняем параметры (для обработки ресайза) ----
        self._last_y_min = y_min
        self._last_y_max = y_max
        self._last_step = step
        


    def nice_step(self, y_min, y_max, target_ticks=8):
        diff = y_max - y_min
        if diff <= 0:
            return 1  # fallback

        raw_step = diff / target_ticks

        nice_values = [1, 2, 5]

        power = 10 ** np.floor(np.log10(raw_step))

        norm = raw_step / power

        nice_norm = min(nice_values, key=lambda x: abs(x - norm))

        # Финальный красивый шаг
        return nice_norm * power
