#Основной файл визуализации
import sys
from numpy import average
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QVBoxLayout, QWidget, QLabel, QComboBox,
    QGroupBox, QGridLayout, QHBoxLayout, QCheckBox, QMessageBox,
    QRadioButton, QButtonGroup, QTableWidget, QTabWidget
)
from PySide6.QtCore import Qt, QSignalBlocker, QTimer
from visualization.table_widget import create_table_widget, update_table_data
from db.MongoDB_handler import (
    find_info, get_all_em_id, get_em_name, find_id_by_name, get_base_info,
    get_companies_in_sector, div_filter, PE_filter, debt_filter, ROE_filter,
    get_last_hour_price
)
from data.filter import (
    apply_filters, apply_PE_mode, apply_debt_mode, apply_ROE_mode, 
    parse_data
)
from visualization.graph import GraphWidget
from utils.helpers import get_all_empty_sectors
from visualization.data_loader import get_sample_data
from visualization.em_layout import update_base_info

class StockAnalyzerApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Stock Analyzer")
        self.setGeometry(100, 100, 1000, 800)

        tabs = QTabWidget()
        self.setCentralWidget(tabs)

        #------------------------------------- Первая вкладка(таблица) ---------------------------------------------------------------
        page_1 = QWidget()
        page_1_layout = QVBoxLayout(page_1)

        main_layout_1 = QVBoxLayout()
        center_layout_1 = QHBoxLayout()

        page_1_layout.addLayout(main_layout_1)
        tabs.addTab(page_1, "Таблица")

        # --- Фильтры ---
        self.filter_group = QGroupBox("Фильтрация компаний")
        self.filter_layout = QGridLayout(self.filter_group)

        # Сектор
        self.filter_box = QComboBox()
        self.filter_box.addItems(get_all_empty_sectors())
        self.filter_box.model().sort(0)
        self.filter_box.setCurrentText("Все секторы")
        self.filter_box.currentTextChanged.connect(self.set_sector)
        self.filter_layout.addWidget(self.filter_box)

        # Чекбокс: дивиденды
        self.div_check = QCheckBox("Платят дивиденды")
        self.div_check.stateChanged.connect(self.change_divCB)
        self.filter_layout.addWidget(self.div_check)

        # --- Группа PE ---
        self.PE_group_box = QGroupBox("P/E фильтрация")
        self.PE_group_layout = QVBoxLayout(self.PE_group_box)
        self.PE_filter_layout = QHBoxLayout(self.PE_group_box)
        self.PE_group_button = QButtonGroup(self)

        self.PE_CB = QCheckBox("P/E фильтрация")
        self.PE_low = QRadioButton("P/E за последний год ниже среднего")
        self.PE_avg = QRadioButton("Все компании")
        self.PE_high = QRadioButton("P/E за последний год выше среднего")

        self.PE_group_layout.addWidget(self.PE_CB)
        self.PE_filter_layout.addWidget(self.PE_low)
        self.PE_filter_layout.addWidget(self.PE_avg)
        self.PE_filter_layout.addWidget(self.PE_high)
        self.PE_group_layout.addLayout(self.PE_filter_layout)

        for btn in (self.PE_low, self.PE_avg, self.PE_high):
            self.PE_group_button.addButton(btn)

        self.PE_group_button.buttonClicked.connect(self.on_radio_changed)
        self.PE_CB.stateChanged.connect(self.change_PECB)
        self.filter_layout.addWidget(self.PE_group_box)

        self.set_active_radio(self.PE_CB, self.PE_group_button)

        # --- Долговая нагрузка фильтр
        self.debt_group_box = QGroupBox("Долговая нагрузка фильтрация")
        self.debt_group_layout = QVBoxLayout(self.debt_group_box)
        self.debt_filter_layout = QHBoxLayout(self.debt_group_box)
        self.debt_group_button = QButtonGroup(self)

        self.debt_CB = QCheckBox("Долговая нагрузка")
        self.debt_last = QRadioButton("Последний год")
        self.debt_avg = QRadioButton("Среднее за 4 года")

        self.debt_group_layout.addWidget(self.debt_CB)
        self.debt_filter_layout.addWidget(self.debt_last)
        self.debt_filter_layout.addWidget(self.debt_avg)
        self.debt_group_layout.addLayout(self.debt_filter_layout)

        for btn in (self.debt_avg, self.debt_last):
            self.debt_group_button.addButton(btn)

        self.debt_group_button.buttonClicked.connect(self.on_radio_changed)
        self.debt_CB.stateChanged.connect(self.change_debt)
        self.filter_layout.addWidget(self.debt_group_box)

        self.set_active_radio(self.debt_CB, self.debt_group_button)


        # --- ROE фильтр
        self.ROE_group_box = QGroupBox("ROE фильтр")
        self.ROE_group_layout = QVBoxLayout(self.ROE_group_box)
        self.ROE_filter_layout = QHBoxLayout(self.ROE_group_box)
        self.ROE_group_button = QButtonGroup(self)

        self.ROE_CB = QCheckBox("ROE фильтрация")
        self.ROE_low = QRadioButton("ROE за последний год ниже среднего")
        self.ROE_avg = QRadioButton("Все компании")
        self.ROE_high = QRadioButton("ROE  за последний год выше среднего")

        self.ROE_group_layout.addWidget(self.ROE_CB)
        self.ROE_filter_layout.addWidget(self.ROE_low)
        self.ROE_filter_layout.addWidget(self.ROE_avg)
        self.ROE_filter_layout.addWidget(self.ROE_high)
        self.ROE_group_layout.addLayout(self.ROE_filter_layout)
        
        for btn in (self.ROE_high, self.ROE_avg, self.ROE_low):
            self.ROE_group_button.addButton(btn)

        self.ROE_group_button.buttonClicked.connect(self.on_radio_changed)
        self.ROE_CB.stateChanged.connect(self.change_ROECB)
        self.filter_layout.addWidget(self.ROE_group_box)

        self.set_active_radio(self.ROE_CB, self.ROE_group_button)

        main_layout_1.addWidget(self.filter_group)

        # --- Список эмитентов ---
        self.emitter_combo = QComboBox()
        self.emitter_combo.addItems(get_em_name(get_all_em_id()))
        self.emitter_combo.currentTextChanged.connect(lambda: parse_data(self))
        main_layout_1.addWidget(self.emitter_combo)

        # --- Таблица ---
        self.table = create_table_widget()
        center_layout_1.addWidget(self.table)

        # --- Блок информации об эмитенте ---
        self.emitter_group = QGroupBox("Информация об эмитенте")
        self.emitter_group.setMaximumHeight(150)
        self.emitter_group.setFixedWidth(450)
        self.emitter_layout = QGridLayout(self.emitter_group)

        self.emitter_name_label = QLabel("Название: Не указано")
        self.emitter_id_label = QLabel("ID: Не указано")
        self.emitter_sector_label = QLabel("Сектор: Не указано")
        self.emitter_inn_label = QLabel("ИНН: Не указано")
        self.emitter_okpo_label = QLabel("ОКПО: Не указано")

        labels = [
            self.emitter_name_label, self.emitter_id_label,
            self.emitter_sector_label, self.emitter_inn_label,
            self.emitter_okpo_label
        ]
        for i, lbl in enumerate(labels):
            self.emitter_layout.addWidget(lbl, i, 0, alignment=Qt.AlignmentFlag.AlignLeft)

        center_layout_1.addWidget(self.emitter_group, alignment=Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)
        main_layout_1.addLayout(center_layout_1)

        # ---------------------------------------------- Страница 2 - графики -------------------------------------------------------------------

        page_2 = QWidget()
        page_2_layout = QVBoxLayout(page_2)

        main_layout_2 = QVBoxLayout()
        center_layout_2 = QVBoxLayout()

        page_2_layout.addLayout(main_layout_2)
        tabs.addTab(page_2, "Графики")
        self.tickers_combo = QComboBox()
        self.tickers_combo.addItems(["SBER", "GAZP", "YDEX"])
        self.tickers_combo.currentTextChanged.connect(self.change_ticker_combo)


        self.graph = GraphWidget()
        center_layout_2.addWidget(self.tickers_combo)
        center_layout_2.addWidget(self.graph)

        main_layout_2.addLayout(center_layout_2)

        # --- Инициализация ---
        self._last_good_state = None
        
        parse_data(self)
        # self.graph.plot_price(get_last_hour_price(self.tickers_combo.currentText()))


    # ------------------------ ОБРАБОТЧИКИ UI -------------------

    def change_PECB(self):
        self.set_active_radio(self.PE_CB, self.PE_group_button)
        self.debt_CB.setEnabled(not(self.PE_CB.isChecked()))
        self.ROE_CB.setEnabled(not(self.PE_CB.isChecked()))
        apply_PE_mode(self)

    def change_ticker_combo(self):
        self.graph.plot_price(get_last_hour_price(self.tickers_combo.currentText()))

    def change_debt(self):
        self.set_active_radio(self.debt_CB, self.debt_group_button)
        self.PE_CB.setEnabled(not(self.debt_CB.isChecked()))
        self.ROE_CB.setEnabled(not(self.debt_CB.isChecked()))
        apply_debt_mode(self)

    def change_divCB(self):
        if self.PE_CB.isChecked():
            apply_PE_mode(self)
        elif self.debt_CB.isChecked():
            apply_debt_mode(self)
        elif self.ROE_CB.isChecked():
            apply_ROE_mode(self)
        else:
            apply_filters(self)

    def change_ROECB(self):
        self.set_active_radio(self.ROE_CB, self.ROE_group_button)
        self.debt_CB.setEnabled(not(self.ROE_CB.isChecked()))
        self.PE_CB.setEnabled(not(self.ROE_CB.isChecked()))
        apply_ROE_mode(self)


    def set_sector(self):
        if self.PE_CB.isChecked():
            apply_PE_mode(self)
        elif self.debt_CB.isChecked():
            apply_debt_mode(self)
        elif self.ROE_CB.isChecked():
            apply_ROE_mode(self)
        else:
            apply_filters(self)

    def on_radio_changed(self, button):
        if button in self.PE_group_button.buttons():
            apply_PE_mode(self)
        elif button in self.debt_group_button.buttons():
            apply_debt_mode(self)
        elif button in self.ROE_group_button.buttons():
            apply_ROE_mode(self)

    # ------------------------ ВСПОМОГАТЕЛЬНЫЕ ------------------

    def set_active_radio(self, CB = QCheckBox, group_button = QButtonGroup):
        isChecked = CB.isChecked()
        for btn in group_button.buttons():
            btn.setEnabled(isChecked)

    def get_active_rb(self, group_button = QButtonGroup):
        for btn in group_button.buttons():
            if btn.isChecked():
                return btn

    def save_state(self):
        """Сохраняет минимальный снимок корректного состояния UI."""
        rb_pe = self.get_active_rb(self.PE_group_button)
        active_rb_pe = rb_pe.text() if rb_pe else None
        
        rb_debt = self.get_active_rb(self.debt_group_button)
        active_rb_debt = rb_debt.text() if rb_debt else None
        
        rb_roe = self.get_active_rb(self.ROE_group_button)
        active_rb_roe = rb_roe.text() if rb_roe else None

        self._last_good_state = {
            "pe_checked": self.PE_CB.isChecked(),
            "div_checked": self.div_check.isChecked(),
            "debt_checked": self.debt_CB.isChecked(),
            "roe_checked": self.ROE_CB.isChecked(),
            "active_rb_pe": active_rb_pe,
            "active_rb_debt": active_rb_debt,
            "active_rb_roe": active_rb_roe,
            "combo_items": [self.emitter_combo.itemText(i) for i in range(self.emitter_combo.count())],
            "combo_current": self.emitter_combo.currentText(),
            "combo_enabled": self.emitter_combo.isEnabled(),
            "sector_current": self.filter_box.currentText()
        }

    def restore_state(self):
        """Восстановление последнего успешного состания в случае, если пользователь задал невозможный вариант фильтрации"""
        if not self._last_good_state:
            return
        st = self._last_good_state

        self.filter_box.blockSignals(True)
        self.emitter_combo.blockSignals(True)

        with QSignalBlocker(self.PE_CB):
            self.PE_CB.setChecked(st["pe_checked"])

        with QSignalBlocker(self.div_check):
            self.div_check.setChecked(st["div_checked"])

        with QSignalBlocker(self.debt_CB):
            self.debt_CB.setChecked(st["debt_checked"])

        with QSignalBlocker(self.debt_CB):
            self.ROE_CB.setChecked(st["roe_checked"])

        for b in self.PE_group_button.buttons():
            with QSignalBlocker(b):
                b.setChecked(b.text() == st.get("active_rb_pe"))

        for b in self.debt_group_button.buttons():
            with QSignalBlocker(b):
                b.setChecked(b.text() == st.get("active_rb_debt"))
        
        for b in self.ROE_group_button.buttons():
            with QSignalBlocker(b):
                b.setChecked(b.text() == st.get("active_rb_roe"))


        self.emitter_combo.clear()
        self.emitter_combo.addItems(st["combo_items"])
        if st["combo_current"] in st["combo_items"]:
            self.emitter_combo.setCurrentText(st["combo_current"])
        elif self.emitter_combo.count() > 0:
            self.emitter_combo.setCurrentIndex(0)
        self.emitter_combo.setEnabled(st["combo_enabled"])

        self.filter_box.setCurrentText(st["sector_current"])

        self.filter_box.blockSignals(False)
        self.emitter_combo.blockSignals(False)

        if self.PE_CB.isChecked():
            apply_PE_mode(self)
        elif self.debt_CB.isChecked():
            apply_debt_mode(self)
        elif self.ROE_CB.isChecked():
            apply_ROE_mode(self)
        else:
            apply_filters(self)

        QApplication.processEvents()

    def handle_no_results(self):
        self.restore_state()
        QMessageBox.warning(self, "Предупреждение", "Не найдено ни одной компании с заданными фильтрами")

# -------------------------- START -------------------------------

def start():
    app = QApplication(sys.argv)
    window = StockAnalyzerApp()
    window.show()
    sys.exit(app.exec())

