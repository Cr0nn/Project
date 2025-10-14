import sys
from PyQt6.QtWidgets import QApplication, QMainWindow, QVBoxLayout, QWidget, QPushButton, QTableWidget, QTableWidgetItem, QLabel, QComboBox, QGroupBox, QGridLayout, QHBoxLayout
from PyQt6.QtCore import Qt
from visualization.table_widget import create_table_widget, update_table_data
from db.MongoDB_handler import find_info, get_all_em_id, get_em_name, find_id_by_name, get_base_info
from visualization.data_loader import get_sample_data
from visualization.em_layout import update_base_info

class StockAnalyzerApp(QMainWindow):
    def __init__(self):
            super().__init__()
            self.setWindowTitle("Stock Analyzer")
            self.setGeometry(100, 100, 1000, 600)

            central_widget = QWidget()
            self.setCentralWidget(central_widget)
            main_layout = QVBoxLayout(central_widget)
            center_layout = QHBoxLayout(central_widget)

            # Выпадающий список эмитентов
            self.emitter_combo = QComboBox()
            self.emitter_combo.addItems(get_em_name(get_all_em_id()))
            self.emitter_combo.currentTextChanged.connect(self.parse_data)  # Исправлено: передаём метод, а не вызов
            main_layout.addWidget(self.emitter_combo)




            # Таблица
            self.table = create_table_widget()
            center_layout.addWidget(self.table)

            # Блок информации об эмитенте
            self.emitter_group = QGroupBox("Информация об эмитенте")
            self.emitter_group.setMaximumHeight(150)
            self.emitter_group.setFixedWidth(300)
            self.emitter_layout = QGridLayout(self.emitter_group)
            self.emitter_name_label = QLabel("Название: Не указано")
            self.emitter_id_label = QLabel("ID: Не указано")
            self.emitter_sector_label = QLabel("Сектор: Не указано")
            self.emitter_inn_label = QLabel("ИНН: Не указано")
            self.emitter_okpo_label = QLabel("ОКПО: Не указано")
            self.emitter_layout.addWidget(self.emitter_name_label, 0, 0, alignment=Qt.AlignmentFlag.AlignLeft)
            self.emitter_layout.addWidget(self.emitter_sector_label, 1, 0, alignment=Qt.AlignmentFlag.AlignLeft)
            self.emitter_layout.addWidget(self.emitter_inn_label, 2, 0, alignment=Qt.AlignmentFlag.AlignLeft)
            self.emitter_layout.addWidget(self.emitter_okpo_label, 3, 0, alignment=Qt.AlignmentFlag.AlignLeft)
            center_layout.addWidget(self.emitter_group, alignment=Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)


            main_layout.addLayout(center_layout)
            self.parse_data()

    def parse_data(self, name=None):
        if name is None:
            name = self.emitter_combo.currentText()
        id =  find_id_by_name(name)
        docs = find_info(id)
        base_info = get_base_info(id)
        for i in docs:
            data = i[id]
        del data["Период"]
        format_data = get_sample_data(data)
        update_table_data(self.table, format_data)
        update_base_info(self, base_info)

def start():
    app = QApplication(sys.argv)
    window = StockAnalyzerApp()
    window.show()
    sys.exit(app.exec())
