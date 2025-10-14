import sys
from PyQt6.QtWidgets import QApplication, QMainWindow, QVBoxLayout, QWidget, QPushButton, QTableWidget, QTableWidgetItem, QLabel, QComboBox, QGroupBox, QGridLayout
from PyQt6.QtCore import Qt
from visualization.table_widget import create_table_widget, update_table_data
from db.MongoDB_handler import find_info, get_all_em_id, get_em_name, find_id_by_name, get_base_info
from visualization.data_loader import get_sample_data

def update_base_info(self, info):
    self.emitter_name_label.setText(info["name"]) 
    self.emitter_id_label.setText(f'ID: {info["id"]}')  
    self.emitter_sector_label.setText(f'Сектор: {info["sector"]}')
    self.emitter_inn_label.setText(f'ИНН: {info["inn"]}')
    self.emitter_okpo_label.setText(f'ОКПО: {info["okpo"]}')