from PyQt6.QtWidgets import QTableWidget, QTableWidgetItem
from PyQt6.QtCore import Qt
import pandas as pd

def create_table_widget():
    table = QTableWidget()
    table.setRowCount(0)
    table.setColumnCount(0)
    table.setFixedWidth(500)

    return table

def update_table_data(table, df):
    table.setRowCount(len(df))
    table.setColumnCount(len(df.columns))
    table.setHorizontalHeaderLabels(df.columns.tolist())
    for i in range(len(df)):
        table.setVerticalHeaderItem(i, QTableWidgetItem(str(df.index[i])))
        for j in range(len(df.columns)):
            item = QTableWidgetItem(str(df.iloc[i, j]))
            table.setItem(i, j, item)
    table.resizeColumnsToContents()
    table.setVisible(True)