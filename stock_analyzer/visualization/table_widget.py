from PySide6.QtWidgets import QTableWidget, QTableWidgetItem, QHeaderView
from PySide6.QtCore import Qt, QTimer
import pandas as pd

def create_table_widget():
    table = QTableWidget()
    table.setRowCount(0)
    table.setColumnCount(0)

    table.setEditTriggers(QTableWidget.NoEditTriggers)


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

    def adjust_width():
        header = table.horizontalHeader()
        total_width = sum(header.sectionSize(i) for i in range(table.columnCount()))
        # добавляем рамки и микроотступы
        total_width += table.verticalHeader().width() 
        total_width += table.frameWidth() * 2  
        total_width += 18  # небольшой запас, чтобы точно не обрезало
        table.setFixedWidth(total_width)

    QTimer.singleShot(0, adjust_width)
