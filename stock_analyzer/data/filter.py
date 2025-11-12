#Файл с основной логикой фильтрации
from PySide6.QtCore import Qt, QSignalBlocker, QTimer
from numpy import average, median
from db.MongoDB_handler import (
    find_info, get_all_em_id, get_em_name, find_id_by_name, get_base_info,
    get_companies_in_sector, div_filter, PE_filter, debt_filter, ROE_filter
)
from visualization.data_loader import get_sample_data
from visualization.em_layout import update_base_info
from visualization.table_widget import create_table_widget, update_table_data


def get_filtered_companies(window):
    """Возвращает актуальный список компаний с учётом всех включённых фильтров."""
    sector = window.filter_box.currentText()
    result = get_companies_in_sector(sector)

    if window.div_check.isChecked():
        result = div_filter(result)

    return result


def parse_data(window, name=None):
    "Информация о текущей компании из списка"
    if name is None:
        name = window.emitter_combo.currentText()
    id = find_id_by_name(name)
    docs = find_info(id)
    base_info = get_base_info(id)
    for i in docs:
        data = i[id]
    if "Период" in data:
        del data["Период"]

    format_data = get_sample_data(data)
    update_table_data(window.table, format_data)
    update_base_info(window, base_info)
    window.save_state()


def apply_filters(window):
    """Перестраивает список эмитентов без PE,ROE и debt фильтров."""
    result = get_filtered_companies(window)
    if not result:
        QTimer.singleShot(0, window.handle_no_results)
        return []

    window.emitter_combo.blockSignals(True)
    window.emitter_combo.clear()
    window.emitter_combo.addItems(result)
    window.emitter_combo.model().sort(0)
    window.emitter_combo.blockSignals(False)
    window.emitter_combo.setEnabled(True)

    parse_data(window,result[0])
    window.save_state()
    return result

def apply_PE_mode(window):
    """Активирует или обновляет режим PE анализа (меняет таблицу и UI)."""
    if not window.PE_CB.isChecked() or window.get_active_rb(window.PE_group_button) is None:
        # Обычный режим — вернуть фильтрацию
        window.emitter_combo.setEnabled(True)
        apply_filters(window)
        return

    companies = get_filtered_companies(window)
    if not companies:
        QTimer.singleShot(0, window.handle_no_results)
        return

    PE_data, avg = PE_filter(companies)
    if not PE_data:
        QTimer.singleShot(0, window.handle_no_results)
        return

    RB = window.get_active_rb(window.PE_group_button)
    if RB is window.PE_high:
        data = {
            "Компания": [k for k, v in PE_data.items() if v[0] > avg],
            "Среднее P/E за 4 года": [v[0] for k, v in PE_data.items() if v[0] > avg],
            "Среднее P/E за последний год": [v[1] for k, v in PE_data.items() if v[0] > avg],
        }
    elif RB is window.PE_low:
        data = {
            "Компания": [k for k, v in PE_data.items() if v[0] < avg],
            "Среднее P/E за 4 года": [v[0] for k, v in PE_data.items() if v[0] < avg],
            "Среднее P/E за последний год": [v[1] for k, v in PE_data.items() if v[0] < avg],
        }
    else:
        data = {
            "Компания": list(PE_data.keys()),
            "Среднее P/E за 4 года": [v[0] for v in PE_data.values()],
            "Среднее P/E за последний год": [v[1] for v in PE_data.values()],
        }

    update_table_data(window.table, get_sample_data(data))
    window.emitter_combo.setEnabled(False)
    window.save_state()

def apply_debt_mode(window):
    """Активирует или обновляет режим debt анализа (меняет таблицу и UI)."""
    if not window.debt_CB.isChecked() or window.get_active_rb(window.debt_group_button) is None:
        # Обычный режим — вернуть фильтрацию
        window.emitter_combo.setEnabled(True)
        apply_filters(window)
        return
    
    companies = get_filtered_companies(window)
    if not companies:
        QTimer.singleShot(0, window.handle_no_results)
        return
    
    debt_data = debt_filter(companies)
    if not debt_data:
        QTimer.singleShot(0, window.handle_no_results)
        return

    RB = window.get_active_rb(window.debt_group_button)
    keys = list((list(debt_data.values())[0]).keys())
    if RB is window.debt_avg:
        data = {
            "Компания": list(debt_data.keys()),
        }
        for i in keys:
            data[i] = [round(average(v[i]),2) for k,v in debt_data.items()]
    else:
        data = {
            "Компания": list(debt_data.keys()),
        }
        for i in keys:
            data[i] = [v[i][-1] for k,v in debt_data.items()]
    update_table_data(window.table, get_sample_data(data))
    window.emitter_combo.setEnabled(False)
    window.save_state()

def apply_ROE_mode(window):
    """Активирует или обновляет режим ROE анализа (меняет таблицу и UI)."""
    if not window.ROE_CB.isChecked() or window.get_active_rb(window.ROE_group_button) is None:
        window.emitter_combo.setEnabled(True)
        apply_filters(window)
        return

    companies = get_filtered_companies(window)
    if not companies:
        QTimer.singleShot(0, window.handle_no_results)
        return

    ROE_data, avg = ROE_filter(companies)
    if not ROE_data:
        QTimer.singleShot(0, window.handle_no_results)
        return

    RB = window.get_active_rb(window.ROE_group_button)
    if RB is window.ROE_high:
        data = {
            "Компания": [k for k, v in ROE_data.items() if v[0] > avg],
            "Среднее P/E за 4 года": [v[0] for k, v in ROE_data.items() if v[0] > avg],
            "Среднее P/E за последний год": [v[1] for k, v in ROE_data.items() if v[0] > avg],
        }
    elif RB is window.ROE_low:
        data = {
            "Компания": [k for k, v in ROE_data.items() if v[0] < avg],
            "Среднее P/E за 4 года": [v[0] for k, v in ROE_data.items() if v[0] < avg],
            "Среднее P/E за последний год": [v[1] for k, v in ROE_data.items() if v[0] < avg],
        }
    else:
        data = {
            "Компания": list(ROE_data.keys()),
            "Среднее P/E за 4 года": [v[0] for v in ROE_data.values()],
            "Среднее P/E за последний год": [v[1] for v in ROE_data.values()],
        }

    update_table_data(window.table, get_sample_data(data))
    window.emitter_combo.setEnabled(False)
    window.save_state()
