import pandas as pd


def format_number(value, scale=1, suffix=''):
    if pd.isna(value):
        return "0" if suffix == '%' else "N/A"
    if value == 0:
        return f"0{suffix}"
    sign = '-' if value < 0 else ''
    abs_value = abs(value * scale)
    if abs_value < 1e3:
        formatted = f"{abs_value:,.2f}"
    elif abs_value < 1e6:
        formatted = f"{abs_value / 1e3:,.2f} тыс."
    elif abs_value < 1e9:
        formatted = f"{abs_value / 1e6:,.2f} млн"
    elif abs_value < 1e12:
        formatted = f"{abs_value / 1e9:,.2f} млрд"
    else:
        formatted = f"{abs_value / 1e12:,.2f} трлн"
    return f"{sign}{formatted}{suffix}"

def format_from_db(value, unit):
    if unit == "млрд руб":
        return value * 10**9