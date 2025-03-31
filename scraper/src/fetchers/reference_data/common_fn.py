import requests
import yfinance as yf
import pandas as pd
import re
import time
import pytz
import schedule
import numpy as np
import os
import warnings
import logging

# import datetime
import io

from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, date
from pathlib import Path
from functools import reduce
from yfinance.exceptions import YFRateLimitError
from tabulate import tabulate
from lxml import html
from io import StringIO

# from .base import DataSource

warnings.filterwarnings("ignore", category=DeprecationWarning)


def sys_time():
    """Return the current system time as a string."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def update_updated(df):
    """Update the 'updated' column to the current time."""
    df = df.copy()
    df["updated"] = sys_time()
    return df


def format_number(x, digits=2):
    """Format a number with the given number of decimal places."""
    return f"{x:.{digits}f}" if pd.notnull(x) else x


def view_data(df):
    if len(df) > 10:
        df_display = df.head(5)._append(df.tail(5))
    else:
        df_display = df
    print()
    print(tabulate(df_display, headers="keys", tablefmt="pretty", showindex=False))


def clean_colnames(df):
    """
    Cleans column names of a DataFrame by:
    - Converting to lowercase
    - Replacing spaces and hyphens with underscores
    - Replacing '%' with 'percent_'
    - Removing '*' characters
    - Stripping any leading/trailing whitespace

    Parameters:
    df (pd.DataFrame): The DataFrame with columns to clean.

    Returns:
    pd.DataFrame: A new DataFrame with cleaned column names.
    """
    df = df.copy()
    df.columns = [
        re.sub(
            r"\s+",
            "",
            re.sub(
                r"\-",
                "_",
                re.sub(
                    r"\%",
                    "percent_",
                    re.sub(r"\*", "", col.lower()),
                ),
            ),
        )  # Remove '*' and make lowercase
        for col in df.columns
    ]
    return df


def clean_number(s):
    """
    Loại bỏ dấu phẩy và chuyển đổi sang kiểu số float.
    Nếu không chuyển được, trả về None.
    """
    if pd.isna(s):
        return None
    if isinstance(s, str):
        s = s.replace(",", "")
    try:
        return float(s)
    except Exception:
        return None


def extract_word(s, index, delimiter=""):
    """
    Tách chuỗi s thành các từ dựa theo khoảng trắng và trả về từ thứ index.
    index bắt đầu từ 1.
    """
    if pd.isna(s):
        return None
    if len(delimiter) > 0:
        parts = str(s).split(delimiter)
    else:
        parts = str(s).split()
    if len(parts) >= index:
        return parts[index - 1]
    return None


def clean_varpc(s):
    """
    Xử lý chuỗi chứa phần trăm: loại bỏ các ký tự không phải số.
    """
    if pd.isna(s):
        return None
    s_clean = re.sub(r"[\(\)%]", "", s)
    return clean_number(s_clean)


def convert_number(value):
    """Helper function to convert strings like '1.2B' into a numeric value."""
    try:
        if "B" in value:
            return float(value.replace("B", "").replace(",", "")) * 1e9
        elif "M" in value:
            return float(value.replace("M", "").replace(",", "")) * 1e6
        elif "K" in value:
            return float(value.replace("K", "").replace(",", "")) * 1e3
        elif "T" in value:
            return float(value.replace("T", "").replace(",", "")) * 1e12
        else:
            return float(value.replace(",", ""))
    except ValueError:
        return None


def get_last_trading_day(target_hour, to_prompt=False):
    now = datetime.now()

    if target_hour < now.hour:
        candidate = now.date() - timedelta(days=1)

        if candidate.weekday() == 5:
            candidate -= timedelta(days=1)
        elif candidate.weekday() == 6:
            candidate -= timedelta(days=2)
    else:
        candidate = now.date()

        if candidate.weekday() == 5:
            candidate += timedelta(days=2)
        elif candidate.weekday() == 6:
            candidate += timedelta(days=1)

    if to_prompt:
        print("Last Trading Day:", candidate)
    return candidate


def clean_numeric(x, factor):
    """Remove commas, convert to float, then multiply by factor. Returns None on failure."""
    if pd.isna(x):
        return None
    try:
        return factor * float(str(x).replace(",", ""))
    except Exception:
        return None


def get_html_selenium(link):

    options = webdriver.ChromeOptions()
    s = Service()
    options.add_argument("--headless")
    options.add_argument("--window-position=-2400,-2400")
    driver = webdriver.Chrome(service=s, options=options)
    html_text = driver.get(link)
    driver.quit()

    return html_text
