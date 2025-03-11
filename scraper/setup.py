from setuptools import setup, find_packages

setup(
    name="finance_data_scraper",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "selenium",
        "webdriver-manager",
    ],
    entry_points={
        "console_scripts": [
            "data_coverage_scraper=src.fetchers.tradingview.data_coverage_scraper:countries_scraper",
        ],
    },
)