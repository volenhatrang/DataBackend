from setuptools import setup

setup(
    name='finance_data_scraper',
    version='0.1.0',
    packages=find_packages(where='scraper'),
    package_dir={'': 'src'},
    install_requires=[
        'beautifulsoup4==4.9.3',
        'selenium',
        'webdriver-manager',
        'cassandra-driver',
        'requests',
        'pandas',
        'numpy',
        'scipy',
        'matplotlib',
        'scikit-learn',
        'tensorflow',
        'keras',
        'pytorch',
        'torchvision',
        'fastai'])