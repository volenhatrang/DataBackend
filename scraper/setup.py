from setuptools import setup, find_packages

setup(
    name='finance_data_scraper',
    version='0.1.0',
    packages=find_packages(where='src'), 
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
        'torch',
        'torchvision',
        'fastai'
    ]
)
