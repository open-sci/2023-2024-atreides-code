from setuptools import setup, find_packages
import sys


setup(
    name='AtreidesOS',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'dask[dataframe]==2024.5.0',
        'polars==1.6.0',
        'python-dotenv==1.0.1',
        'requests==2.32.2',
        'rapidfuzz==3.2.0',
        'SPARQLWrapper==2.0.0',
        'tqdm==4.66.4',
    ],
    python_requires='<=3.11.5',
)
