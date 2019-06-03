#!/usr/bin/env python

from setuptools import setup
from setuptools import find_packages

setup(
    name='tap-shiphero',
    version='1.1.4',
    description="Singer.io tap for extracting Shiphero data",
    author="Stitch",
    url="http://singer.io",
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    py_modules=['tap_shiphero'],
    install_requires=[
        'backoff==1.3.2',
        'ratelimit==2.2.0',
        'requests==2.20.1',
        'singer-python==5.5.0'
    ],
    entry_points='''
      [console_scripts]
      tap-shiphero=tap_shiphero:main
    ''',
    packages=find_packages(),
    include_package_data=True,
    package_data = {
        "schemas": ["tap_shiphero/schemas/*.json"]
    }
)
