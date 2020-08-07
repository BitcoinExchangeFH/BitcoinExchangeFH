#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

requirements = [
    'Click>=6.0',
    'PyYAML>=4.2b1',
    'ccxt>=1.17.485',
    'sqlalchemy>=1.1.11',
    'alembic>=0.9.3',
    'pyzmq>=17.1.2',
]

setup_requirements = [
    'setuptools_scm',
    'pytest-runner',
]

test_requirements = [
    'pytest',
]

extra_requirements = {
    ":python_version>='3.5.3'": ["cryptofeed>=1.4.1"],
}


setup(
    name='BitcoinExchangeFH',
    use_scm_version=True,
    description="Cryptocurrency exchange market data feed handler",
    author="Gavin Chan",
    author_email='gavincyi@gmail.com',
    url='https://github.com/gavincyi/BitcoinExchangeFH',
    packages=find_packages(include=['befh']),
    entry_points={
        'console_scripts': [
            'bitcoinexchangefh=befh.cli:main'
        ]
    },
    include_package_data=True,
    install_requires=requirements,
    license="Apache Software License 2.0",
    zip_safe=False,
    keywords='BitcoinExchangeFH',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    test_suite='tests',
    tests_require=test_requirements,
    setup_requires=setup_requirements,
    extras_require=extra_requirements,
)
