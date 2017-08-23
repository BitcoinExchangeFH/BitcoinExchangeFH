from distutils.core import setup

setup(
    name='BitcoinExchangeFH',
    version='0.2.0',
    author='Gavin Chan',
    author_email='gavincyi@gmail.com',
    packages=['befh'],
    url='http://pypi.python.org/pypi/BitcoinExchangeFH/',
    license='LICENSE.txt',
    description='Cryptocurrency exchange market data feed handler.',
    entry_points={
            'console_scripts': ['bitcoinexchangefh=befh.bitcoinexchangefh:main']
        },
    install_requires=[
            'pymysql',
            'psycopg2',
            'websocket-client',
            'numpy',
            'qpython',
            'pyzmq'
        ]
    )