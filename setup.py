from distutils.core import setup

setup(
    name='BitcoinExchangeFH',
    version='0.1.2rc1',
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
            'websocket',
            'websocket-client',
            'socketIO_client==0.5.6',
            'numpy',
            'qpython',
            'pyzmq'
        ]
    )