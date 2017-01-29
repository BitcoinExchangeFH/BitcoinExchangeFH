<p align="center">
  <img src="doc/icon.jpg">
</p>

# BitcoinExchangeFH - Bitcoin exchange market data feed handler (Bitstamp, BTCC, Bitfinex, BitMEX, Gatecoin, GDAX, Huobi, Kraken, OkCoin, Quoine)

BitcoinExchangeFH is a slim application to record the price depth and trades in various exchanges. You can set it up quickly and record the all the exchange data in a few minutes!

Users can

1. Recording market data for backtesting and analysis.
2. Recording market data to a in-memory database and other applications can quickly access to it.
3. Customize the project for trading use.

<p align="center">
  <img src="doc/sample.jpg">
</p>

## Supported exchanges

- Bitfinex (Websocket)
- BitMEX (Websocket)
- Bitstamp (Websocket)
- BTCC (RESTful)
- Gatecoin (RESTful)
- GDAX (Websocket)
- Huobi (SocketIO)
- Kraken (RESTful)
- OkCoin (Websocket)
- Quoine (RESTful)

Currently the support of other exchanges is still under development.

Scheduled exchange supported soon:
- xBTCe
- Poloniex
- DABTC
- FX rate (USDCNY, EURUSD)

## Supported database

- Kdb+
- MySQL
- Sqlite
- CSV

## Getting started

It is highly recommended to use pip for installing python dependence. 

```
pip install -r python/requirement.txt
```

### Database

#### Sqlite

No further setup is required.

#### Kdb+

First, start your Kdb+ database. You can either choose your own binary or the binary in the [third-party](https://github.com/gavincyi/BitcoinExchangeFH/tree/master/third-party) folder.

```
q -p 5000
```

Then connect to the database with dedicated port (for example 5000 in the example).

#### MySQL

To store the market data to MySQL database, please install [mysql-server](https://dev.mysql.com/downloads/mysql/) first. Then enable the following user privileges on your target schema

```
CREATE
UPDATE
INSERT
SELECT
```

### Process startup

For testing, you can quick start with Sqlite as follows. It uses the default subscription list and records the data to default sqlite file "bitcoinexchange.raw"

```
python python/bitcoinexchangefh.py -sqlite
```

To record the data to Kdb+ database, for example connecting to localhost at port 5000, you can run the following command

```
python python/bitcoinexchangefh.py -kdb -dbaddr localhost -dbport 5000
```

To record the data to MySQL database, for example connecting to localhost with user "bitcoin" and schema "bcex", you can run the following command.

```
python python/bitcoinexchangefh.py -mysql -dbaddr localhost -dbport 3306 -dbuser bitcoin -dbpwd bitcoin -dbschema bcex
```

To record the data to csv files, for example to a folder named "data", you can run the following command.

```
python python/bitcoinexchangefh.py -csv -dbdir data/
```

### Arguments

|Argument|Description|
|---|---|
|mode|Please refer to [Mode](#mode)|
|instmts|Instrument subscription file.|
|exchtime|Use exchange timestamp if possible.|
|kdb|Use Kdb+ database.|
|sqlite|Use SQLite database.|
|mysql|Use MySQL.|
|csv|Use CSV file as database.|
|dbpath|Database file path. Supported for SQLite only.|
|dbaddr|Database address. Defaulted as localhost. Supported for database with connection.|
|dbport|Database port, Defaulted as 3306. Supported for database with connection.|
|dbuser|Database user. Supported for database with connection.|
|dbpwd|Database password. Supported for database with connection.|
|dbschema|Database schema. Supported for database with connection.|
|output|Verbose output file path.|

### Mode

Currently it supports five modes to distribute market data.

|Mode|Description|
|---|---|
|SNAPSHOT_ONLY|Market snapshot only. Shown in table exchange_snapshot|
|ORDER_BOOK_ONLY|Order book only. Shown in tables exch_xxxx_yyyy_order_book, where xxxxx and yyyyy are the exchange and instrument name respectively|
|TRADES_ONLY|Trades only. Shown in tables exch_xxxx_yyyy_trades, where xxxxx and yyyyy are the exchange and instrument name respectively|
|ORDER_BOOK_AND_TRADES_ONLY|Order book and trades only|
|ALL|Supports snapshot, order book and trades|

### Subscription
All the instrument subscription are mentioned in the configuration file [subscriptions.ini](subscriptions.ini). For supported exchanges, you can include its instruments as a block of subscription.

|Argument|Description|
|---|---|
|(block name)|Unique subscription ID|
|exchange|Exchange name.|
|instmt_name|Instrument name. Used in application, e.g. database table name|
|instmt_code|Exchange instrument code. Used in exchange API|
|enabled|Indicate whether to subscribe it|

### Market Data

All market data are stored in the dedicated database. For each instrument, there are two tables, order book and trades. The order book is the price depth at top five levels. They are recorded under the table names of

```
exch_<exchange name>_<instrument name>_book
exch_<exchange name>_<instrument name>_trades
```

## Inquiries

You can first look up to the page [FAQ](https://github.com/gavincyi/BitcoinExchangeFH/wiki/FAQ). For more inquiries, you can either leave it in issues or drop me an email. I will get you back as soon as possible.

## Compatibility
The application is compatible with version higher or equal to python 3.0.

## Contributions
Always welcome for any contribution. Please fork the project, make the changes, and submit the merge request. :)

For any questions and comment, please feel free to contact me through email (gavincyi at gmail)

Your comment will be a huge contribution to the project!

## Continuity
If you are not satisified with python performance, you can contact me to discuss migrating the project into other languages, e.g. C++.
