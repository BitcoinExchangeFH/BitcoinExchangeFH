# BitcoinExchangeFH - Bitcoin exchange market data feed handler (Bitfinex, BitMEX and BTCC)

Feed handler is a slim application to record the price depth and trades in bitcoin exchanges. It targets on real time market data recording into a database. Users can

1. Recording market data for backtesting and analysis.
2. Recording market data to a in-memory database and other applications can quickly access to it.
3. Customize the project for trading use.

## Supported exchanges

- OkCoin (Websocket)
- Bitfinex (Websocket)
- BitMEX (Websocket)
- BTCC (RESTful)

Currently the support of other exchanges is still under development.

Scheduled exchange supported soon:
- Kraken
- xBTCe
- Poloniex
- DABTC
- FX rate (USDCNY, EURUSD)

## Supported database

- Sqlite
- MySQL

Currently the support of other databases is still under development.

## Getting started

It is highly recommended to use pip for installing python dependence. 

```
pip install -r python/requirement.txt
```

### Database

#### Sqlite

No further setup is required.

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

To record the data to MySQL database, for example connecting to localhost with user "bitcoin" and schema "bcex", you can run the following command.

```
python python/bitcoinexchangefh.py -mysql -dbaddr localhost -dbport 3306 -dbuser bitcoin -dbpwd bitcoin -dbschema bcex
```

### Arguments

|Argument|Description|
|---|---|
|instmts|Instrument subscription file.|
|sqlite|Use SQLite database.|
|mysql|Use MySQL.|
|dbpath|Database file path. Supported for SQLite only.|
|dbaddr|Database address. Defaulted as localhost. Supported for database with connection.|
|dbport|Database port, Defaulted as 3306. Supported for database with connection.|
|dbuser|Database user. Supported for database with connection.|
|dbpwd|Database password. Supported for database with connection.|
|dbschema|Database schema. Supported for database with connection.|


## Compatibility
The application is compatible with version higher or equal to python 3.0.

## Contributions
Always welcome for any contribution. Please fork the project, make the changes, and submit the merge request. :)

For any questions, please feel free to contact me through email (gavincyi at gmail)
