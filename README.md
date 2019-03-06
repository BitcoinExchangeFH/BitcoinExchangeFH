<p align="center">
  <img src="docs/images/icon.jpg">
</p>

# BitcoinExchangeFH - Cryptocurrency exchange market data feed handler

BitcoinExchangeFH is a slim application to record the price depth and trades in various exchanges. You can set it up quickly and record the all the exchange data in a few minutes!

Users can

1. Streaming market data to a target application (via ZeroMQ)
2. Recording market data for backtesting and analysis.
3. Recording market data to a in-memory database and other applications can quickly access to it.
4. Customize the project for trading use.

### MySQL

<p align="center">
  <img src="docs/images/sample.jpg">
</p>

### Kdb+

<p align="center">
  <img src="docs/images/sample2.jpg">
</p>

## Supported exchanges

All exchanges supported by [ccxt](https://github.com/ccxt/ccxt). Currently more than 130 exchanges are supported.

Websocket feeds of the following exchanges are supported by [cryptofeed](https://github.com/bmoscon/cryptofeed)

- Bitfinex

- Coinbase

- Poloniex

- Gemini

- HitBTC

- Bitstamp

- BitMEX

- Kraken

- Binance

- EXX

- Huobi


If the exchange is not supported with websocket API feed, it will automatically fall into using its REST API feed.


## Supported database/channel

- RDMBS (e.g. sqlite, MySQL, PostgreSQL)

- ZeroMQ

- Kdb+ (Coming soon)

## Getting started

```
pip install bitcoinexchangefh
bitcoinexchangefh --configuration example/configuration.yaml
```

## Configuration

The configuration follows [YAML](https://pyyaml.org/wiki/PyYAMLDocumentation) syntax and contains two sections

- subscriptions

- handlers


### Subscriptions

Subscription section specifies the exchange and instruments to subscribe. 

The first key is the exchange name and then follows the exchange details, 
    
- instruments 

- number of depth (default is 5 if not specified)


For example, 

```
subscription:
    Binance:
        instruments:
            - XRP/BTC
            - BCH/BTC
        depth
    Poloniex:
        instruments:
            - ETH/BTC
        depth: 10
```

### Handlers

After receiving the order book or trade update, each handler is updated. For example, for SQL database handler, it is updated with the corresponding SQl statements.

For example,

```
handlers:
    sql: 
        connection: "sqlite://"
    
```

#### SQL handler

The following settings can be customized

|Parameter|Description|
|---|---|
|connection|Database connection string required by [SQLAlchemy](https://docs.sqlalchemy.org/en/latest/core/engines.html)|
|is_rotate|Boolean indicating whether to rotate to record the table.|
|rotate_frequency|String in [format](https://docs.python.org/2/library/datetime.html#strftime-strptime-behavior) same as `strftime` and `strptime`|

#### ZeroMQ handler

The feed handler acts as a [publisher](https://learning-0mq-with-pyzmq.readthedocs.io/en/latest/pyzmq/patterns/pubsub.html) in ZeroMQ. To receive the feed, please follow ZeroMQ instructions to start a [subscriber](tests/zmq/zmq_subscriber.py).

The following settings can be customized

|Parameter|Description|
|---|---|
|connection|Connection [format](http://api.zeromq.org/3-2:zmq-connect) in ZeroMQ. For example, "tcp://127.0.0.1:3456"|


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
