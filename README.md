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

All exchanges supported by [ccxt](https://github.com/ccxt/ccxt).


## Supported database/channel

- RDMBS (e.g. sqlite, MySQL, PostgreSQL)

## Getting started

```
pip install bitcoinexchangefh
bitcoinexchangefh --configuration example/configuration.yaml
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
