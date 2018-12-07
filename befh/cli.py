# -*- coding: utf-8 -*-

"""Console script for BitcoinExchangeFH."""

import logging

import click
import yaml

from befh import Configuration, Runner

LOGGER = logging.getLogger(__name__)


@click.command()
@click.option(
    '--configuration',
    help='Configuration file.',
    required=True)
@click.option(
    '--debug',
    default=False,
    is_flag=True,
    help='Debug mode.')
def main(configuration, debug):
    """Console script for BitcoinExchangeFH."""
    if debug:
        level = logging.DEBUG
    else:
        level = logging.INFO

    logging.basicConfig(
        level=level,
        format='%(asctime)s %(levelname)s %(message)s')

    configuration = open(configuration, 'r')
    configuration = yaml.load(configuration)
    LOGGER.debug('Configuration:\n%s', configuration)
    configuration = Configuration(configuration)

    runner = Runner(config=configuration)
    runner.load()
    runner.run()


if __name__ == "__main__":
    main()