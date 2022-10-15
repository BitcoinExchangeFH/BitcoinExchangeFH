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
@click.option(
    '--cold',
    default=False,
    is_flag=True,
    help='Cold start mode.')
@click.option(
    '--archive',
    default=None,
    help='Manually archive the tables.',
    required=False)
def main(configuration, debug, cold, archive):
    """Console script for BitcoinExchangeFH."""
    if debug:
        level = logging.DEBUG
    else:
        level = logging.INFO

    logging.basicConfig(
        level=level,
        format='%(asctime)s %(levelname)s %(message)s')

    configuration = open(configuration, 'r')
    configuration = yaml.safe_load(configuration, Loader=yaml.FullLoader)
    LOGGER.debug('Configuration:\n%s', configuration)
    configuration = Configuration(configuration)

    runner = Runner(
        config=configuration,
        is_debug=debug,
        is_cold=cold)
    runner.load()

    if archive is not None:
        runner.archive(date=archive)
    else:
        runner.run()


if __name__ == "__main__":
    main()
