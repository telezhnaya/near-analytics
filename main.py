import argparse
import dotenv
import os
import psycopg2
import time

from aggregations import DailyActiveAccountsCount, DailyActiveContractsCount, DailyDeletedAccountsCount, \
    DailyDepositAmount, DailyGasUsed, DailyNewAccountsCount, DailyNewContractsCount, DailyNewUniqueContractsCount, \
    DailyReceiptsPerContractCount, DailyTransactionsCount, DailyTransactionsPerAccountCount, DeployedContracts, \
    WeeklyActiveAccountsCount
from aggregations.db_tables import GENESIS_SECONDS, DAY_LEN_SECONDS

from datetime import datetime

# TODO think how to get rid of these lists. It would be better if we don't need to populate them
STATS = {
    'daily_active_accounts_count': DailyActiveAccountsCount,
    'daily_active_contracts_count': DailyActiveContractsCount,
    'daily_deleted_accounts_count': DailyDeletedAccountsCount,
    'daily_deposit_amount': DailyDepositAmount,
    'daily_gas_used': DailyGasUsed,
    'daily_new_accounts_count': DailyNewAccountsCount,
    'daily_new_contracts_count': DailyNewContractsCount,
    'daily_new_unique_contracts_count': DailyNewUniqueContractsCount,
    'daily_receipts_per_contract_count': DailyReceiptsPerContractCount,
    'daily_transactions_count': DailyTransactionsCount,
    'daily_transactions_per_account_count': DailyTransactionsPerAccountCount,
    'deployed_contracts': DeployedContracts,
    'weekly_active_accounts_count': WeeklyActiveAccountsCount,
}

# For these aggregations, it's unable to compute all historical data by one SELECT query.
# We have to compute these aggregations by pieces
UNABLE_TO_COMPUTE_ALL_IN_ONCE = (
    'daily_new_unique_contracts_count',
    'daily_receipts_per_contract_count',
    'daily_transactions_per_account_count',
)


def compute(analytics_connection, indexer_connection, statistics_type, statistics, timestamp, collect_all):
    try:
        timestamp = None if collect_all else (timestamp or int(time.time() - DAY_LEN_SECONDS))
        printable_period = 'all period' if collect_all else f'{datetime.utcfromtimestamp(timestamp).date()}'
        print(f'Started computing {statistics_type} for {printable_period}')
        start_time = time.time()

        if collect_all:
            statistics.drop_table()
        statistics.create_table()

        result = statistics.collect(timestamp)
        statistics.store(result)

        print(f'Finished computing {statistics_type} in {round(time.time() - start_time, 1)} seconds')
    except Exception as e:
        analytics_connection.rollback()
        indexer_connection.rollback()
        raise e


def compute_statistics(analytics_connection, indexer_connection, statistics_type, timestamp, collect_all):
    statistics_cls = STATS[statistics_type]
    statistics = statistics_cls(analytics_connection, indexer_connection)

    for cls in statistics.dependencies():
        compute_statistics(analytics_connection, indexer_connection, cls, timestamp, collect_all)

    if collect_all and statistics_type in UNABLE_TO_COMPUTE_ALL_IN_ONCE:
        # It's not possible to hide this logic inside `collect_statistics` because the resulting list
        # will be potentially too large to store it in RAM
        # For example, we have > 1.6M lines for daily_transactions_per_account_count
        statistics.drop_table()
        current_day = GENESIS_SECONDS
        while current_day < int(time.time()):
            compute(analytics_connection, indexer_connection, statistics_type, statistics, current_day, False)
            current_day += DAY_LEN_SECONDS
    else:
        compute(analytics_connection, indexer_connection, statistics_type, statistics, timestamp, collect_all)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Compute aggregations for given Indexer DB')
    parser.add_argument('-t', '--timestamp', type=int,
                        help='The timestamp in seconds precision, indicates the period for computing the aggregations. '
                             'The rounding will be performed. By default, takes yesterday. '
                             'If it\'s not possible to compute the aggregations for given period, '
                             'nothing will be added to the DB.')
    parser.add_argument('-s', '--stats-types', nargs='+', choices=STATS, default=[],
                        help='The type of aggregations to compute. By default, everything will be computed.')
    parser.add_argument('-a', '--all', action='store_true',
                        help='Drop all previous data for given `stats-types` and fulfill the DB '
                             'with all values till now. Can\'t be used with `--timestamp`')
    args = parser.parse_args()
    if args.all and args.timestamp:
        raise ValueError('`timestamp` parameter can\'t be combined with `all` option')

    dotenv.load_dotenv()
    ANALYTICS_DATABASE_URL = os.getenv('ANALYTICS_DATABASE_URL')
    INDEXER_DATABASE_URL = os.getenv('INDEXER_DATABASE_URL')

    first_found_error = None
    with psycopg2.connect(ANALYTICS_DATABASE_URL) as analytics_connection, \
            psycopg2.connect(INDEXER_DATABASE_URL) as indexer_connection:
        for stats_type in args.stats_types or STATS:
            try:
                compute_statistics(analytics_connection, indexer_connection, stats_type, args.timestamp, args.all)
            except Exception as e:
                if not first_found_error:
                    first_found_error = e
                print(e)

    # It's important to have non-zero exit code in case of any errors,
    # It helps AWX to identify and report the problem
    if first_found_error:
        raise first_found_error
