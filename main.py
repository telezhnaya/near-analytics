import argparse
import dotenv
import os
import psycopg2
import time
import traceback
import typing

from aggregations import (
    DailyAccountsAddedPerEcosystemEntity,
    DailyActiveAccountsCount,
    DailyActiveContractsCount,
    DailyDeletedAccountsCount,
    DailyDepositAmount,
    DailyGasUsed,
    DailyIngoingTransactionsPerAccountCount,
    DailyNewAccountsCount,
    DailyNewAccountsPerEcosystemEntityCount,
    DailyNewContractsCount,
    DailyNewUniqueContractsCount,
    DailyOutgoingTransactionsPerAccountCount,
    DailyReceiptsPerContractCount,
    DailyTokensSpentOnFees,
    DailyTransactionCountByGasBurntRanges,
    DailyTransactionsCount,
    DeployedContracts,
    UniqueContracts,
    WeeklyActiveAccountsCount,
    NearEcosystemEntities,
)
from aggregations.db_tables import DAY_LEN_SECONDS, query_genesis_timestamp

from datetime import datetime

# TODO maybe we want to get rid of this list somehow
STATS = {
    "daily_accounts_added_per_ecosystem_entity": DailyAccountsAddedPerEcosystemEntity,
    "daily_active_accounts_count": DailyActiveAccountsCount,
    "daily_active_contracts_count": DailyActiveContractsCount,
    "daily_deleted_accounts_count": DailyDeletedAccountsCount,
    "daily_deposit_amount": DailyDepositAmount,
    "daily_gas_used": DailyGasUsed,
    "daily_ingoing_transactions_per_account_count": DailyIngoingTransactionsPerAccountCount,
    "daily_new_accounts_count": DailyNewAccountsCount,
    "daily_new_accounts_per_ecosystem_entity_count": DailyNewAccountsPerEcosystemEntityCount,
    "daily_new_contracts_count": DailyNewContractsCount,
    "daily_new_unique_contracts_count": DailyNewUniqueContractsCount,
    "daily_outgoing_transactions_per_account_count": DailyOutgoingTransactionsPerAccountCount,
    "daily_receipts_per_contract_count": DailyReceiptsPerContractCount,
    "daily_tokens_spent_on_fees": DailyTokensSpentOnFees,
    "daily_transaction_count_by_gas_burnt_ranges": DailyTransactionCountByGasBurntRanges,
    "daily_transactions_count": DailyTransactionsCount,
    "deployed_contracts": DeployedContracts,
    "unique_contracts": UniqueContracts,
    "weekly_active_accounts_count": WeeklyActiveAccountsCount,
    "near_ecosystem_entities": NearEcosystemEntities,
}


def compute(
    analytics_connection,
    indexer_connection,
    statistics_type: str,
    statistics,
    timestamp: int,
):
    start_time = time.time()
    try:
        print(
            f"Started computing {statistics_type} for {datetime.utcfromtimestamp(timestamp).date()}"
        )

        statistics.create_table()
        result = statistics.collect(timestamp)
        statistics.store(result)

        print(
            f"Finished computing {statistics_type} in {round(time.time() - start_time, 1)} seconds"
        )
    except Exception as e:
        print(
            f"Failed to compute {statistics_type} (spent {round(time.time() - start_time, 1)} seconds)"
        )
        # psycopg2 does not provide proper exception if the connection is closed.
        # The given exception is too broad, and sometimes psycopg2 gives different error types on a same reason.
        # As a result, we can fail here if we try to rollback the transaction on the closed connection.
        # We anyway handle the exception further, so I decided to ignore this issue here
        analytics_connection.rollback()
        indexer_connection.rollback()
        raise e


def compute_statistics(
    analytics_database_url,
    indexer_database_url,
    statistics_type: str,
    timestamp: typing.Optional[int],
    collect_all,
):
    statistics_cls = STATS[statistics_type]

    for cls in statistics_cls.DEPENDENCIES:
        compute_statistics(
            analytics_database_url, indexer_database_url, cls, timestamp, collect_all
        )

    analytics_connection = psycopg2.connect(analytics_database_url)
    indexer_connection = psycopg2.connect(indexer_database_url)
    if collect_all:
        statistics = statistics_cls(analytics_connection, indexer_connection)
        statistics.drop_table()
        current_day = query_genesis_timestamp(indexer_connection)
        while current_day < int(time.time()):
            for attempt in range(10, 0, -1):
                try:
                    compute(
                        analytics_connection,
                        indexer_connection,
                        statistics_type,
                        statistics_cls(analytics_connection, indexer_connection),
                        current_day,
                    )
                except Exception:
                    print(f"Compute for {current_day} failed. See details below.")
                    traceback.print_exc()
                    if attempt == 0:
                        raise
                    print(f"Retrying...")
                    analytics_connection = psycopg2.connect(analytics_database_url)
                    indexer_connection = psycopg2.connect(indexer_database_url)
                else:
                    break
            current_day += DAY_LEN_SECONDS
    else:
        # Computing for yesterday by default
        timestamp = timestamp or int(time.time() - DAY_LEN_SECONDS)
        compute(
            analytics_connection,
            indexer_connection,
            statistics_type,
            statistics_cls(analytics_connection, indexer_connection),
            timestamp,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Compute aggregations for given Indexer DB"
    )
    parser.add_argument(
        "-t",
        "--timestamp",
        type=int,
        help="The timestamp in seconds precision, indicates the period for computing the aggregations. "
        "The rounding will be performed. By default, takes yesterday. "
        "If it's not possible to compute the aggregations for given period, "
        "nothing will be added to the DB.",
    )
    parser.add_argument(
        "-s",
        "--stats-types",
        nargs="+",
        choices=STATS,
        default=[],
        help="The type of aggregations to compute. By default, everything will be computed.",
    )
    parser.add_argument(
        "-a",
        "--all",
        action="store_true",
        help="Drop all previous data for given `stats-types` and fulfill the DB "
        "with all values till now. Can't be used with `--timestamp`",
    )
    args = parser.parse_args()
    if args.all and args.timestamp:
        raise ValueError("`timestamp` parameter can't be combined with `all` option")

    dotenv.load_dotenv()
    ANALYTICS_DATABASE_URL = os.getenv("ANALYTICS_DATABASE_URL")
    INDEXER_DATABASE_URL = os.getenv("INDEXER_DATABASE_URL")

    stats_need_to_compute = set(args.stats_types or STATS.keys())
    for i in range(1, 6):
        print(f"Attempt {i}...")
        stats_computed = set()
        try:
            for stats_type in stats_need_to_compute:
                try:
                    compute_statistics(
                        ANALYTICS_DATABASE_URL,
                        INDEXER_DATABASE_URL,
                        stats_type,
                        args.timestamp,
                        args.all,
                    )
                    stats_computed.add(stats_type)
                except Exception:
                    print(f"Failed to compute the value for {stats_type}")
                    traceback.print_exc()

        except Exception as e:
            # If we lost connection and try to catch related DB exception here,
            # it raises a new one in a process of handling the initial one,
            # so we have to catch the general Exception here
            print("The connection is probably lost")
            print(e)
            time.sleep(10)

        stats_need_to_compute -= stats_computed
        if not stats_need_to_compute:
            break

    # It's important to have non-zero exit code in case of any errors,
    # It helps AWX to identify and report the problem
    if stats_need_to_compute:
        raise TimeoutError(
            f"Some aggregations could not be calculated: [{' '.join(stats_need_to_compute)}]"
        )
