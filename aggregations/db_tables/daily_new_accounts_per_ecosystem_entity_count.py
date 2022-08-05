import datetime

from . import DAY_LEN_SECONDS, daily_start_of_range, time_range_json
from ..periodic_aggregations import PeriodicAggregations


# This metric is computed based on `daily_accounts_added_per_ecosystem_entity` table in Analytics DB
class DailyNewAccountsPerEcosystemEntityCount(PeriodicAggregations):
    DEPENDENCIES = ["daily_accounts_added_per_ecosystem_entity"]

    @property
    def sql_create_table(self):
        return """
            CREATE TABLE IF NOT EXISTS daily_new_accounts_per_ecosystem_entity_count
            (
                collected_for_day          DATE NOT NULL,
                entity_id                  TEXT NOT NULL,
                new_accounts_count         BIGINT  NOT NULL,
                CONSTRAINT daily_new_accounts_per_ecosystem_entity_count_pk PRIMARY KEY (collected_for_day, entity_id)
            )
        """

    @property
    def sql_drop_table(self):
        return """
            DROP TABLE IF EXISTS daily_new_accounts_per_ecosystem_entity_count
        """

    @property
    def sql_select(self):
        raise NotImplementedError(
            "no reason to request from Indexer DB for daily_new_accounts_per_ecosystem_entity_count"
        )

    @property
    def sql_insert(self):
        return """
            INSERT INTO daily_new_accounts_per_ecosystem_entity_count VALUES %s
            ON CONFLICT DO NOTHING
        """

    def collect(self, requested_timestamp: int) -> list:
        new_entity_users_select = """
            SELECT
              entity_id,
              COUNT(*) as new_accounts_count
            FROM daily_accounts_added_per_ecosystem_entity
            WHERE
              added_at_block_timestamp >= %(from_timestamp)s
              AND added_at_block_timestamp < %(to_timestamp)s
            GROUP BY entity_id
        """

        from_timestamp = self.start_of_range(requested_timestamp)
        with self.analytics_connection.cursor() as analytics_cursor:
            analytics_cursor.execute(
                new_entity_users_select,
                time_range_json(from_timestamp, self.duration_seconds),
            )
            result = analytics_cursor.fetchall()
            return self.prepare_data(result, start_of_range=from_timestamp)

    @property
    def duration_seconds(self):
        return DAY_LEN_SECONDS

    def start_of_range(self, timestamp: int) -> int:
        return daily_start_of_range(timestamp)

    @staticmethod
    def prepare_data(parameters: list, **kwargs) -> list:
        computed_for = datetime.datetime.utcfromtimestamp(
            kwargs["start_of_range"]
        ).strftime("%Y-%m-%d")
        return [(computed_for, entity_id, count) for (entity_id, count) in parameters]
