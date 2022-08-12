from . import DAY_LEN_SECONDS, daily_start_of_range
from ..periodic_aggregations import PeriodicAggregations

"""
This code builds a table intended to assist with calculating statistics
relating accounts ("added_accounts") to ecosystem entities ("entities")
and is originally intended to be used for tracking new accounts added to each entity.
"""


class DailyAccountsAddedPerEcosystemEntity(PeriodicAggregations):
    DEPENDENCIES = ["near_ecosystem_entities"]

    @property
    def sql_create_table(self):
        return """
            CREATE TABLE IF NOT EXISTS daily_accounts_added_per_ecosystem_entity
            (
                entity_id                           TEXT NOT NULL,
                account_id                          TEXT NOT NULL,
                added_at_block_timestamp            numeric(20, 0) NOT NULL,
                CONSTRAINT daily_accounts_added_per_ecosystem_entity_pk PRIMARY KEY (entity_id, account_id)
            );
        """

    @property
    def sql_drop_table(self):
        return """
            DROP TABLE IF EXISTS daily_accounts_added_per_ecosystem_entity
            """

    @property
    def sql_select(self):
        # grab map of entity slug/contract-id pairs from near analytics db
        # and apply that mapping to receiver entity ID's from near explore indexer db
        # because each entity may be associated with more than one contract
        entity_contracts_sql = """
            SELECT
                REPLACE(TRIM(slug),$$'$$,'')          AS entity 
                , REPLACE(TRIM(contract_id),$$'$$,'') AS contract_id
            FROM public.near_ecosystem_entities e, unnest(string_to_array(e.contract, ', ')) s(contract_id)
            WHERE length(contract) > 0
            """

        with self.analytics_connection.cursor() as analytics_cursor:
            analytics_cursor.execute(entity_contracts_sql)
            entity_contracts = analytics_cursor.fetchall()

        indented_newline = "                    \n"
        cases_sql = indented_newline.join(
            [f"WHEN '{c}' THEN '{e}'" for e, c in entity_contracts]
        )

        return """
            WITH
            added_to_entity_events AS
            (
                SELECT
                    CASE (args -> 'access_key' -> 'permission' -> 'permission_details' ->> 'receiver_id')
                        {cases_sql}
                        END                         AS entity_id
                    , receipt_receiver_account_id AS account_id
                    , receipt_included_in_block_timestamp as added_at_timestamp
                FROM public.action_receipt_actions
                WHERE action_kind IN ('ADD_KEY')
                    AND args ->'access_key' -> 'permission' ->> 'permission_kind' = 'FUNCTION_CALL'
                    AND receipt_included_in_block_timestamp  >= %(from_timestamp)s
                    AND receipt_included_in_block_timestamp  < %(to_timestamp)s
                GROUP BY 1, 2, 3
            )
            SELECT entity_id,
            account_id,
            MIN(added_at_timestamp) as added_at_timestamp
            FROM added_to_entity_events
            WHERE entity_id NOT IN (account_id, 'near')
            GROUP BY 1, 2
            """.format(
            cases_sql=cases_sql
        )

    @property
    def sql_insert(self):
        return """
            INSERT INTO daily_accounts_added_per_ecosystem_entity VALUES %s 
            ON CONFLICT DO NOTHING
        """

    @property
    def duration_seconds(self):
        return DAY_LEN_SECONDS

    def start_of_range(self, timestamp: int) -> int:
        return daily_start_of_range(timestamp)

    @staticmethod
    def prepare_data(parameters: list, **kwargs) -> list:
        return parameters
