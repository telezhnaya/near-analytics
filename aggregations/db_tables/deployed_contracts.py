from . import DAY_LEN_SECONDS, daily_start_of_range
from ..periodic_aggregations import PeriodicAggregations


class DeployedContracts(PeriodicAggregations):
    @property
    def sql_create_table(self):
        return """
            CREATE TABLE IF NOT EXISTS deployed_contracts
            (
                contract_code_sha256        text           NOT NULL,
                deployed_to_account_id      text           NOT NULL,
                deployed_by_receipt_id      text           PRIMARY KEY,
                -- It is important to store here not only day, but the exact timestamp
                -- Because there could be several deployments at the same day
                deployed_at_block_timestamp numeric(20, 0) NOT NULL
            );
            CREATE INDEX IF NOT EXISTS deployed_contracts_timestamp_idx
                ON deployed_contracts (deployed_at_block_timestamp);
            CREATE INDEX IF NOT EXISTS deployed_contracts_sha256_idx
                ON deployed_contracts (contract_code_sha256);
            CREATE INDEX IF NOT EXISTS deployed_contracts_deployed_to_account_id_idx
                ON deployed_contracts (deployed_to_account_id);
            ALTER TABLE deployed_contracts
                ADD COLUMN IF NOT EXISTS deployed_at_block_hash text NOT NULL DEFAULT '';
        """

    @property
    def sql_drop_table(self):
        return """
            DROP TABLE IF EXISTS deployed_contracts
        """

    @property
    def sql_select(self):
        return """
            SELECT
                action_receipt_actions.args->>'code_sha256' as contract_code_sha256,
                action_receipt_actions.receipt_receiver_account_id as deployed_to_account_id,
                action_receipt_actions.receipt_id as deployed_by_receipt_id,
                execution_outcomes.executed_in_block_timestamp as deployed_at_block_timestamp,
                execution_outcomes.executed_in_block_hash as deployed_at_block_hash
            FROM action_receipt_actions
            JOIN execution_outcomes ON execution_outcomes.receipt_id = action_receipt_actions.receipt_id
            WHERE action_receipt_actions.action_kind = 'DEPLOY_CONTRACT'
                AND execution_outcomes.status = 'SUCCESS_VALUE'
                AND execution_outcomes.executed_in_block_timestamp >= %(from_timestamp)s
                AND execution_outcomes.executed_in_block_timestamp < %(to_timestamp)s
            ORDER BY execution_outcomes.executed_in_block_timestamp
        """

    @property
    def sql_insert(self):
        return """
            INSERT INTO deployed_contracts (
                contract_code_sha256,
                deployed_to_account_id,
                deployed_by_receipt_id,
                deployed_at_block_timestamp,
                deployed_at_block_hash
            ) VALUES %s
            ON CONFLICT DO NOTHING
        """

    @property
    def duration_seconds(self):
        return DAY_LEN_SECONDS

    def start_of_range(self, timestamp: int) -> int:
        return daily_start_of_range(timestamp)

    @staticmethod
    def prepare_data(parameters: list, *, start_of_range=None, **kwargs) -> list:
        return parameters
