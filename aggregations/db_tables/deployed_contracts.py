import base64
import near_api

from . import DAY_LEN_SECONDS, daily_start_of_range
from ..periodic_aggregations import PeriodicAggregations


class DeployedContracts(PeriodicAggregations):
    @property
    def sql_create_table(self):
        return """
            CREATE TABLE IF NOT EXISTS deployed_contracts__TEST
            (
                contract_code_sha256        text           NOT NULL,
                deployed_to_account_id      text           NOT NULL,
                deployed_by_receipt_id      text           PRIMARY KEY,
                -- It is important to store here not only day, but the exact timestamp
                -- Because there could be several deployments at the same day
                deployed_at_block_timestamp numeric(20, 0) NOT NULL
            );
            CREATE INDEX IF NOT EXISTS deployed_contracts_timestamp_idx__TEST
                ON deployed_contracts__TEST (deployed_at_block_timestamp);
            CREATE INDEX IF NOT EXISTS deployed_contracts_sha256_idx__TEST
                ON deployed_contracts__TEST (contract_code_sha256);
            CREATE INDEX IF NOT EXISTS deployed_contracts_deployed_to_account_id_idx__TEST
                ON deployed_contracts__TEST (deployed_to_account_id);
            ALTER TABLE deployed_contracts__TEST
                ADD COLUMN IF NOT EXISTS deployed_at_block_hash text NOT NULL DEFAULT '',
                ADD COLUMN IF NOT EXISTS contract_sdk_type text NOT NULL DEFAULT '';
            CREATE INDEX IF NOT EXISTS deployed_contracts_contract_sdk_type_idx__TEST
                ON deployed_contracts__TEST (contract_sdk_type);
        """

    @property
    def sql_drop_table(self):
        return """
            DROP TABLE IF EXISTS deployed_contracts__TEST
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
            INSERT INTO deployed_contracts__TEST (
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

    def store(self, parameters: list) -> list:
        super().store(parameters)

        near_rpc_url = os.getenv('NEAR_RPC_URL')
        if not near_rpc_url:
            return
        near_rpc = near_api.providers.JsonProvider(near_rpc_url)

        def view_code(account_id, block_id):
            for _ in range(1000):
                try:
                    response = near_rpc.json_rpc('query', {"request_type": "view_code", "account_id": account_id, "block_id": block_id})
                except near_api.providers.JsonProviderError as e:
                    if e.args[0].get('cause', {}).get('name') == 'UNKNOWN_ACCOUNT':
                        return b''
                    print("Retrying fetching contract code...")
                    import traceback
                    traceback.print_exc()
                except Exception:
                    print("Retrying fetching contract code...")
                    import traceback
                    traceback.print_exc()
                else:
                    break
            return base64.b64decode(response["code_base64"])

        sql_missing_sdk_type = """
            SELECT contract_code_sha256, deployed_to_account_id, deployed_at_block_hash
            FROM deployed_contracts__TEST
            WHERE contract_sdk_type = '' AND deployed_at_block_hash != ''
            LIMIT 10
        """

        with self.analytics_connection.cursor() as analytics_cursor:
            while True:
                analytics_cursor.execute(sql_missing_sdk_type)
                unknown_contracts = analytics_cursor.fetchall()
                if not unknown_contracts:
                    break
                print(unknown_contracts)

                contract_sdk_types = []
                for contract_code_sha256, contract_account_id, deployed_at_block_hash in unknown_contracts:
                    print('fetching ', contract_account_id)
                    contract_code = view_code(contract_account_id, deployed_at_block_hash)

                    if contract_code == b'':
                        contract_sdk_type = 'EMPTY'
                    elif not contract_code.startswith(b'\0asm'):
                        contract_sdk_type = 'NOT_WASM'
                    else:
                        likely_sdk_types = set()
                        if b'__data_end' in contract_code and b'__heap_base' in contract_code:
                            likely_sdk_types.add('RS')
                        if b'JS_TAG_MODULE' in contract_code and b'quickjs-libc-min.js' in contract_code:
                            likely_sdk_types.add('JS')
                        if b'l\x00i\x00b\x00/\x00a\x00s\x00s\x00e\x00m\x00b\x00l\x00y\x00s\x00c\x00r\x00i\x00p\x00t' in contract_code \
                            or b'~lib/near-sdk-core/collections/persistentMap/PersistentMap' in contract_code:
                            likely_sdk_types.add('AS')

                        # Only set the sdk type if exactly one match is received since if we matched multiple, it is impossible to make a call.
                        contract_sdk_type = likely_sdk_types.pop() if len(likely_sdk_types) == 1 else 'UNKNOWN'
                        print(contract_account_id, contract_sdk_type, likely_sdk_types)

                    contract_sdk_types.append((contract_code_sha256, contract_sdk_type))

                sql_update_contract_sdk_types = ";".join(f"""UPDATE deployed_contracts__TEST SET contract_sdk_type = '{contract_sdk_type}' WHERE contract_code_sha256 = '{contract_code_sha256}'""" for (contract_code_sha256, contract_sdk_type) in contract_sdk_types)
                analytics_cursor.execute(sql_update_contract_sdk_types)
                self.analytics_connection.commit()
                print('done updating')

