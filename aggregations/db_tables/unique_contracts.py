import base64
import near_api
import os
import traceback


from . import DAY_LEN_SECONDS, daily_start_of_range, time_range_json
from ..periodic_aggregations import PeriodicAggregations


class UniqueContracts(PeriodicAggregations):
    DEPENDENCIES = ["deployed_contracts"]

    @property
    def sql_create_table(self):
        return """
            CREATE TABLE IF NOT EXISTS unique_contracts
            (
                contract_code_sha256              text           PRIMARY KEY,
                contract_sdk_type                 text           NOT NULL DEFAULT '',
                first_deployed_to_account_id      text           NOT NULL,
                first_deployed_by_receipt_id      text           NOT NULL,
                -- It is important to store here not only day, but the exact timestamp
                -- Because there could be several deployments at the same day
                first_deployed_at_block_timestamp numeric(20, 0) NOT NULL,
                first_deployed_at_block_hash      text           NOT NULL
            );
            CREATE INDEX IF NOT EXISTS unique_contracts_contract_sdk_type_idx
                ON unique_contracts (contract_sdk_type);
            CREATE INDEX IF NOT EXISTS unique_contracts_timestamp_idx
                ON unique_contracts (first_deployed_at_block_timestamp);
            CREATE INDEX IF NOT EXISTS unique_contracts_first_deployed_to_account_id_idx
                ON unique_contracts (first_deployed_to_account_id);
        """

    @property
    def sql_drop_table(self):
        return """
            DROP TABLE IF EXISTS unique_contracts
        """

    @property
    def sql_select(self):
        raise NotImplementedError(
            "No requests to Indexer DB needed for unique_contracts"
        )

    @property
    def sql_insert(self):
        return """
            INSERT INTO unique_contracts (
                contract_code_sha256,
                first_deployed_to_account_id,
                first_deployed_by_receipt_id,
                first_deployed_at_block_timestamp,
                first_deployed_at_block_hash
            ) VALUES %s
            ON CONFLICT DO NOTHING
        """

    @property
    def duration_seconds(self):
        return DAY_LEN_SECONDS

    def start_of_range(self, timestamp: int) -> int:
        return daily_start_of_range(timestamp)

    def collect(self, requested_timestamp: int) -> list:
        new_unique_contracts_select = """
            SELECT
                DISTINCT contract_code_sha256,
                deployed_to_account_id,
                deployed_by_receipt_id,
                deployed_at_block_timestamp,
                deployed_at_block_hash
            FROM deployed_contracts
            WHERE deployed_at_block_timestamp >= %(from_timestamp)s
                AND deployed_at_block_timestamp < %(to_timestamp)s
            ORDER BY contract_code_sha256, deployed_at_block_timestamp
        """

        from_timestamp = self.start_of_range(requested_timestamp)
        with self.analytics_connection.cursor() as analytics_cursor:
            analytics_cursor.execute(
                new_unique_contracts_select,
                time_range_json(from_timestamp, self.duration_seconds),
            )
            result = analytics_cursor.fetchall()
            return self.prepare_data(result, start_of_range=from_timestamp)

    @staticmethod
    def prepare_data(parameters: list, *, start_of_range=None, **kwargs) -> list:
        print("INFO: Preparing unique_contracts...")
        return parameters

    def store(self, parameters: list) -> list:
        print("INFO: Storing unique_contracts...")
        super().store(parameters)

        print("INFO: Updating SDK types in unique_contracts...")

        near_rpc_url = os.getenv("NEAR_RPC_URL")
        if not near_rpc_url:
            print("WARN: NEAR_RPC_URL is not set, so contract SDK types won't be set")
            return
        near_rpc = near_api.providers.JsonProvider(near_rpc_url)

        sql_missing_sdk_type = """
            SELECT contract_code_sha256, first_deployed_to_account_id, first_deployed_at_block_hash
            FROM unique_contracts
            WHERE contract_sdk_type = ''
            LIMIT 100
        """

        with self.analytics_connection.cursor() as analytics_cursor:
            while True:
                analytics_cursor.execute(sql_missing_sdk_type)
                unknown_contracts = analytics_cursor.fetchall()
                print(
                    f"INFO: There are {len(unknown_contracts)} unique contracts pending for SDK type identification..."
                )
                if not unknown_contracts:
                    break

                contract_sdk_types = []
                for (
                    contract_code_sha256,
                    contract_account_id,
                    deployed_at_block_hash,
                ) in unknown_contracts:
                    print(
                        f"INFO: Fetching contract code for {contract_account_id} at block {deployed_at_block_hash}..."
                    )
                    contract_code = download_contract_code(
                        near_rpc, contract_account_id, deployed_at_block_hash
                    )

                    contract_sdk_type = get_contract_sdk_type(
                        contract_code, contract_code_sha256
                    )

                    contract_sdk_types.append((contract_code_sha256, contract_sdk_type))

                sql_update_contract_sdk_types = ";".join(
                    f"""UPDATE unique_contracts SET contract_sdk_type = '{contract_sdk_type}' WHERE contract_code_sha256 = '{contract_code_sha256}'"""
                    for (contract_code_sha256, contract_sdk_type) in contract_sdk_types
                )
                analytics_cursor.execute(sql_update_contract_sdk_types)
                self.analytics_connection.commit()

        print("INFO: Finished updating unique_contracts")


def download_contract_code(
    near_rpc: near_api.providers.JsonProvider, account_id: str, block_id: str
) -> bytes:
    for _ in range(1000):
        try:
            response = near_rpc.json_rpc(
                "query",
                {
                    "request_type": "view_code",
                    "account_id": account_id,
                    "block_id": block_id,
                },
            )
        except near_api.providers.JsonProviderError as e:
            if e.args[0].get("cause", {}).get("name") == "UNKNOWN_ACCOUNT":
                return b""
            print("WARN: Retrying fetching contract code...")
            traceback.print_exc()
        except Exception:
            print("WARN: Retrying fetching contract code...")
            traceback.print_exc()
        else:
            return base64.b64decode(response["code_base64"])

    raise Exception("Could not download contract code even after 1000 retries")


def get_contract_sdk_type(contract_code: bytes, contract_code_sha256: str) -> str:
    # Since there is no way to remove a contract once deployed, users can only deploy an empty file to reduce the storage usage.
    if contract_code == b"":
        return "EMPTY"

    # Sometimes people deploy some garbage (images, text files, etc).
    if not contract_code.startswith(b"\0asm"):
        return "NOT_WASM"

    likely_sdk_types = set()

    if b"__data_end" in contract_code and b"__heap_base" in contract_code:
        likely_sdk_types.add("RS")

    if b"JS_TAG_MODULE" in contract_code and b"quickjs-libc-min." in contract_code:
        likely_sdk_types.add("JS")

    if (
        b"l\x00i\x00b\x00/\x00a\x00s\x00s\x00e\x00m\x00b\x00l\x00y\x00s\x00c\x00r\x00i\x00p\x00t"
        in contract_code
        or b"~lib/near-sdk-core/collections/persistentMap/PersistentMap"
        in contract_code
    ):
        likely_sdk_types.add("AS")

    # Only set the sdk type if exactly one match is received since if we matched multiple, it is impossible to make a call.
    if len(likely_sdk_types) == 1:
        return likely_sdk_types.pop()
    else:
        if len(likely_sdk_types) > 1:
            print(
                f"WARN: We detected markers of several programming languages ({likely_sdk_types}) at once for contract with hash {contract_code_sha256}, falling back to UNKNOWN type..."
            )
        return "UNKNOWN"
