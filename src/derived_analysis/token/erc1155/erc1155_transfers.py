from src.utils.db import table_exists


def erc1155_transfers_analysis(ch_client, chain_name):
    if not table_exists(ch_client, chain_name + "_derived", "erc1155_transfers"):
        print(f"Creating {chain_name}_derived.erc1155_transfers table")
        
        # Create the table (with the initial SELECT to populate).
        ch_client.command(erc1155_transfers_table_cmd(chain_name))
        print(f"Created {chain_name}_derived.erc1155_transfers table and populated it")
        
        # Create the materialized view that inserts into the table continuously.
        ch_client.command(erc1155_transfers_mv_cmd(chain_name))
        print(f"Created {chain_name}_derived.erc1155_transfers_mv materialized view")


def erc1155_transfers_table_cmd(chain_name):
    return f"""
        CREATE TABLE IF NOT EXISTS {chain_name}_derived.erc1155_transfers
        (
            contract FixedString(20),
            block_number UInt64,
            timestamp UInt64,
            transaction_hash FixedString(32),
            transaction_index UInt64,
            log_index UInt64,

            operator FixedString(20),
            from FixedString(20),
            to FixedString(20),
            id UInt256,
            value UInt256
        )
        ENGINE = MergeTree
        ORDER BY (contract, block_number, timestamp, transaction_hash, transaction_index, log_index)
        AS
        SELECT
            address AS contract,
            block_number,
            timestamp,
            transaction_hash,
            transaction_index,
            log_index,

            substring(topic1, -20) AS operator,
            substring(topic2, -20) AS from,
            substring(topic3, -20) AS to,
            reinterpretAsUInt256(reverse(substring(data, 1, 32))) AS id,
            reinterpretAsUInt256(reverse(substring(data, 33, 32))) AS value
        FROM {chain_name}_raw.logs
        WHERE
            topic0 = unhex('c3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62')
            AND topic1 IS NOT NULL
            AND topic2 IS NOT NULL
            AND topic3 IS NOT NULL;
    """


def erc1155_transfers_mv_cmd(chain_name):
    return f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {chain_name}_derived.erc1155_transfers_mv
        TO {chain_name}_derived.erc1155_transfers
        AS
        SELECT
            address AS contract,
            block_number,
            timestamp,
            transaction_hash,
            transaction_index,
            log_index,

            substring(topic1, -20) AS operator,
            substring(topic2, -20) AS from,
            substring(topic3, -20) AS to,
            reinterpretAsUInt256(reverse(substring(data, 1, 32))) AS id,
            reinterpretAsUInt256(reverse(substring(data, 33, 32))) AS value
        FROM {chain_name}_raw.logs
        WHERE
            topic0 = unhex('c3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62')
            AND topic1 IS NOT NULL
            AND topic2 IS NOT NULL
            AND topic3 IS NOT NULL;
    """

