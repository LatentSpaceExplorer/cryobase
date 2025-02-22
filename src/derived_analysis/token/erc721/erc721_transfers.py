from src.utils.db import table_exists


def erc721_transfers_analysis(ch_client, chain_name):

    if table_exists(ch_client, chain_name + "_derived", "erc721_transfers") == False:
        print(f"Creating {chain_name}_derived.erc721_transfers table")
            
        ch_client.command(erc721_transfers_table_cmd(chain_name))

        print(f"Created {chain_name}_derived.erc721_transfers table and populated it")

        ch_client.command(erc721_transfers_mv_cmd(chain_name))

        print(f"Created {chain_name}_derived.erc721_transfers_mv materialized view")


def erc721_transfers_table_cmd(chain_name):

    return f"""
        CREATE TABLE IF NOT EXISTS {chain_name}_derived.erc721_transfers 
        (
            contract FixedString(20),
            block_number UInt64,
            timestamp UInt64,
            transaction_hash FixedString(32),
            transaction_index UInt64,
            log_index UInt64,

            from FixedString(20),
            to FixedString(20),
            tokenId UInt256
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

            substring(topic1, -20) AS from,
            substring(topic2, -20) AS to,
            reinterpretAsUInt256(reverse(topic3)) AS tokenId
        FROM {chain_name}_raw.logs
        WHERE
            topic0 = unhex('ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef')
            AND topic1 IS NOT NULL
            AND topic2 IS NOT NULL
            AND topic3 IS NOT NULL;
        """



def erc721_transfers_mv_cmd(chain_name):

    return f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {chain_name}_derived.erc721_transfers_mv
        TO {chain_name}_derived.erc721_transfers
        AS
        SELECT
            address AS contract,
            block_number,
            timestamp,
            transaction_hash,
            transaction_index,
            log_index,
            substring(topic1, -20) AS from,
            substring(topic2, -20) AS to,
            reinterpretAsUInt256(reverse(topic3)) AS tokenId
        FROM {chain_name}_raw.logs
        WHERE
            topic0 = unhex('ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef')
            AND topic1 IS NOT NULL
            AND topic2 IS NOT NULL
            AND topic3 IS NOT NULL;
        """

