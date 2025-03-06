from src.utils.db import table_exists


def amm_v2_swaps_analysis(ch_client, chain_name):

    if table_exists(ch_client, chain_name + "_derived", "amm_v2_swaps") == False:
        print(f"Creating {chain_name}_derived.amm_v2_swaps table")
            
        ch_client.command(amm_v2_swaps_table_cmd(chain_name))

        print(f"Created {chain_name}_derived.amm_v2_swaps table and populated it")

        ch_client.command(amm_v2_swaps_mv_cmd(chain_name))

        print(f"Created {chain_name}_derived.amm_v2_swaps_mv materialized view")



def amm_v2_swaps_table_cmd(chain_name):
    return f"""
        CREATE TABLE IF NOT EXISTS {chain_name}_derived.amm_v2_swaps
        (
            pool FixedString(20),
            block_number UInt64,
            timestamp DateTime,
            transaction_hash FixedString(32),
            transaction_index UInt64,
            log_index UInt64,
            
            sender FixedString(20),
            recipient FixedString(20),
            amount0In UInt256,
            amount1In UInt256,
            amount0Out UInt256,
            amount1Out UInt256,
        )
        ENGINE = MergeTree
        ORDER BY (pool, block_number, timestamp, transaction_hash, transaction_index, log_index)
        AS 
        SELECT    
            address AS pool,
            block_number,
            timestamp,
            transaction_hash,
            transaction_index,
            log_index,

            substring(topic1, -20) as sender,
            substring(topic2, -20) as recipient,
            reinterpretAsUInt256(reverse(substring(data, 1, 32))) AS amount0In,
            reinterpretAsUInt256(reverse(substring(data, 33, 32))) AS amount1In,
            reinterpretAsUInt256(reverse(substring(data, 65, 32))) AS amount0Out,
            reinterpretAsUInt256(reverse(substring(data, 97, 32))) AS amount1Out
        FROM {chain_name}_raw.logs
        WHERE
            topic0 = unhex('d78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822');
        """



def amm_v2_swaps_mv_cmd(chain_name):
    return f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {chain_name}_derived.amm_v2_swaps_mv
        TO {chain_name}_derived.amm_v2_swaps
        AS
        SELECT
            address AS pool,
            block_number,
            timestamp,
            transaction_hash,
            transaction_index,
            log_index,

            substring(topic1, -20) as sender,
            substring(topic2, -20) as recipient,
            reinterpretAsUInt256(reverse(substring(data, 1, 32))) AS amount0In,
            reinterpretAsUInt256(reverse(substring(data, 33, 32))) AS amount1In,
            reinterpretAsUInt256(reverse(substring(data, 65, 32))) AS amount0Out,
            reinterpretAsUInt256(reverse(substring(data, 97, 32))) AS amount1Out
        FROM {chain_name}_raw.logs
        WHERE
            topic0 = unhex('d78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822');
        """