from src.utils.db import table_exists


def amm_v3_swaps_analysis(ch_client, chain_name):

    if table_exists(ch_client, chain_name + "_derived", "amm_v3_swaps") == False:
        print(f"Creating {chain_name}_derived.amm_v3_swaps table")
            
        ch_client.command(amm_v3_swaps_table_cmd(chain_name))

        print(f"Created {chain_name}_derived.amm_v3_swaps table and populated it")

        ch_client.command(amm_v3_swaps_mv_cmd(chain_name))

        print(f"Created {chain_name}_derived.amm_v3_swaps_mv materialized view")


def amm_v3_swaps_table_cmd(chain_name):
    return f"""
        CREATE TABLE IF NOT EXISTS {chain_name}_derived.amm_v3_swaps
        (
            pool FixedString(20),
            block_number UInt64,
            timestamp UInt64,
            transaction_hash FixedString(32),
            transaction_index UInt64,
            log_index UInt64,
            sender FixedString(20),
            recipient FixedString(20),
            amount0 Int256,
            amount1 Int256,
            sqrtPriceX96 UInt256,
            liquidity UInt128,
            tick Int32
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
            reinterpretAsInt256(reverse(substring(data, 1, 32))) AS amount0,
            reinterpretAsInt256(reverse(substring(data, 33, 32))) AS amount1,
            reinterpretAsUInt256(reverse(substring(data, 65, 32))) AS sqrtPriceX96,
            reinterpretAsUInt128(reverse(substring(data, 97, 32))) AS liquidity,
            reinterpretAsInt32(reverse(substring(data, 129, 32))) AS tick
        FROM {chain_name}_raw.logs
        WHERE
            topic0 = unhex('c42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67');
        """


def amm_v3_swaps_mv_cmd(chain_name):
    return f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {chain_name}_derived.amm_v3_swaps_mv
        TO {chain_name}_derived.amm_v3_swaps
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
            reinterpretAsInt256(reverse(substring(data, 1, 32))) AS amount0,
            reinterpretAsInt256(reverse(substring(data, 33, 32))) AS amount1,
            reinterpretAsUInt256(reverse(substring(data, 65, 32))) AS sqrtPriceX96,
            reinterpretAsUInt128(reverse(substring(data, 97, 32))) AS liquidity,
            reinterpretAsInt32(reverse(substring(data, 129, 32))) AS tick
        FROM {chain_name}_raw.logs
        WHERE
            topic0 = unhex('c42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67');
        """