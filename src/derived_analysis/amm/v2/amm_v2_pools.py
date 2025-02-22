from src.utils.db import table_exists


def amm_v2_pools_analysis(ch_client, chain_name):

    if table_exists(ch_client, chain_name + "_derived", "amm_v2_pools") == False:
        print(f"Creating {chain_name}_derived.amm_v2_pools table")
            
        ch_client.command(amm_v2_pools_table_cmd(chain_name))

        print(f"Created {chain_name}_derived.amm_v2_pools table and populated it")

        ch_client.command(amm_v2_pools_mv_cmd(chain_name))

        print(f"Created {chain_name}_derived.amm_v2_pools_mv materialized view")



def amm_v2_pools_table_cmd(chain_name):
    return f"""
        CREATE TABLE IF NOT EXISTS {chain_name}_derived.amm_v2_pools
        (
            factory FixedString(20),
            block_number UInt64,
            timestamp UInt64,
            transaction_hash FixedString(32),
            transaction_index UInt64,
            log_index UInt64,

            pool FixedString(20),
            token0 FixedString(20),
            token1 FixedString(20),
            pool_index UInt256,

        )
        ENGINE = MergeTree
        ORDER BY (pool, block_number, timestamp, transaction_hash, transaction_index, log_index)
        AS 
        SELECT    
            address AS factory,
            block_number,
            timestamp,
            transaction_hash,
            transaction_index,
            log_index,

            toFixedString(substring(data, 13, 20), 20) AS pool,
            substring(topic1, -20) as token0,
            substring(topic2, -20) as token1,
            reinterpretAsUInt256(reverse(substring(data, 33, 32))) AS pool_index
        FROM {chain_name}_raw.logs
        WHERE
            topic0 = unhex('0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9');
        """



def amm_v2_pools_mv_cmd(chain_name):
    return f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {chain_name}_derived.amm_v2_pools_mv
        TO {chain_name}_derived.amm_v2_pools
        AS
        SELECT
            address AS factory,
            block_number,
            timestamp,
            transaction_hash,
            transaction_index,
            log_index,

            toFixedString(substring(data, 13, 20), 20) AS pool,
            substring(topic1, -20) as token0,
            substring(topic2, -20) as token1,
            reinterpretAsUInt256(reverse(substring(data, 33, 32))) AS pool_index
        FROM {chain_name}_raw.logs
        WHERE
            topic0 = unhex('0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9');
        """