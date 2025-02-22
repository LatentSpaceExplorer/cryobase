from src.utils.db import table_exists


def amm_v3_stats_analysis(ch_client, chain_name):

    # create table and populate
    if table_exists(ch_client, chain_name + "_derived", "amm_v3_stats") == False:

        ch_client.command(create_amm_v3_stats_table_cmd(chain_name))

        ch_client.command(amm_v3_stats_mv_cmd(chain_name))

        print(f"Created {chain_name}_derived.amm_v3_stats table + materialized view and populated it")



def create_amm_v3_stats_table_cmd(chain_name):
    return f"""
        CREATE TABLE IF NOT EXISTS {chain_name}_derived.amm_v3_stats
        (
            pool FixedString(20),
            date Date,
            swap_count UInt64,
            volume0 UInt256,
            volume1 UInt256
        ) ENGINE = SummingMergeTree
        ORDER BY (pool, date)
        AS
        SELECT
            pool,
            toDate(timestamp) AS date,
            count(*) as swap_count,
            SUM(ABS(amount0)) AS volume0,
            SUM(ABS(amount1)) AS volume1
        FROM {chain_name}_derived.amm_v3_swaps
        GROUP BY pool, date
    """


def amm_v3_stats_mv_cmd(chain_name):
    return f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {chain_name}_derived.amm_v3_stats_mv
        TO {chain_name}_derived.amm_v3_stats
        AS
        SELECT
            pool,
            toDate(timestamp) AS date,
            count(*) as swap_count,
            SUM(ABS(amount0)) AS volume0,
            SUM(ABS(amount1)) AS volume1
        FROM {chain_name}_derived.amm_v3_swaps
        GROUP BY pool, date
    """