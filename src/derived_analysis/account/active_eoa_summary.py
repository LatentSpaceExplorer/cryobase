from tqdm import tqdm
from src.utils.db import table_exists


def active_eoa_summary_analysis(ch_client, chain_name):

    if table_exists(ch_client, chain_name + "_derived", "active_eoa_summary") == False:

        print(f"Creating {chain_name}_derived.active_eoa_summary table")
            
        ch_client.command(active_eoa_summary_table_cmd(chain_name))
        populate_active_eoa_summary_in_batches(ch_client, chain_name, 250_000_000)

        print(f"Created {chain_name}_derived.active_eoa_summary table and populated it")

        ch_client.command(active_aoe_summary_from_mv_cmd(chain_name))

        print(f"Created {chain_name}_derived.active_aoe_summary_mv materilaised view")



def active_eoa_summary_table_cmd(chain_name):
    return f"""
        CREATE TABLE IF NOT EXISTS {chain_name}_derived.active_eoa_summary 
        (
            eoa FixedString(20),
            --
            number_of_transactions SimpleAggregateFunction(sum, UInt64),
            --
            first_transaction_time SimpleAggregateFunction(min, DateTime),
            last_transaction_time SimpleAggregateFunction(max, DateTime),
            --
            total_gas_used SimpleAggregateFunction(sum, UInt64),
            --
            total_value_transferred SimpleAggregateFunction(sum, UInt256),
            average_transaction_value AggregateFunction(avg, UInt256),
            max_transaction_value SimpleAggregateFunction(max, UInt256),
            min_transaction_value SimpleAggregateFunction(min, UInt256),
            --
            number_of_unique_to AggregateFunction(uniq, FixedString(20)),
            --
            number_of_contracts_created SimpleAggregateFunction(sum, UInt64)
        )
        ENGINE = AggregatingMergeTree
        ORDER BY eoa;
    """



selection = """
    from_address AS eoa,
    --
    count() AS number_of_transactions,
    --
    min(timestamp) AS first_transaction_time,
    max(timestamp) AS last_transaction_time,
    --
    sum(gas_used) AS total_gas_used,
    --
    sum(value) AS total_value_transferred,
    avgState(value) AS average_transaction_value,
    max(value) AS max_transaction_value,
    min(value) AS min_transaction_value,
    --
    uniqState(COALESCE(to_address, toFixedString(UNHEX('0000000000000000000000000000000000000000'), 20))) AS number_of_unique_to,
    --
    sum(if(contract_address IS NOT NULL, 1, 0)) AS number_of_contracts_created
"""


def populate_active_eoa_summary_in_batches(ch_client, chain_name, batch_size = 1_000_000):
    offset = 0

    total_rows_to_process = ch_client.query(f"SELECT COUNT(*) FROM {chain_name}_raw.transactions").result_columns[0][0]

    for offset in tqdm(range(0, total_rows_to_process, batch_size)):
        ch_client.command(populate_active_eoa_summary_batch_cmd(chain_name, batch_size, offset))




def populate_active_eoa_summary_batch_cmd(chain_name, batch_size, offset):
    return f"""
        INSERT INTO {chain_name}_derived.active_eoa_summary
        SELECT
            {selection}
        FROM 
        (
            SELECT *
            FROM {chain_name}_raw.transactions
            ORDER BY (block_number, timestamp, transaction_hash, transaction_index)
            LIMIT {batch_size} OFFSET {offset}
        )
        GROUP BY eoa
        """



def active_aoe_summary_from_mv_cmd(chain_name):
    return f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {chain_name}_derived.active_aoe_summary_mv
        TO {chain_name}_derived.active_eoa_summary
        AS
        SELECT
            {selection}
        FROM {chain_name}_raw.transactions
        GROUP BY from_address
    """