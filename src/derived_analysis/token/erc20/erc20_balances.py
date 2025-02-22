from tqdm import tqdm
from src.utils.db import table_exists




def erc20_balance_analysis(ch_client, chain_name):

    if table_exists(ch_client, chain_name + "_derived", "erc20_balances") == False:

        print(f"Creating {chain_name}_derived.erc20_balances table")
            
        ch_client.command(erc20_balances_table_cmd(chain_name))
        populate_erc20_balances_by_blocks(ch_client, chain_name, 1_000_000)

        print(f"Created {chain_name}_derived.erc20_balances table and populated it")

        # ch_client.command(erc20_balances_table_mv_cmd(chain_name))
        ch_client.command(erc20_balances_from_mv_cmd(chain_name))
        ch_client.command(erc20_balances_to_mv_cmd(chain_name))

        print(f"Created {chain_name}_derived.erc20_balances_mv materialized view")




def erc20_balances_table_cmd(chain_name):
    return f"""
        CREATE TABLE IF NOT EXISTS {chain_name}_derived.erc20_balances
        (
            contract FixedString(20),
            address FixedString(20),
            balance Int256
        )
        ENGINE = SummingMergeTree
        ORDER BY (contract, address);
        """



def populate_erc20_balances_by_blocks(ch_client, chain_name, block_chunk_size):
    # Get min and max block numbers
    min_max_blocks = ch_client.query(f"""
        SELECT 
            MIN(block_number) as min_block,
            MAX(block_number) as max_block 
        FROM {chain_name}_derived.erc20_transfers
    """).result_rows[0]
    
    min_block = min_max_blocks[0]
    max_block = min_max_blocks[1]

    for start_block in tqdm(range(min_block, max_block + 1, block_chunk_size)):

        end_block = min(start_block + block_chunk_size - 1, max_block)

        print(f"Processing blocks {start_block} to {end_block}")

        ch_client.command(populate_erc20_holders_balance_block_chunk_cmd(chain_name, start_block, end_block))



def populate_erc20_holders_balance_block_chunk_cmd(chain_name, start_block, end_block):
    return f"""
        INSERT INTO {chain_name}_derived.erc20_balances
        SELECT
            contract,
            address,
            sum(value) AS balance
        FROM
        (
            SELECT
                contract,
                from AS address,
                -toInt256(value) AS value
            FROM {chain_name}_derived.erc20_transfers
            WHERE block_number BETWEEN {start_block} AND {end_block}
            UNION ALL
            SELECT
                contract,
                to AS address,
                toInt256(value) AS value
            FROM {chain_name}_derived.erc20_transfers
            WHERE block_number BETWEEN {start_block} AND {end_block}
        )
        GROUP BY contract, address;
        """



def erc20_balances_from_mv_cmd(chain_name):
    return f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {chain_name}_derived.erc20_balances_from_mv
        TO {chain_name}_derived.erc20_balances
        AS
        SELECT
            contract,
            from AS address,
            -sum(toInt256(value)) AS balance
        FROM {chain_name}_derived.erc20_transfers
        GROUP BY contract, address;
    """

def erc20_balances_to_mv_cmd(chain_name):
    return f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {chain_name}_derived.erc20_balances_to_mv
        TO {chain_name}_derived.erc20_balances
        AS
        SELECT
            contract,
            to AS address,
            sum(toInt256(value)) AS balance
        FROM {chain_name}_derived.erc20_transfers
        GROUP BY contract, address;
    """

