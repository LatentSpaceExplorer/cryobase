import multiprocessing
from tqdm import tqdm
from web3 import Web3
from src.utils.db import table_exists
from src.db.client import get_db_client
from src.chain_rpc.client import get_web3_client


# "PoolCreated(address indexed token0, address indexed token1, uint24 indexed fee, int24 tickSpacing, address pool)"

v3_pool_api = [{"inputs":[],"name":"token0","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"token1","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"}]


# maybe add fee or other metadata 
def amm_v3_pools_analysis(ch_client, chain_name, rpc_url):

    # create table and populate
    if table_exists(ch_client, chain_name + "_derived", "amm_v3_pools") == False:
            
        ch_client.command(amm_v3_pools_table_cmd(chain_name))

        print(f"Created {chain_name}_derived.amm_v3_pools table")


    process_amm_v3_pools(ch_client, chain_name, rpc_url)



def process_amm_v3_pools(ch_client, chain_name, rpc_url):

    # find pools that are in amm_v3_swaps but not in amm_v3_pools
    all_new_v3_pools_q = ch_client.query(f""" 
        SELECT DISTINCT pool
        FROM {chain_name}_derived.amm_v3_swaps
        WHERE pool NOT IN (SELECT DISTINCT pool FROM {chain_name}_derived.amm_v3_pools)
    """).result_columns

    if len(all_new_v3_pools_q) == 0:
        print(f"No new v3 pools found for {chain_name}_derived.amm_v3_pools")
        return

    all_new_v3_pools = all_new_v3_pools_q[0]

    print(f"Total remaining new v3 pools: {len(all_new_v3_pools)} for {chain_name}_derived.amm_v3_pools")


    # Create batches
    batch_size = 1_000
    new_erc20s_batches = [all_new_v3_pools[i:i + batch_size] for i in range(0, len(all_new_v3_pools), batch_size)]

    # Setup multiprocessing pool
    pool = multiprocessing.Pool(processes=16) # multiprocessing.cpu_count()
    jobs = []

    for batch in new_erc20s_batches:
        job = pool.apply_async(get_pool_meta_data, (batch, chain_name, "amm_v3_pools", rpc_url))
        jobs.append(job)

    # Wait for all jobs to complete
    for job in tqdm(jobs):
        job.get()

    pool.close()
    pool.join()



def get_pool_meta_data(pools, chain_name, table_name, rpc_url):

    ch_client = get_db_client(host='localhost', port=18123)
    w3 = get_web3_client(rpc_url)


    for pool in pools:

        hex_contract = Web3.to_checksum_address(pool)
        pool_contract = w3.eth.contract(address=hex_contract, abi=v3_pool_api)

        token0 = call_contract(pool_contract.functions.token0)
        token1 = call_contract(pool_contract.functions.token1)


        # Handle None values
        token0_hex = f"UNHEX('{token0.replace('0x', '')}')" if token0 else "NULL"
        token1_hex = f"UNHEX('{token1.replace('0x', '')}')" if token1 else "NULL"


        # Execute the update command
        ch_client.command(f"""
            INSERT INTO {chain_name}_derived.{table_name}
            VALUES 
            (
                UNHEX('{hex_contract.replace('0x', '')}'), 
                {token0_hex},
                {token1_hex}
            )
        """)



def amm_v3_pools_table_cmd(chain_name):
    return f"""
        CREATE TABLE IF NOT EXISTS {chain_name}_derived.amm_v3_pools
        (
            pool FixedString(20),
            token0 Nullable(FixedString(20)),
            token1 Nullable(FixedString(20))
        )
        ENGINE = MergeTree()
        ORDER BY (pool)
    """



def call_contract(function):
    try:
        return function().call()
    except:
        return None
    


