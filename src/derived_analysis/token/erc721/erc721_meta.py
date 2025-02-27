import multiprocessing
from tqdm import tqdm
from web3 import Web3
from src.utils.db import table_exists
from src.db.client import get_db_client
from src.chain_rpc.client import get_web3_client


erc721_api = [
    {"inputs":[],"name":"name","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},
    {"inputs":[],"name":"symbol","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"}
]


def erc721_meta_analysis(ch_client, chain_name, rpc_url):
    # create table and populate
    if table_exists(ch_client, chain_name + "_derived", "erc721_meta") == False:
        print(f"Creating {chain_name}_derived.erc721_meta table")
        ch_client.command(create_erc721_meta_table_cmd(chain_name))

    process_erc721_meta(ch_client, chain_name, rpc_url)


def create_erc721_meta_table_cmd(chain_name):
    return f"""
        CREATE TABLE IF NOT EXISTS {chain_name}_derived.erc721_meta
        (
            contract FixedString(20),
            name Nullable(String),
            symbol Nullable(String)
        )
        ENGINE = MergeTree()
        ORDER BY (contract)
    """


def process_erc721_meta(ch_client, chain_name, rpc_url):
    # find contracts that are in erc721_transfers but not in erc721_meta
    all_new_erc721s_q = ch_client.query(f""" 
        SELECT DISTINCT contract
        FROM {chain_name}_derived.erc721_transfers
        WHERE contract NOT IN (SELECT DISTINCT contract FROM {chain_name}_derived.erc721_meta)
    """).result_columns

    if len(all_new_erc721s_q) == 0:
        print(f"No new erc721s found for {chain_name}_derived.erc721_meta")
        return

    all_new_erc721s = all_new_erc721s_q[0]

    print(f"Total remaining new erc721s: {len(all_new_erc721s)} for {chain_name}_derived.erc721_meta")

    # Create batches
    batch_size = 1_000
    new_erc721s_batches = [all_new_erc721s[i:i + batch_size] for i in range(0, len(all_new_erc721s), batch_size)]

    # Setup multiprocessing pool
    pool = multiprocessing.Pool(processes=16)
    jobs = []

    for batch in new_erc721s_batches:
        job = pool.apply_async(update_contract_details_batch, (batch, chain_name, "erc721_meta", rpc_url))
        jobs.append(job)

    # Wait for all jobs to complete
    for job in tqdm(jobs):
        job.get()

    pool.close()
    pool.join()


def update_contract_details_batch(contracts, chain_name, table_name, rpc_url):
    ch_client = get_db_client(host='localhost', port=18123)
    w3 = get_web3_client(rpc_url)

    def call_contract(function):
        try:
            return function().call()
        except:
            return None

    for contract in contracts:
        hex_contract = Web3.to_checksum_address(contract)
        contract_abi = w3.eth.contract(address=hex_contract, abi=erc721_api)

        name = call_contract(contract_abi.functions.name)
        symbol = call_contract(contract_abi.functions.symbol)

        if name == None:
            name = 'NULL'
        else:
            name = "'" + name.replace("'", "''").replace("\\", "\\\\") + "'"

        if symbol == None:
            symbol = 'NULL'
        else:
            symbol = "'" + symbol.replace("'", "''").replace("\\", "\\\\") + "'"

        # Execute the update command
        ch_client.command(f"""
            INSERT INTO {chain_name}_derived.{table_name}
            VALUES 
            (
                UNHEX('{hex_contract.replace('0x', '')}'), 
                {name}, 
                {symbol}
            )
        """) 