from src.db.client import get_db_client_from_config
from src.chain_rpc.client import get_web3_client
from src.db.cryo_to_db import cryo_to_db
from src.derived_analysis.derived_analysis import run_derived_analysis
import toml


def main():
    
    # Load config
    with open("config.toml", "r") as f:
        config = toml.load(f)

    # Get cryo settings from config
    temp_cryo_data_path = config["cryo"]["temp_data_path"]
    safe_block_margin = config["cryo"]["safe_block_margin"]


    # Initialize DB connection
    ch_client = get_db_client_from_config(config["clickhouse"])


    # run cryobase for each chain
    for chain_name, chain_config in config["chains"].items():

        print(f"Running cryobase for chain: {chain_name}")

        chain_rpc_url = chain_config["rpc_url"]
        chain_datasets = chain_config["datasets"]

        # test connection to web3 node
        try:
            get_web3_client(chain_rpc_url)
        except Exception as e:
            print(f"Skipping chain {chain_name} due to connection error with web3 client: {e}")
            continue

        # Raw Data Processing
        cryo_to_db(ch_client, chain_datasets, temp_cryo_data_path, chain_name, chain_rpc_url, safe_block_margin)

        # Run all derived analysis
        run_derived_analysis(ch_client, chain_name, chain_rpc_url, analysis_types=["token", "amm", "account"])


if __name__ == "__main__":
    main()

