import os
import subprocess

from src.db.insertion import insert_into_db
from src.db.tables import create_table_cmd, create_staging_table_cmd
from src.chain_rpc.client import get_web3_client



def cryo_to_db(ch_client, datasets, temp_data_path, chain_name, rpc_url, safe_block_margin):
   

    # create dbs
    ch_client.command("CREATE DATABASE IF NOT EXISTS " + chain_name + "_raw")
    ch_client.command("CREATE DATABASE IF NOT EXISTS " + chain_name + "_derived")


    # web3 setup
    w3 = get_web3_client(rpc_url)

    # get highest block from web3 client
    highest_block_from_web3 = w3.eth.get_block('latest').number

    # run cryo for each dataset and write to db
    for dataset in datasets:
        
        # delete all files temp folder
        delete_files_in_directory(temp_data_path)

        # create table if it doesn't exist
        ch_client.command(create_table_cmd(chain_name, dataset))

        if dataset in ["native_transfers", "contracts"]:
            # drop staging table
            ch_client.command("DROP TABLE IF EXISTS " + chain_name + "_raw." + dataset + "_staging" + " SETTINGS max_table_size_to_drop = 0;")
            # create new staging table
            ch_client.command(create_staging_table_cmd(chain_name, dataset))

        # get highest block from db
        highest_block_from_db = ch_client.command("SELECT max(block_number) FROM " + chain_name + "_raw." + dataset)

        # determine block range
        from_block = highest_block_from_db + 1 if highest_block_from_db > 0 else 0
        to_block = highest_block_from_web3 - safe_block_margin

        # info
        print()
        print("chain:", chain_name, "| dataset:", dataset, "| from_block:", from_block, "| to_block:", to_block)


        # continue if no new blocks
        if from_block >= to_block:
            print("No new blocks")
            continue


        # collect with cryo and write to /raid/cryo/temp
        run_cryo_dataset(temp_data_path, dataset, chain_name, rpc_url, from_block, to_block)


        # write to clickhouse
        insert_into_db(temp_data_path, chain_name, dataset, ch_client)


        # delete temp files
        delete_files_in_directory(temp_data_path)



def run_cryo_dataset(temp_data_path, dataset, chain_name, rpc_url, from_block, to_block):

    # cryo chunk size
    # chunk_size = 10000
    # if chain_name.contains("eth"):
    #     chunk_size = 1000

    # cryo command
    process_cmd = [
        "cryo", 
        dataset, 
        "--exclude-failed",
        "-r", rpc_url, 
        "-o", temp_data_path,    
        "-b", str(from_block) + ":" + str(to_block),
        # "-c", str(chunk_size),
    ]

    # add timestamp if not contracts or native_transfers (trace datasets)
    if dataset not in ['contracts', 'native_transfers']:
        process_cmd.append("-i")
        process_cmd.append("timestamp")

    # add contract_address if transactions
    if dataset == 'transactions':
        process_cmd.append("-i")
        process_cmd.append("contract_address")
    
    # info
    print("Running cryo command: " + str(process_cmd))

    # run cryo
    subprocess.run(process_cmd)



def delete_files_in_directory(temp_data_path):
   try:
     files = os.listdir(temp_data_path)
     for file in files:
       file_path = os.path.join(temp_data_path, file)
       if os.path.isfile(file_path):
         os.remove(file_path)

   except OSError:
     print("Error occurred while deleting files.")