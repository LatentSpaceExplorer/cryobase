import subprocess


def insert_into_db(path, chain_name, dataset, ch_client):

    #info
    print("inserting into db, chain:", chain_name, ", dataset:", dataset, ", path:", path)


    # for trace derived datasets first insert into temp table to later join with blocks for timestamp
    trace_derived = dataset in ["native_transfers", "contracts"]
    table_to_insert = dataset + "_staging" if trace_derived else dataset


    select_cmd = get_select_from_dataset(dataset)

    filter = ""
    if dataset == "native_transfers":
        filter = "WHERE value > 0"

    # First part of the command: Execute the SELECT query and output the result
    select_command = [
        "clickhouse-local", "-q",
        select_cmd + " FROM file('"+path+"*.parquet') " + filter
    ]

    # Second part of the command: Insert the output into ClickHouse using clickhouse-client
    insert_command = [
        "clickhouse-client", "--port", "19000", "-q",
        "INSERT INTO "+chain_name+"_raw."+table_to_insert+" FORMAT TSV"
    ]

    # Open the first process
    p1 = subprocess.Popen(select_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # Open the second process, using the output of the first process as its input
    p2 = subprocess.Popen(insert_command, stdin=p1.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # Close the output of the first process so the second can read EOF
    p1.stdout.close()

    # Get the output and errors
    output, error = p2.communicate()

    # Print outputs and errors
    if p2.returncode != 0:
        print("Error:", error)
    else:
        print("Data inserted successfully")


    if trace_derived:

        # join with blocks for timestamp
        ch_client.command("""
            INSERT INTO """ + chain_name + """_raw.""" + dataset + """
            SELECT 
                staging.block_number,
                blocks.timestamp,
                staging.transaction_hash,
                staging.transaction_index,
                staging.transfer_index,
                staging.from_address,
                staging.to_address,
                staging.value
            FROM """ + chain_name + """_raw.""" + table_to_insert + """ AS staging
            JOIN """ + chain_name + """_raw.blocks AS blocks ON staging.block_number = blocks.block_number;
        """)

        
        # drop temp table
        ch_client.command("DROP TABLE IF EXISTS " + chain_name + "_raw." + table_to_insert + " SETTINGS max_table_size_to_drop = 0;")




def get_select_from_dataset(dataset):
   
   match dataset:
      case "blocks": return insert_blocks
      case "transactions": return insert_transactions
      case "logs": return insert_logs
      case "contracts": return insert_contracts
      case "native_transfers": return insert_native_transfers
      case _:
        print("Unknown dataset:", dataset)
        print("exiting...")
        exit()


insert_blocks = "SELECT block_number, toDateTime(timestamp) as timestamp, block_hash, author, gas_used, base_fee_per_gas, extra_data"

insert_transactions = """SELECT block_number, toDateTime(timestamp) as timestamp, transaction_hash, transaction_index, nonce, from_address, to_address, contract_address, toUInt256(value_string) as value, 
input, gas_limit, gas_used, gas_price, transaction_type, max_priority_fee_per_gas, max_fee_per_gas, success, n_input_bytes, n_input_zero_bytes, n_input_nonzero_bytes"""

insert_logs = "SELECT block_number, toDateTime(timestamp) as timestamp, transaction_hash, transaction_index, log_index, address, topic0, topic1, topic2, topic3, data"

insert_contracts = "SELECT block_number, transaction_hash, create_index, contract_address, deployer, factory, init_code, code, init_code_hash, n_init_code_bytes, n_code_bytes, code_hash"

insert_native_transfers = "SELECT block_number, transaction_hash, transaction_index, transfer_index, from_address, to_address, toUInt256(value_string) as value"






