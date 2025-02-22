def create_table_cmd(chain, dataset):
   match dataset:
      case "blocks": return create_blocks_table_cmd(chain)
      case "transactions": return create_transactions_table_cmd(chain)
      case "logs": return create_logs_table_cmd(chain)
      case "contracts": return create_contracts_table_cmd(chain)
      case "native_transfers": return create_native_transfers_table_cmd(chain)
      case _:
        print("Unknown dataset:", dataset)
        print("exiting...")
        exit()


def create_blocks_table_cmd(chain):
    return """
        CREATE TABLE IF NOT EXISTS """ + chain + """_raw.blocks
        (
            block_number UInt32,
            timestamp DateTime,
            block_hash FixedString(32),  
            author FixedString(20),     
            gas_used UInt64,
            base_fee_per_gas UInt64,
            extra_data FixedString(32)
        )
        ENGINE = MergeTree
        ORDER BY (block_number, timestamp, block_hash);
    """


def create_transactions_table_cmd(chain):
    return """
        CREATE TABLE IF NOT EXISTS """ + chain + """_raw.transactions 
        (
            block_number UInt32,
            timestamp DateTime,
            transaction_hash FixedString(32),  
            transaction_index UInt64,
            nonce UInt64,
            from_address FixedString(20),      
            to_address Nullable(FixedString(20)),        
            contract_address Nullable(FixedString(20)),        
            value UInt256,                      
            input String,
            gas_limit UInt64,
            gas_used UInt64,
            gas_price UInt64,
            transaction_type UInt32,
            max_priority_fee_per_gas Nullable(UInt64),
            max_fee_per_gas Nullable(UInt64),
            success Boolean,
            n_input_bytes UInt32,
            n_input_zero_bytes UInt32,
            n_input_nonzero_bytes UInt32
        )
        ENGINE = MergeTree
        ORDER BY (block_number, timestamp, transaction_hash, transaction_index);
        """


def create_logs_table_cmd(chain):
    return """
        CREATE TABLE IF NOT EXISTS """ + chain + """_raw.logs
        (
            block_number UInt32,
            timestamp DateTime,
            transaction_hash FixedString(32),  
            transaction_index UInt32,
            log_index UInt32,
            address FixedString(20),           
            topic0 Nullable(FixedString(32)),  
            topic1 Nullable(FixedString(32)),
            topic2 Nullable(FixedString(32)),
            topic3 Nullable(FixedString(32)),
            data String            
        )
        ENGINE = MergeTree
        ORDER BY (block_number, timestamp, transaction_hash, transaction_index, log_index);
        """



def create_contracts_table_cmd(chain):
    return """
        CREATE TABLE IF NOT EXISTS """ + chain + """_raw.contracts
        (
            block_number UInt32,
            transaction_hash FixedString(32),   
            create_index UInt32,
            contract_address FixedString(20),    
            deployer FixedString(20),           
            factory FixedString(20),            
            init_code String,                    
            code String,                        
            init_code_hash FixedString(32),     
            n_init_code_bytes UInt32,
            n_code_bytes UInt32,
            code_hash FixedString(32)           
        )
        ENGINE = MergeTree
        ORDER BY (block_number, transaction_hash, create_index, contract_address, deployer, factory);
        """



def create_native_transfers_table_cmd(chain):
    return """
        CREATE TABLE IF NOT EXISTS """ + chain + """_raw.native_transfers
        (
            block_number UInt32,
            timestamp DateTime,
            transaction_hash FixedString(32),
            transaction_index UInt32,        
            transfer_index UInt32,                     
            from_address FixedString(20),              
            to_address FixedString(20),               
            value UInt256,                            
        )
        ENGINE = MergeTree
        ORDER BY (block_number, timestamp, transaction_hash, transfer_index);
        """


# temp table for trace derived datasets to join with blocks for timestamp
def create_staging_table_cmd(chain, dataset):
   match dataset:
    #   case "contracts": return create_contracts_table_cmd(chain)
      case "native_transfers": return create_staging_native_transfers_table_cmd(chain)
      case _:
        print("Unknown dataset for staging table:", dataset)
        print("exiting...")
        exit()


def create_staging_native_transfers_table_cmd(chain):
    return """
        CREATE TABLE IF NOT EXISTS """ + chain + """_raw.native_transfers_staging
        (
            block_number UInt32,
            transaction_hash FixedString(32),
            transaction_index UInt32,        
            transfer_index UInt32,                     
            from_address FixedString(20),              
            to_address FixedString(20),               
            value UInt256,                            
        )
        ENGINE = MergeTree
        ORDER BY (block_number, transaction_hash, transfer_index);
    """