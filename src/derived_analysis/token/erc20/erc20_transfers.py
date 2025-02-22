from src.utils.db import table_exists


def erc20_transfers_analysis(ch_client, chain_name):

    if table_exists(ch_client, chain_name + "_derived", "erc20_transfers") == False:
        print(f"Creating {chain_name}_derived.erc20_transfers table")
            
        ch_client.command(erc20_transfers_table_cmd(chain_name))

        print(f"Created {chain_name}_derived.erc20_transfers table and populated it")

        ch_client.command(erc20_transfers_mv_cmd(chain_name))

        print(f"Created {chain_name}_derived.erc20_transfers_mv materialized view")

        # convert WETH deposits and withdrawals to transfer logs for ethereum mainnet
        if chain_name == "ethereum":

            weth_contract = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2".replace("0x", "")

            ch_client.command(populate_deposits_to_transfers_cmd(chain_name, weth_contract))
            ch_client.command(populate_withdrawals_to_transfers_cmd(chain_name, weth_contract))

            print(f"Added WETH deposits and withdrawals to transfers, {chain_name}_derived.erc20_transfers")

            ch_client.command(deposits_to_transfers_mv_cmd(chain_name, weth_contract))
            ch_client.command(withdrawals_to_transfers_mv_cmd(chain_name, weth_contract))

            print(f"Created materialized view for WETH deposits and withdrawals to transfers, {chain_name}_derived.erc20_transfers_mv")






def erc20_transfers_table_cmd(chain_name):

    return f"""
        CREATE TABLE IF NOT EXISTS {chain_name}_derived.erc20_transfers 
        (
            contract FixedString(20),
            block_number UInt64,
            timestamp DateTime,
            transaction_hash FixedString(32),
            transaction_index UInt64,
            log_index UInt64,
            from FixedString(20),
            to FixedString(20),
            value UInt256
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMM(timestamp) 
        ORDER BY (contract, timestamp, transaction_hash, log_index) -- 
        AS
        SELECT
            address AS contract,
            block_number,
            timestamp,
            transaction_hash,
            transaction_index,
            log_index,
            substring(topic1, -20) AS from,
            substring(topic2, -20) AS to,
            reinterpretAsUInt256(reverse(data)) AS value
        FROM {chain_name}_raw.logs
        WHERE
            topic0 = unhex('ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef')
            AND topic1 IS NOT NULL
            AND topic2 IS NOT NULL
            AND topic3 IS NULL;
        """



def erc20_transfers_mv_cmd(chain_name):

    return f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {chain_name}_derived.erc20_transfers_mv
        TO {chain_name}_derived.erc20_transfers
        AS
        SELECT
            address AS contract,
            block_number,
            timestamp,
            transaction_hash,
            transaction_index,
            log_index,
            substring(topic1, -20) AS from,
            substring(topic2, -20) AS to,
            reinterpretAsUInt256(reverse(data)) AS value
        FROM {chain_name}_raw.logs
        WHERE
            topic0 = unhex('ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef')
            AND topic1 IS NOT NULL
            AND topic2 IS NOT NULL
            AND topic3 IS NULL;
        """



def populate_deposits_to_transfers_cmd(chain_name, contact):

    return f"""
        INSERT INTO {chain_name}_derived.erc20_transfers
        SELECT
            address AS contract,
            block_number,
            timestamp,
            transaction_hash,
            transaction_index,
            log_index,
            UNHEX('0000000000000000000000000000000000000000') AS from,
            substring(topic1, -20) AS to,
            reinterpretAsUInt256(reverse(data)) AS value
        FROM {chain_name}_raw.logs
        WHERE 
            topic0 = unhex('e1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c')
            AND address = unhex('{contact.replace("0x", "")}');
    """

def populate_withdrawals_to_transfers_cmd(chain_name, contact):

    return f"""
        INSERT INTO {chain_name}_derived.erc20_transfers
        SELECT
            address AS contract,
            block_number,
            timestamp,
            transaction_hash,
            transaction_index,
            log_index,
            substring(topic1, -20) AS from,
            UNHEX('0000000000000000000000000000000000000000') AS to,
            reinterpretAsUInt256(reverse(data)) AS value
        FROM {chain_name}_raw.logs
        WHERE 
            topic0 = unhex('7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65')
            AND address = unhex('{contact.replace("0x", "")}');
    """



def deposits_to_transfers_mv_cmd(chain_name, contact):

    return f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {chain_name}_derived.weth_deposits_to_transfers_mv
        TO {chain_name}_derived.erc20_transfers
        AS
        SELECT
            address AS contract,
            block_number,
            timestamp,
            transaction_hash,
            transaction_index,
            log_index,
            UNHEX('0000000000000000000000000000000000000000') AS from,
            substring(topic1, -20) AS to,
            reinterpretAsUInt256(reverse(data)) AS value
        FROM {chain_name}_raw.logs
        WHERE 
            topic0 = unhex('e1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c')
            AND address = unhex('{contact.replace("0x", "")}');
    """

def withdrawals_to_transfers_mv_cmd(chain_name, contact):

    return f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {chain_name}_derived.weth_withdrawals_to_transfers_mv
        TO {chain_name}_derived.erc20_transfers
        AS
        SELECT
            address AS contract,
            block_number,
            timestamp,
            transaction_hash,
            transaction_index,
            log_index,
            substring(topic1, -20) AS from,
            UNHEX('0000000000000000000000000000000000000000') AS to,
            reinterpretAsUInt256(reverse(data)) AS value
        FROM {chain_name}_raw.logs
        WHERE 
            topic0 = unhex('7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65')
            AND address = unhex('{contact.replace("0x", "")}');
    """