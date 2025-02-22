


def hot_contracts_analysis(ch_client, chain_name, days=7):
    
        chain_db_raw = chain_name + "_raw"
        chain_db_derived = chain_name + "_derived"
        table_name = f"hot_contracts_{days}d"

        ch_client.command(f"""
            CREATE TABLE IF NOT EXISTS {chain_db_derived}.{table_name}
            (
                address String,
                entity Nullable(String),
                label Nullable(String),
                deployment_timestamp Nullable(Timestamp),
                transaction_count UInt64, 
                unique_senders_count UInt64,
                is_erc20 Boolean,
                erc20_name String
            )
            ENGINE = MergeTree
            ORDER BY (transaction_count, unique_senders_count)
            """)

        # Drop all rows from the table to refresh the data
        ch_client.command(f"TRUNCATE TABLE {chain_db_derived}.{table_name}")


        # Insert data into the table
        ch_client.command(f"""
            INSERT INTO {chain_db_derived}.{table_name}
            SELECT
                HEX(tc.address),
                entities.entity_name as entity,
                address_labels.label,
                ct.deployment_timestamp,
                tc.transaction_count,
                tc.unique_senders_count,
                IF(em.contract != UNHEX('0000000000000000000000000000000000000000'), 1, 0) AS is_erc20,
                em.name AS erc20_name
            FROM 
                (
                SELECT
                    contract_address AS address,
                    timestamp AS deployment_timestamp
                FROM 
                    {chain_db_raw}.transactions AS tx
                WHERE
                    contract_address IS NOT NULL AND
                    timestamp >= (NOW() - INTERVAL {days} DAY)
                ) AS ct
            RIGHT JOIN 
                (
                SELECT
                    tx.to_address AS address,
                    COUNT(*) AS transaction_count,
                    COUNT(DISTINCT tx.from_address) AS unique_senders_count
                FROM 
                    {chain_db_raw}.transactions AS tx
                WHERE
                    timestamp >= (NOW() - INTERVAL {days} DAY)
                GROUP BY
                    tx.to_address
                ) AS tc ON ct.address = tc.address
            LEFT JOIN EVM_meta.address_labels ON tc.address = address_labels.address
            LEFT JOIN EVM_meta.entities ON entities.entity_id = address_labels.entity_id
            LEFT JOIN {chain_db_derived}.erc20_meta AS em ON tc.address = em.contract
            ORDER BY
                tc.unique_senders_count DESC
            LIMIT 1000;

        """)

        print(f"Data refreshed for {chain_name} in {chain_db_derived}.{table_name}")