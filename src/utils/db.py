def table_exists(ch_client, db, table) -> bool:
    return len(ch_client.query(f"SHOW TABLES FROM {db} LIKE '{table}'").result_set) > 0