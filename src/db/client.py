import clickhouse_connect



def get_db_client_from_config(clickhouse_config: dict) -> clickhouse_connect.driver.Client:
    """
    Creates and validates a connection to the ClickHouse database using configuration settings.
    
    Args:
        config: Dictionary containing database connection parameters
    
    Returns:
        A valid ClickHouse client connection
        
    Raises:
        Exception: If connection fails or test query fails
    """
    try: 
        return get_db_client(
            clickhouse_config["host"], 
            clickhouse_config["port"], 
            clickhouse_config["user"], 
            clickhouse_config["password"]
        )
    except Exception as e:
        print(f"Error connecting to ClickHouse: {e}")
        return


def get_db_client(host: str, port: int, username: str = 'default', password: str = '') -> clickhouse_connect.driver.Client:
    """
    Creates and validates a connection to the ClickHouse database.
    
    Args:
        host: Database host address
        port: Database port number
    
    Returns:
        A valid ClickHouse client connection
        
    Raises:
        Exception: If connection fails or test query fails
    """
    try:
        ch_client = clickhouse_connect.get_client(host=host, port=port, username=username, password=password)
        # Test the connection with a simple query
        ch_client.command("SELECT 1")
        return ch_client
    except Exception as e:
        raise Exception(f"Database connection failed: {str(e)}")