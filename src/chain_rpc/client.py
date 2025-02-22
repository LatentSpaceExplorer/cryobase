from web3 import Web3


def get_web3_client(url) -> Web3:
    """
    Creates and validates a connection to the web3 client.
    
    Args:
        url: Web3 client URL
    
    Returns:
        A valid Web3 client connection
        
    Raises:
        Exception: If connection fails or test query fails
    """
    w3 = Web3(Web3.HTTPProvider(url))

    try:
        last_block = w3.eth.get_block('latest')
        if last_block.number > 0:
            return w3
        else:
            raise Exception("No blocks found on node")
    except Exception as e:
        raise Exception(f"Web3 connection failed: {str(e)}")