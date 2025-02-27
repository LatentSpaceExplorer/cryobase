from typing import Optional

from src.derived_analysis.token.erc20.erc20_transfers import erc20_transfers_analysis
from src.derived_analysis.token.erc20.erc20_balances import erc20_balance_analysis
from src.derived_analysis.token.erc20.erc20_meta import erc20_meta_analysis

from src.derived_analysis.token.erc721.erc721_transfers import erc721_transfers_analysis
from src.derived_analysis.token.erc721.erc721_meta import erc721_meta_analysis

from src.derived_analysis.token.erc1155.erc1155_transfers import erc1155_transfers_analysis

from src.derived_analysis.amm.v2.amm_v2_swaps import amm_v2_swaps_analysis
from src.derived_analysis.amm.v2.amm_v2_pools import amm_v2_pools_analysis

from src.derived_analysis.amm.v3.amm_v3_swaps import amm_v3_swaps_analysis
from src.derived_analysis.amm.v3.amm_v3_pools import amm_v3_pools_analysis
from src.derived_analysis.amm.v3.amm_v3_stats import amm_v3_stats_analysis

from src.derived_analysis.account.active_eoa_summary import active_eoa_summary_analysis
from src.derived_analysis.account.hot_contracts import hot_contracts_analysis



def run_derived_analysis(
    ch_client,
    chain_name: str,
    chain_rpc_url: str,
    analysis_types: Optional[list[str]] = None
):

    print(f"Running derived analysis for {chain_name}")

    # Default to all analyses if none specified
    if analysis_types is None:
        analysis_types = ["token", "amm", "account"]

    # Token Analysis
    if "token" in analysis_types:
        print("Running token analysis...")

        # erc20
        erc20_transfers_analysis(ch_client, chain_name)
        erc20_meta_analysis(ch_client, chain_name, chain_rpc_url)
        erc20_balance_analysis(ch_client, chain_name)

        # erc721
        erc721_transfers_analysis(ch_client, chain_name)
        erc721_meta_analysis(ch_client, chain_name, chain_rpc_url)

        # erc1155
        erc1155_transfers_analysis(ch_client, chain_name) # currently only supporting single transfers


    # AMM Analysis
    if "amm" in analysis_types:
        print("Running AMM analysis...")

        # # v2 amms
        amm_v2_swaps_analysis(ch_client, chain_name)
        amm_v2_pools_analysis(ch_client, chain_name)

        # # v3 amms
        amm_v3_swaps_analysis(ch_client, chain_name)
        amm_v3_pools_analysis(ch_client, chain_name, chain_rpc_url)
        amm_v3_stats_analysis(ch_client, chain_name)

    # Account Analysis
    if "account" in analysis_types:
        print("Running account analysis...")

        active_eoa_summary_analysis(ch_client, chain_name)
        hot_contracts_analysis(ch_client, chain_name)




    print(f"Completed derived analysis for {chain_name}")
