from pycoingecko import CoinGeckoAPI
from pprint import pprint
import urllib.request
import pandas as pd
import numpy as np
import logging
import json



logger = logging.getLogger()
def uniswapv2_graphql(target_pool):
    """
    Input: target Uniswap V2 pool address
    Output: dict["token_name"]:asset_units_in_pool

    Returns current pool sizes (pool reserves) for a Unsiwap V2 trading pair using Graphql.

    """
    # The Graph query
    query ="""
    query pair($pool_id: String) {
      pair (id: $pool_id) {
         token0 {
           id
           symbol
           name
         }
         token1 {
           id
           symbol
           name
         }
         reserve0
         reserve1
      }
    }
    """
    # Query the subgraph
    url = "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2"
    req = urllib.request.Request(url)
    req.add_header('Content-Type', 'application/json; charset=utf-8')
    jsondata = {"query": query, "variables": {"pool_id": str(target_pool)}}
    jsondataasbytes = json.dumps(jsondata).encode('utf-8')
    req.add_header('Content-Length', len(jsondataasbytes))
    response = urllib.request.urlopen(req, jsondataasbytes)
    obj = json.load(response)

    res0 = obj["data"]["pair"]["reserve0"]
    res1 = obj["data"]["pair"]["reserve1"]
    name0 = obj["data"]["pair"]["token0"]["symbol"]
    name1 = obj["data"]["pair"]["token1"]["symbol"]
    res_dict = {}
    res_dict[name0] = res0
    res_dict[name1] = res1

    return res_dict

def calc_eth_received(pool_sizes,usd_amounts):
    """
    Returns ETH received from swap "asset -> eth" in Unsiwap V2 asset-eth pool.
    """
    base_asset = "WETH"
    base_asset_volume = float(pool_sizes[base_asset])
    quote_asset = [asset for asset in pool_sizes.keys() if asset != base_asset]
    quote_asset = quote_asset[0]
    quote_asset_volume = float(pool_sizes[quote_asset])

    def get_ethusdc_ex_rate():
            """Gets current ETH/USDC exchange rate from Uniswap V2 usdc/eth pool."""
            eth_usdc_pool = "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"
            pool_sizes = uniswapv2_graphql(eth_usdc_pool)
            base_asset = "USDC"
            base_asset_volume = float(pool_sizes[base_asset])
            quote_asset = [asset for asset in pool_sizes.keys() if asset != base_asset]
            quote_asset = quote_asset[0]
            quote_asset_volume = float(pool_sizes[quote_asset])
            exch_rate = base_asset_volume / quote_asset_volume
            return exch_rate

    exch_rate_asset = quote_asset_volume / base_asset_volume #how much ETHEREUM per unit of asset

    received_eth_dict = {}
    for usd_amount in usd_amounts:
        k = quote_asset_volume * base_asset_volume #invariance k = x * y
        position_asset_volume = usd_amount / get_ethusdc_ex_rate() #how much ETH for USD value of position
        position_asset_volume = position_asset_volume * exch_rate_asset #multiplied by eth/asset exchange rate
        fee = 0.003 * position_asset_volume #liquidity provider fee = 0.3% @ Uniswap V2
        x = quote_asset_volume + position_asset_volume - fee
        y = k / x
        swapper_receives = base_asset_volume - y #the amount to be received
        received_eth_dict[usd_amount] = swapper_receives

    return received_eth_dict

def calc_price_impact(eth_usdc_pool,received_eth_dict):
    """
    Takes in amount of ETH received from the "asset -> eth" swap @calc_eth_received(pool_sizes,usd_amounts,base_asset)
    Returns price impact of "eth -> usdc" swap in Uniswap V2 usdc-eth pool
    """
    pool_sizes = uniswapv2_graphql(eth_usdc_pool)
    base_asset = "USDC"
    base_asset_volume = float(pool_sizes[base_asset])
    quote_asset = [asset for asset in pool_sizes.keys() if asset != base_asset]
    quote_asset = quote_asset[0]
    quote_asset_volume = float(pool_sizes[quote_asset])
    exch_rate = base_asset_volume / quote_asset_volume

    price_impact_dict = {}
    for usd_amount in usd_amounts:
        position_asset_volume = received_eth_dict[usd_amount] #Use the amount of ETH received from the "asset -> eth" swap @calc_eth_received(pool_sizes,usd_amounts,base_asset)
        k = quote_asset_volume * base_asset_volume #invariance k = x * y
        fee = 0.003 * position_asset_volume #liquidity provider fee = 0.3% @ Uniswap V2
        x = quote_asset_volume + position_asset_volume - fee
        y = k / x
        swapper_receives = base_asset_volume - y #the amount to be received
        price_impact = (1 - (swapper_receives / usd_amount)) * (-1)
        price_impact_dict[usd_amount] = price_impact

    return price_impact_dict

def ethusdc_pool_calc_price_impact(pool_sizes,usd_amounts):
    """
    Returns price impact of "eth -> usdc" swap in Uniswap V2 usdc-eth pool
    """
    base_asset = "USDC"
    base_asset_volume = float(pool_sizes[base_asset])
    quote_asset = [asset for asset in pool_sizes.keys() if asset != base_asset]
    quote_asset = quote_asset[0]
    quote_asset_volume = float(pool_sizes[quote_asset])
    exch_rate = base_asset_volume / quote_asset_volume

    price_impact_dict = {}
    for usd_amount in usd_amounts:
        position_asset_volume = usd_amount / exch_rate
        k = quote_asset_volume * base_asset_volume #invariance k = x * y
        fee = 0.003 * position_asset_volume #liquidity provider fee = 0.3% @ Uniswap V2
        x = quote_asset_volume + position_asset_volume - fee
        y = k / x
        swapper_receives = base_asset_volume - y #the amount to be received
        price_impact = (1 - (swapper_receives / usd_amount)) * (-1)
        price_impact_dict[usd_amount] = price_impact

    return price_impact_dict

if __name__ == '__main__':
    """
    Enter your configs here: Uniswap V2 Trading Pairs & USD Position Sizes

    Workflow: Simulates swap "asset->eth" in asset/eth pool, then simulates swaps "eth->usdc" in usdc/eth pool with received ETH from previous swap.

    Cost of liquidation = cost(asset->eth) swap + cost(eth->usdc) swap
    Slippage = (1 - cost_of_liquidation/position_volume)*(-1)
    """
    #WBTC/ETH, UNI/ETH
    target_pools_dict = {}
    target_pools_dict["bitcoin"] = "0xbb2b8038a1640196fbe3e38816f3e67cba72d940"
    target_pools_dict["ethereum"] = "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"
    target_pools_dict["uniswap"] = "0xd3d2e2692501a5c9ca623199d38826e513033a17"
    # USDC/WETH
    eth_usdc_pool = "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"
    usd_amounts = [10000,100000,1000000,10000000,100000000]

    res_dict = {}
    for asset_name, target_pool in target_pools_dict.items():
        pool_sizes = uniswapv2_graphql(target_pool)
        if "ethereum" in asset_name:
            price_impact_dict = ethusdc_pool_calc_price_impact(pool_sizes,usd_amounts)
        else:
            received_eth_dict = calc_eth_received(pool_sizes,usd_amounts)
            price_impact_dict = calc_price_impact(eth_usdc_pool,received_eth_dict)

        res_dict[asset_name] = price_impact_dict

    result_df = pd.DataFrame(res_dict)
    result_df.to_csv('slippage_report_uniswapv2.csv')

    print("all done.")
