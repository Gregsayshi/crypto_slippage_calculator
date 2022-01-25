import pandas as pd
import numpy as np
import requests
import json
import io

def get_market_data(exch,pair):
    #data source: https://github.com/crypto-chassis/cryptochassis-data-api-docs
    response = requests.get("https://api.cryptochassis.com/v1/market-depth/"+exch+"/"+pair) #initial request
    content = response.json()
    market_data_link = content["urls"][0]["url"] #get link to market data CSV from intial request
    market_data_compressed = requests.get(market_data_link).content #get compressed market data from AWS
    market_data = pd.read_csv(io.BytesIO(market_data_compressed),sep=",", compression="gzip", index_col=0, lineterminator='\n') #decompress into a dataframe -> bid/ask + depth PER SECOND for 24H
    market_data["bid_price"],market_data["bid_size"] = zip(*market_data["bid_price_bid_size"].str.split("_").tolist()) #split bid price bid size into two sep columns
    market_data["bid_price"] = pd.to_numeric(market_data["bid_price"])
    market_data["bid_size"] = pd.to_numeric(market_data["bid_size"])
    market_data["ask_price"],market_data["ask_size"] = zip(*market_data["ask_price_ask_size"].str.split("_").tolist()) #split ask price ask size into two sep columns
    market_data["ask_price"] = pd.to_numeric(market_data["ask_price"])
    market_data["ask_size"] = pd.to_numeric(market_data["ask_size"])
    market_data = market_data.reset_index().drop(["bid_price_bid_size","ask_price_ask_size"],axis=1) #remove not needed columns
    market_data['time_seconds'] = pd.to_datetime(market_data['time_seconds'], unit='s')
    market_data["cum_bid_size"] = market_data["bid_size"].cumsum() #create column with cumulitative bid volumes
    return market_data

def bids_fill_calc(market_data,usd_amount):
    asset_price_usd = market_data["bid_price"][0] #we start selling at 00:00
    volume = usd_amount / asset_price_usd
    filled_time = market_data.loc[market_data.cum_bid_size > volume].min() #find the datetime / timestamp when the order will be filled
    return filled_time,volume

def slip_calc(filled_time,market_data):
    initial_selling_price = market_data["bid_price"][0] #we start selling at 00:00
    market_data = market_data.set_index('time_seconds')
    truncated_data = market_data.truncate(after=filled_time.time_seconds) #cut market data df after the sell order is filled
    actual_selling_price = truncated_data["bid_price"].mean() #get mean sell price for filled orders
    sell_cost = actual_selling_price - initial_selling_price
    slip = sell_cost / initial_selling_price
    return sell_cost,slip

def results_generator(exch_list,pair_list,usd_amounts):
    #configure result data frame
    res_df = pd.DataFrame(columns=["exchange","pair","selling_start_date","selling_end_date","time_until_filled_largest_order"])
    col_list = []
    for usd_amount in usd_amounts:
        sc_name = "sell_cost_"+str(usd_amount)+"usd"
        sl_name = "slippage_"+str(usd_amount)+"usd"
        col_list.append(sc_name)
        col_list.append(sl_name)
    res_df = pd.concat([res_df,pd.DataFrame(columns=col_list)])

    #get data
    for exch in exch_list:
        for pair in pair_list:
            res_dict = {}
            res_dict["exchange"] = exch
            res_dict["pair"] = pair
            try:
                market_data = get_market_data(exch,pair)
                res_dict["selling_start_date"] = market_data["time_seconds"][0]
                for usd_amount in usd_amounts:
                    filled_time,volume = bids_fill_calc(market_data,usd_amount)
                    sell_cost, slip = slip_calc(filled_time,market_data)
                    res_dict["selling_end_date"] = filled_time.time_seconds
                    res_dict["time_until_filled_largest_order"] = filled_time.time_seconds - res_dict["selling_start_date"]
                    res_dict["sell_cost_"+str(usd_amount)+"usd"] = sell_cost * volume #sell cost = average slippage per unit * volume
                    res_dict["slippage_"+str(usd_amount)+"usd"] = slip
            except Exception as e: #if exception -> set field as NaN
                for usd_amount in usd_amounts:
                    res_dict["sell_cost_"+str(usd_amount)+"usd"] = np.nan
                    res_dict["slippage_"+str(usd_amount)+"usd"] = np.nan

            res_df = res_df.append(res_dict, ignore_index=True)

    return res_df


if __name__ == '__main__':
    ############################################################################################
    #########Enter your configs here!###########################################################
    ############################################################################################
    exch_list = ["coinbase","kraken"]
    pair_list = ["btc-usd","eth-usd","ltc-usd","uni-usd"]
    usd_amounts = [10000,100000,1000000,10000000,100000000]
    ############################################################################################
    ############################################################################################
    ############################################################################################
    #supported exchanges: Allowed values: coinbase, gemini, kraken, bitstamp, bitfinex, bitmex,
    #binance, binance-us, binance-usds-futures, binance-coin-futures, huobi, huobi-usdt-swap,
    #huobi-coin-swap, okex, kucoin, ftx, ftx-us, deribit.
    #source: https://github.com/crypto-chassis/cryptochassis-data-api-docs

    result_df = results_generator(exch_list,pair_list,usd_amounts)
    result_df.to_csv('slippage_report_cex.csv')

    print("all done.")
