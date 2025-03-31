import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../..")))

from .common_fn import *
import shutil
import requests
import json
import polars as pl


def download_coinmarket_prices_all_crypto():
    url = "https://api.coinmarketcap.com/data-api/v3/cryptocurrency/listing"
    params = {
        "start": 1,
        "limit": 1000,
        "sortBy": "rank",
        "sortType": "desc",
        "cryptoType": "all",
        "tagType": "all",
        "audited": "false",
        "aux": "ath,atl,high24h,low24h,num_market_pairs,cmc_rank,date_added,max_supply,circulating_supply,total_supply,volume_7d,volume_30d,self_reported_circulating_supply,self_reported_market_cap",
    }

    processed_data = []
    start = 1
    while True:
        params["start"] = start
        response = requests.get(url, params=params)

        if response.status_code == 200:
            data = response.json().get("data", {}).get("cryptoCurrencyList", [])
            if not data:
                break  # Dừng khi không còn dữ liệu

            for entry in data:
                base_info = {
                    "id": entry.get("id"),
                    "name": entry.get("name"),
                    "symbol": entry.get("symbol"),
                    "cmcRank": entry.get("cmcRank"),
                    "circulatingSupply": entry.get("circulatingSupply"),
                    "totalSupply": entry.get("totalSupply"),
                    "maxSupply": entry.get("maxSupply"),
                    "ath": entry.get("ath"),
                    "atl": entry.get("atl"),
                    "high24h": entry.get("high24h"),
                    "low24h": entry.get("low24h"),
                    "lastUpdated": entry.get("lastUpdated"),
                }

                if entry.get("quotes"):
                    quote = entry["quotes"][0]  # Extract USD data
                    base_info.update(
                        {
                            "price": quote.get("price"),
                            "volume24h": quote.get("volume24h"),
                            "marketCap": quote.get("marketCap"),
                            "percentChange1h": quote.get("percentChange1h"),
                            "percentChange24h": quote.get("percentChange24h"),
                            "percentChange7d": quote.get("percentChange7d"),
                            "percentChange30d": quote.get("percentChange30d"),
                            "percentChange60d": quote.get("percentChange60d"),
                            "percentChange90d": quote.get("percentChange90d"),
                            "fullyDilluttedMarketCap": quote.get(
                                "fullyDilluttedMarketCap"
                            ),
                            "dominance": quote.get("dominance"),
                            "turnover": quote.get("turnover"),
                            "ytdPriceChangePercentage": quote.get(
                                "ytdPriceChangePercentage"
                            ),
                            "percentChange1y": quote.get("percentChange1y"),
                        }
                    )

                processed_data.append(base_info)

            start += 1000
        else:
            print(f"Failed to fetch data. Status code: {response.status_code}")
            break

    df = pl.DataFrame(processed_data)

    if "id" in df.columns:
        df = df.sort("cmcRank")
    print(df)
    print(df.columns)
    print(df.describe())

    return df


# https://api.coinmarketcap.com/data-api/v3/etf/overview/netflow/chart?category=all&range=30d&convertId=2781
# https://api.coinmarketcap.com/data-api/v3/etf/overview/aum/chart?category=all&range=30d&convertId=2781
# https://api.coinmarketcap.com/data-api/v3/etf/list?category=all&size=50&start=1

# https://api.coinmarketcap.com/nft/v3/nft/chain/total?cryptoSlug=ethereum&period=1
# https://api.coinmarketcap.com/data-api/v3/fear-greed/chart?start=1367193600&end=1741971600&convertId=2781
