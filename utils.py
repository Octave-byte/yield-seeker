
import requests
import pandas as pd
from pathlib import Path


###################
## READING DATA
###################

DATA_FILENAME_1 = Path(__file__).parent/'data/category_protocol_mapping.csv'
mapping_protocol_category = pd.read_csv(DATA_FILENAME_1)

DATA_FILENAME_2 = Path(__file__).parent/'data/chain_mapping.csv'
mapping_chain = pd.read_csv(DATA_FILENAME_2)

DATA_FILENAME_3 = Path(__file__).parent/'data/protocol_mapping.csv'
mapping_protocol = pd.read_csv(DATA_FILENAME_3)


def determine_underlying(row):
    if row["stablecoin"]:
        return "stable"
    elif any(token in row["symbol"] for token in ["ETH", "BTC", "AVAX", "MATIC", "SOL"]) and row["exposure"] == "single":
        return next(token for token in ["ETH", "BTC", "AVAX", "MATIC", "SOL"] if token in row["symbol"])
    elif any(token in row["symbol"] for token in ["ETH", "BTC", "AVAX", "MATIC", "SOL"]) and row["exposure"] == "multi" and  row["ilRisk"] == 'no':
        return next(token for token in ["ETH", "BTC", "AVAX", "MATIC", "SOL"] if token in row["symbol"])
    elif any(token in row["symbol"] for token in ["ETH", "BTC", "AVAX", "MATIC", "SOL"]) and row["exposure"] == "multi" and row["ilRisk"] == 'yes':
        return next(f"{token}-IL" for token in ["ETH", "BTC", "AVAX", "MATIC", "SOL"] if token in row["symbol"])
    else:
        return "other"

def determine_underlying_balance(row):
    stablecoins = ["USDC", "DAI", "USDT", "USDC.e", "DAI.e", "USDT.e"]
    crypto_assets = ["ETH", "BTC", "AVAX", "MATIC", "SOL", "WETH", "WBTC", "CBBTC","BNB","WBNB"]
    
    if any(stable in row["symbol"] for stable in stablecoins):
        return "stable"
    elif any(asset in row["symbol"] for asset in crypto_assets):
        return next(asset for asset in crypto_assets if asset in row["symbol"])
    else:
        return "other"

def get_pools(min_tvl, categories_to_filter, category = False):
    
    # getting defillama data
    url = 'https://yields.llama.fi/pools'
    response = requests.get(url)
    data = response.json()
    df = pd.json_normalize(data, 'data')
    
    
    # filtering & formatting data
    df = df[df["tvlUsd"] > min_tvl]

    df = df.merge(mapping_protocol_category, on="project", how="left")
    
    if category == True:
        df = df[df['category'].isin(categories_to_filter)]
    
    
    df = df[['chain','project','category','symbol','tvlUsd','apy','underlyingTokens','apyMean30d','exposure', 'stablecoin','outlier','ilRisk']]
    
    df["underlying"] = df.apply(determine_underlying, axis=1)
    return df   


## ?? units 
def get_protocols(address, API_KEY): 
      
    url = f"https://pro-openapi.debank.com/v1/user/all_complex_protocol_list?id={address}&chain_ids=blast,scrl,era,metis,linea,base,eth,op,arb,xdai,matic,bsc,avax,sonic,bera"
    headers = {
    'accept': 'application/json',
    'AccessKey': API_KEY  # Replace 'API_KEY' with your actual access key
    }
    response = requests.get(url, headers=headers)
    data = response.json()
    
    df = pd.DataFrame(data)
    df['address'] = address
    #df = df[df['asset_usd_value'] > 10]
    
    
    df_expanded = df.explode("portfolio_item_list")

    # Convert the dictionary inside 'portfolio_item_list' into separate columns
    df_expanded = pd.concat([
        df_expanded.drop(columns=["portfolio_item_list"]),
        df_expanded["portfolio_item_list"].apply(pd.Series)
    ], axis=1)
    
    def extract_symbols_from_detail(detail):
        if isinstance(detail, dict) and "supply_token_list" in detail:
            symbols = list(set(token["symbol"] for token in detail["supply_token_list"]))
            return symbols[:3]  # Keep only two symbols max
        return []

    # Apply function to extract symbols
    df_expanded["symbols"] = df_expanded["detail"].apply(extract_symbols_from_detail)


    # Split into supply_token_1 and supply_token_2
    df_expanded["supply_token_1"] = df_expanded["symbols"].apply(lambda x: x[0] if len(x) > 0 else None)
    df_expanded["supply_token_2"] = df_expanded["symbols"].apply(lambda x: x[1] if len(x) > 1 else None)
    df_expanded["supply_token_3"] = df_expanded["symbols"].apply(lambda x: x[2] if len(x) > 2 else None)


    df_expanded["asset_usd_value"] = df_expanded["stats"].apply(lambda x: x.get("asset_usd_value") if isinstance(x, dict) else None)

    df_expanded = df_expanded[["id", "chain", "name", "site_url", "asset_usd_value", "supply_token_1", "supply_token_2", "supply_token_3"]]
    
    df_expanded = df_expanded.loc[:, ~df_expanded.columns.duplicated()]
    
    df_expanded = df_expanded[df_expanded['asset_usd_value'] > 50]
    
    return df_expanded



##18 units
def get_token_balance(address, API_KEY):
    
    url = f"https://pro-openapi.debank.com/v1/user/all_token_list?id={address}"
    headers = {
    'accept': 'application/json',
    'AccessKey': API_KEY
    }
    response = requests.get(url, headers=headers)
    data = response.json()
    
    df = pd.DataFrame(data)
    df['address'] = address
    symbols_to_keep = ["WETH", "ETH", "WBTC", "CBBTC", "USDC", "USDC.e", "USDT","DAI", "USDbC","AVAX", "WAVAX", "BNB", "WBNB" ]
    df = df[df['symbol'].isin(symbols_to_keep)]
    
    df = df[df['is_verified'] == True]
    df = df[['chain','name','symbol','price', 'amount']]
    df['value'] = df['price'] * df['amount']
    
    df["underlying"] = df.apply(determine_underlying_balance, axis=1)
    
    return df

## 30 units
def get_balance(address, API_KEY): 
    url = f"https://pro-openapi.debank.com/v1/user/total_balance?id={address}"
    headers = {
    'accept': 'application/json',
    'AccessKey': API_KEY
    }
    response = requests.get(url, headers=headers)
    data = response.json()
    
    df = pd.DataFrame(data)
    
    return df


def analyze_yield_opportunities(df_token_balance, df_pools):
    """
    This function extracts the top 10 yield opportunities per underlying from `df_pools`
    for assets present in `df_token_balance` where the total value > 100.
    It also computes potential revenue for idle assets based on the highest APY.
    
    Parameters:
    df_token_balance (DataFrame): Contains token balances with 'underlying' and 'value' columns.
    df_pools (DataFrame): Contains yield opportunities with 'underlying' and 'apyMean30d' columns.
    
    Returns:
    Tuple of DataFrames:
        - Top 10 yield opportunities for each underlying.
        - Potential revenue computation based on idle assets.
    """
    
    # Step 1: Aggregate token balance by underlying to filter out assets with a total balance > 100
    df_token_balance_grouped = df_token_balance.groupby("underlying")["value"].sum().reset_index()
    df_token_balance_grouped = df_token_balance_grouped[df_token_balance_grouped["value"] > 100]

    # Step 2: Ensure we only analyze underlyings present in both DataFrames
    common_underlying = set(df_token_balance_grouped["underlying"]) & set(df_pools["underlying"])
    
    # Filter both DataFrames to only include relevant underlyings
    df_token_balance_grouped = df_token_balance_grouped[df_token_balance_grouped["underlying"].isin(common_underlying)]
    df_pools_filtered = df_pools[df_pools["underlying"].isin(common_underlying)]

    # Step 3: Extract the top 10 opportunities for each underlying based on APY
    df_top_opportunities = df_pools_filtered.sort_values(["underlying", "apyMean30d"], ascending=[True, False]) \
                                            .groupby("underlying") \
                                            .head(10)
    df_top_opportunities = df_top_opportunities.merge(mapping_protocol, left_on="project", right_on ="defillama_id" , how="left")

    # Step 4: Compute potential revenue for each underlying
    df_revenue = df_token_balance_grouped.merge(
        df_pools_filtered.groupby("underlying")["apyMean30d"].max().reset_index(),
        on="underlying"
    )
    df_revenue["potential_revenue"] = df_revenue["apyMean30d"] * df_revenue["value"] / 100

    return df_top_opportunities, df_revenue

#balance = get_balance(address, API_KEY)
#balance = balance.join(pd.json_normalize(balance["chain_list"])).drop(columns=["chain_list"])
#protocols = get_protocols(address, API_KEY)


def match_pools(protocol_balance_df, pools_df):
    matches = []
    for _, protocol in protocol_balance_df.iterrows():
        # Extract relevant protocol details
        protocol_chain = protocol["defillama_chain_name"]
        protocol_project = protocol["id"]
        protocol_assets = {protocol["supply_token_1"], protocol["supply_token_2"], protocol["supply_token_3"]}
        
        # Remove empty strings (NaN handling)
        protocol_assets.discard("")
        
        # Filter pools based on chain and project
        filtered_pools = pools_df[
            (pools_df["chain"] == protocol_chain) & (pools_df["project"] == protocol_project)
        ]
        
        # Check if the underlying asset in pools matches any supply tokens from protocol
        for _, pool in filtered_pools.iterrows():
            if pool["underlying"] in protocol_assets:
                matches.append({
                    "protocol_id": protocol["id"],
                    "protocol_chain": protocol_chain,
                    "protocol_asset": pool["underlying"],
                    "pool_project": pool["project"],
                    "pool_chain": pool["chain"],
                    "pool_symbol": pool["symbol"],
                    "pool_tvl": pool["tvlUsd"],
                    "pool_apy": pool["apy"],
                    "pool_apy_30d": pool["apyMean30d"]
                })

    # Convert matches to DataFrame
    matches_df = pd.DataFrame(matches)

    return matches_df


def find_similar_pools(protocol_balance_df, pools_df, vertical=False, chain=False, min_tvl=0):
    suggestions = []

    for _, protocol in protocol_balance_df.iterrows():
        # Extract protocol details
        protocol_chain = protocol["defillama_chain_name"]
        protocol_vertical = protocol.get("category", None)  # Assuming category is missing in protocol_balance_df
        protocol_assets = {protocol["supply_token_1"], protocol["supply_token_2"], protocol["supply_token_3"]}
        
        # Remove empty strings (NaN handling)
        protocol_assets.discard("")

        # Filter pools by TVL
        filtered_pools = pools_df[pools_df["tvlUsd"] >= min_tvl]

        # Apply matching rules based on chain and vertical flags
        if vertical and chain:
            filtered_pools = filtered_pools[
                (filtered_pools["chain"] == protocol_chain) &
                (filtered_pools["category"] == protocol_vertical) &
                (filtered_pools["underlying"].isin(protocol_assets))
            ]
        elif chain:
            filtered_pools = filtered_pools[
                ((filtered_pools["chain"] == protocol_chain)) &
                (filtered_pools["underlying"].isin(protocol_assets))
            ]
        elif vertical:
            filtered_pools = filtered_pools[
                ((filtered_pools["category"] == protocol_vertical)) &
                (filtered_pools["underlying"].isin(protocol_assets))
            ]
        else:
            filtered_pools = filtered_pools[
                (filtered_pools["underlying"].isin(protocol_assets))
            ]

        # Rank by APY and get top 5 suggestions per asset
        for asset in protocol_assets:
            asset_pools = filtered_pools[filtered_pools["underlying"] == asset].sort_values(by="apy", ascending=False)
            top_pools = asset_pools.head(5)

            for _, pool in top_pools.iterrows():
                suggestions.append({
                    "protocol_id": protocol["id"],
                    "protocol_chain": protocol_chain,
                    "protocol_asset": asset,
                    "pool_project": pool["project"],
                    "pool_chain": pool["chain"],
                    "pool_symbol": pool["symbol"],
                    "pool_tvl": pool["tvlUsd"],
                    "pool_apy": pool["apy"],
                    "pool_category": pool["category"],
                    "pool_apy_30d": pool["apyMean30d"]
                })

    # Convert matches to DataFrame
    suggestions_df = pd.DataFrame(suggestions)
    return suggestions_df