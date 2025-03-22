import streamlit as st
import pandas as pd
from pathlib import Path
import utils

###################
## Streamlit UI
###################

st.title("DeFi Yield Seeker & Optimizer")

# User Inputs
st.header("User Parameters")
address = st.text_input("Enter your wallet address")

st.subheader("Yield Seeker Settings")
min_tvl = st.number_input("Minimum TVL", min_value=0, value=1_000_000, step=100_000)
categories = st.multiselect(
    "Choose verticals", ["liquid_staking", "yield", "money_market", "dex", "perps"], default=["liquid_staking","money_market"]
)

st.subheader("Yield Optimizer Settings")
min_tvl_opti = st.number_input("Minimum TVL for optimization", min_value=0, value=1_000_000, step=100_000)
vertical_opti = st.checkbox("Keep positions in the same vertical", value=True)
chain_opti = st.checkbox("Keep positions on the same chain", value=True)

# ✅ Initialize empty DataFrames to avoid errors
df_top_opportunities = pd.DataFrame()
df_revenue = pd.DataFrame()
matched_pools = pd.DataFrame()
similar_pools = pd.DataFrame()

if st.button("Find Opportunities"):
    # 1 - PUT YOUR TOKENS AT WORK
    token_balance = utils.get_token_balance(address, st.secrets["auth_token"])
    pools = utils.get_pools(min_tvl, categories, category=True)
    df_top_opportunities, df_revenue = utils.analyze_yield_opportunities(token_balance, pools)
    
    # 2 - OPTIMIZE YOUR POSITION
    protocol_balance = utils.get_protocols(address, st.secrets["auth_token"])
    protocol_balance["id"] = protocol_balance["id"].apply(lambda x: x.split("_")[-1] if "_" in x else x)
    protocol_balance_adj = protocol_balance.merge(utils.mapping_protocol, left_on="id", right_on="debank_id", how="left")
    protocol_balance_adj = protocol_balance_adj.merge(utils.mapping_chain, left_on="chain", right_on="debank_chain_id", how="left")
    protocol_balance_adj = protocol_balance_adj.merge(utils.mapping_protocol_category, left_on="defillama_id", right_on="project", how="left")
    
    pools_2 = utils.get_pools(min_tvl_opti, categories)

    # Check how much APY your pools earn
    matched_pools = utils.match_pools(protocol_balance_adj, pools_2)
    if not matched_pools.empty:
        matched_pools = matched_pools.merge(utils.mapping_protocol, left_on="protocol_id", right_on="debank_id", how="left")

    # Find alternative pools
    similar_pools = utils.find_similar_pools(protocol_balance_adj, pools_2, vertical=vertical_opti, chain=chain_opti, min_tvl=min_tvl_opti)
    if not similar_pools.empty:
        similar_pools = similar_pools.merge(utils.mapping_protocol, left_on="pool_project", right_on="defillama_id", how="left")

# ✅ Yield Seeker Section
st.header("Yield Seeker Results")

if not df_top_opportunities.empty:
    st.subheader("Top Yield Opportunities")
    df_display = df_top_opportunities[["chain", "project", "symbol", "tvlUsd", "apy", "apyMean30d", "ilRisk", "underlying", "url"]]
    df_display = df_display.rename(columns={"tvlUsd": "TVL", "apy": "APY","apyMean30d": "30d APY"})
    st.dataframe(df_display)  # Display as a table
else:
    st.write("No opportunities found.")

# ✅ Keep Potential Earnings as a list
if not df_revenue.empty:
    st.subheader("Potential Earnings")
    for _, row in df_revenue.iterrows():
        st.markdown(
            f"If you made **{row['underlying']}** work at its full potential, "
            f"you'd earn **${row['potential_revenue']:.2f}** on a yearly basis."
        )

# ✅ Yield Optimizer Section
st.header("Yield Optimizer Results")

if not matched_pools.empty:
    st.subheader("Current Yield Performance")
    df_matched_display = matched_pools[["pool_project", "protocol_asset", "pool_apy","pool_apy_30d", "url"]]
    df_matched_display = df_matched_display.rename(columns={"pool_project": "Project", "protocol_asset": "Asset", "pool_apy": "APY", "pool_apy_30d": "30d APY"})
    st.dataframe(df_matched_display)  # Display as a table
else:
    st.write("No matched pools found.")

if not similar_pools.empty:
    st.subheader("Alternative Yield Opportunities")
    df_similar_display = similar_pools[["protocol_id", "pool_project", "pool_chain", "pool_symbol", "pool_apy","pool_apy_30d", "url"]]
    df_similar_display = df_similar_display.rename(columns={"pool_chain": "Chain", "pool_symbol": "Symbol", "pool_apy": "APY", "pool_apy_30d": "30d APY"})
    st.dataframe(df_similar_display)  # Display as a table
else:
    st.write("No alternative pools found.")
