import pandas as pd
import numpy as np
import math

# INITIALIZATION
# 2012 excluded since invalid data
total_sample = ("FQ12013", "FQ22013", "FQ32013", "FQ42013",
                "FQ12014", "FQ22014", "FQ32014", "FQ42014",
                "FQ12015", "FQ22015", "FQ32015", "FQ42015",
                "FQ12016", "FQ22016", "FQ32016", "FQ42016",
                "FQ12017", "FQ22017", "FQ32017", "FQ42017",
                "FQ12018", "FQ22018", "FQ32018", "FQ42018",
                "FQ12019", "FQ22019", "FQ32019", "FQ42019",
                "FQ12020", "FQ22020", "FQ32020", "FQ42020",
                "FQ12021", "FQ22021", "FQ32021")

in_sample = []  # historic data used to optimize
live_quarter = ""  # live trading quarter

# LOAD DATA SET
beta = pd.read_excel('ALL_BETA_READY.xlsx', index_col=0)  # set stock code as index
upside = pd.read_excel('AF_UPSIDE_READY.xlsx', index_col=0)  # set stock code as index
rtr50 = pd.read_excel('RTR50.xlsx', index_col=0)
rtr40 = pd.read_excel('RTR40.xlsx', index_col=0)
rtr30 = pd.read_excel('RTR30.xlsx', index_col=0)
# rtr20 = pd.read_excel('RTR20.xlsx', index_col=0)
# rtr10 = pd.read_excel('RTR10.xlsx', index_col=0)
# rtr5 = pd.read_excel('RTR5.xlsx', index_col=0)
rtr_array = (rtr30, rtr40, rtr50)  # based on domain knowledge we excl. rtr5 and rtr10
rtr_array_str = ("rtr30", "rtr40", "rtr50")
uebit = pd.read_excel('AF_UEBIT_READY.xlsx', index_col=1)  # set company name as index

# WEIGHTS
weight_long_uebit = (0.2, 0.25, 0.3, 0.35, 0.4, 0.45)
weight_short_uebit = (0.2, 0.25, 0.3, 0.35, 0.4, 0.45)
weight_upside = (0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8)
combination_array = []

for w_l_uebit in weight_long_uebit:
    for w_s_uebit in weight_short_uebit:
        for w_upside in weight_upside:
            for i in range(len(rtr_array)):
                combination_array.append([w_l_uebit, w_s_uebit, w_upside, rtr_array[i], rtr_array_str[i]])

def createDataFrame(quarter, w_leg, w_upside, true_if_long):

    uebit_leg_df = pd.DataFrame()
    uebit_leg_df_tickers = []
    uebit_leg_df_uebit = []

    uebit_sorted = uebit.sort_values(by=quarter, ascending= not true_if_long)

    for i in range(len(uebit_sorted[quarter])):
        if uebit_sorted[quarter][i] > 0 or uebit_sorted[quarter][i] < 0:
            uebit_leg_df_tickers.append(uebit_sorted["Ticker"][i])
            uebit_leg_df_uebit.append(uebit_sorted[quarter][i])

    cut_off_uebit = math.floor(w_leg * len(uebit_leg_df_tickers))

    uebit_leg_df_tickers_cut_off = []
    uebit_leg_df_uebit_cut_off = []
    uebit_leg_df_beta_cut_off = []
    uebit_leg_df_upside_cut_off = []
    for i in range(cut_off_uebit):
        uebit_leg_df_tickers_cut_off.append(uebit_sorted["Ticker"][i])
        uebit_leg_df_uebit_cut_off.append(uebit_sorted[quarter][i])

    for ticker in uebit_leg_df_tickers_cut_off:
        uebit_leg_df_beta_cut_off.append(beta[quarter][ticker])
        uebit_leg_df_upside_cut_off.append(upside[quarter][ticker])
    uebit_leg_df["Ticker"] = uebit_leg_df_tickers_cut_off
    uebit_leg_df["uebit"] = uebit_leg_df_uebit_cut_off
    uebit_leg_df["upside"] = uebit_leg_df_upside_cut_off
    uebit_leg_df["beta"] = uebit_leg_df_beta_cut_off
    uebit_leg_df.set_index("Ticker")

    # END OF UEBIT DF
    # START OF UEBIT UPSIDE DF

    uebit_upside_leg_df = pd.DataFrame()
    uebit_upside_leg_df_tickers = []
    uebit_upside_leg_df_upside = []

    uebit_upside_leg_df_sorted = uebit_leg_df.sort_values(by="upside", ascending=true_if_long)
    uebit_upside_leg_df_sorted.reset_index(drop=True, inplace=True)

    # GET RID OF NAN
    for i in range(len(uebit_upside_leg_df_sorted["Ticker"])):
        if uebit_upside_leg_df_sorted["upside"][i] > 0 or uebit_upside_leg_df_sorted["upside"][i] < 0:
            uebit_upside_leg_df_tickers.append(uebit_upside_leg_df_sorted["Ticker"][i])
            uebit_upside_leg_df_upside.append(uebit_upside_leg_df_sorted["upside"][i])

    # IDENTIFY CUTOFF POINT
    cut_off_upside = math.floor(w_upside * len(uebit_upside_leg_df_tickers))

    uebit_upside_leg_df_tickers_cut_off = []
    uebit_upside_leg_df_uebit_cut_off = []
    uebit_upside_leg_df_upside_cut_off = []
    uebit_upside_leg_df_beta_cut_off = []
    # IF NON NAN and IN RANGE OF CUT_OFF
    for i in range(cut_off_upside):
        if uebit_upside_leg_df_sorted["upside"][i] > 0 or uebit_upside_leg_df_sorted["upside"][i] < 0:
            uebit_upside_leg_df_tickers_cut_off.append(uebit_upside_leg_df_sorted["Ticker"][i])
            uebit_upside_leg_df_uebit_cut_off.append(uebit_upside_leg_df_sorted["uebit"][i])
            uebit_upside_leg_df_upside_cut_off.append(uebit_upside_leg_df_sorted["upside"][i])
            if uebit_upside_leg_df_sorted["beta"][i] == "NM":
                uebit_upside_leg_df_beta_cut_off.append(1)
            else:
                uebit_upside_leg_df_beta_cut_off.append(uebit_upside_leg_df_sorted["beta"][i])

    uebit_upside_leg_df["Ticker"] = uebit_upside_leg_df_tickers_cut_off
    uebit_upside_leg_df["uebit"] = uebit_upside_leg_df_uebit_cut_off
    uebit_upside_leg_df["upside"] = uebit_upside_leg_df_upside_cut_off
    uebit_upside_leg_df["beta"] = uebit_upside_leg_df_beta_cut_off

    return uebit_upside_leg_df

def combination(in_sample, w_l_uebit, w_s_uebit, w_upside, rtr, rtr_str, before_combination):

    quarterly_returns = []
    lever_ratio_array = []

    if before_combination == []:
      w_l_uebit = w_l_uebit
      w_s_uebit = w_s_uebit
      w_upside = w_upside
      rtr = rtr
      rtr_str = rtr_str
    else:
      w_l_uebit = w_l_uebit * 0.2 + before_combination[0] * 0.8
      w_s_uebit = w_s_uebit * 0.2 + before_combination[1] * 0.8
      w_upside = w_upside * 0.2 + before_combination[2] * 0.8
      rtr = rtr
      rtr_str = rtr_str

    for quarter in in_sample[-1:]:
        long_leg_df = createDataFrame(quarter=quarter, w_leg=w_l_uebit, w_upside=w_upside, true_if_long=True)
        short_leg_df = createDataFrame(quarter=quarter, w_leg=w_s_uebit, w_upside=w_upside, true_if_long=False)

        long_return = []
        for stock_code in long_leg_df["Ticker"]:
            if rtr[quarter][stock_code] != "NM":
                long_return.append(rtr[quarter][stock_code])

        if len(long_return) > 1:
            long_mean = np.mean(long_return)
        elif len(long_return) == 1:
            long_mean = long_return[0]
        else:
            long_mean = 0

        if len(long_leg_df["beta"]) > 1:
            long_beta_mean = np.mean(long_leg_df["beta"])
        elif len(long_leg_df["beta"]) == 1:
            long_beta_mean = long_leg_df["beta"][0]
        else:
            long_beta_mean = 1

        short_return = []
        for stock_code in short_leg_df["Ticker"]:
            if rtr[quarter][stock_code] != "NM":
                short_return.append(rtr[quarter][stock_code])

        if len(short_return) > 1:
            short_mean = np.mean(short_return)
        elif len(short_return) == 1:
            short_mean = short_return[0]
        else:
            short_mean = 0

        if len(short_leg_df["beta"]) > 1:
            short_beta_mean = np.mean(short_leg_df["beta"])
        elif len(long_leg_df["beta"]) == 1:
            short_beta_mean = short_leg_df["beta"][0]
        else:
            short_beta_mean = 1

        short_adj_mean = (long_beta_mean/short_beta_mean) * short_mean

        spread = long_mean - short_adj_mean
        quarterly_returns.append(spread + 1)

        lever_ratio = 1 + (long_beta_mean/short_beta_mean)
        lever_ratio_array.append(lever_ratio)

    if len(quarterly_returns) > 1:
        geometric_mean = np.prod(quarterly_returns) ** (1 / len(quarterly_returns))
    elif len(quarterly_returns) == 1:
        geometric_mean = quarterly_returns[0]
    else:
        geometric_mean = 0

    lever_ratio_mean = np.mean(lever_ratio_array)

    return geometric_mean, w_l_uebit, w_s_uebit, w_upside, rtr_str, lever_ratio_mean

def liveTrade(quarter, w_l_uebit, w_s_uebit, w_upside, rtr):

    long_leg_df = createDataFrame(quarter=quarter, w_leg=w_l_uebit, w_upside=w_upside, true_if_long=True)
    short_leg_df = createDataFrame(quarter=quarter, w_leg=w_s_uebit, w_upside=w_upside, true_if_long=False)

    long_return = []
    for stock_code in long_leg_df["Ticker"]:
        if rtr[quarter][stock_code] != "NM":
            long_return.append(rtr[quarter][stock_code])

    if len(long_return) > 1:
        long_mean = np.mean(long_return)
    elif len(long_return) == 1:
        long_mean = long_return[0]
    else:
        long_mean = 0

    if len(long_leg_df["beta"]) > 1:
        long_beta_mean = np.mean(long_leg_df["beta"])
    elif len(long_leg_df["beta"]) == 1:
        long_beta_mean = long_leg_df["beta"][0]
    else:
        long_beta_mean = 1

    short_return = []
    for stock_code in short_leg_df["Ticker"]:
        if rtr[quarter][stock_code] != "NM":
            short_return.append(rtr[quarter][stock_code])

    if len(short_return) > 1:
        short_mean = np.mean(short_return)
    elif len(short_return) == 1:
        short_mean = short_return[0]
    else:
        short_mean = 0

    if len(short_leg_df["beta"]) > 1:
        short_beta_mean = np.mean(short_leg_df["beta"])
    elif len(long_leg_df["beta"]) == 1:
        short_beta_mean = short_leg_df["beta"][0]
    else:
        short_beta_mean = 1

    short_adj_mean = (long_beta_mean / short_beta_mean) * short_mean

    spread = long_mean - short_adj_mean
    lever_ratio = 1 + (long_beta_mean / short_beta_mean)

    return long_mean, short_adj_mean, lever_ratio, spread

# MAIN
export_quarters = [] # from historic
export_long_leg = []
export_short_leg = []
export_lever_ratio = []
export_mean_lever_ratio_historic = [] # from historic
export_returns = []
export_long_percentile = [] # from historic
export_short_percentile = [] # from historic
export_upside_percentile = [] # from historic
export_expected_geo_return = [] # from historic
export_holding_period = [] # from historic

for i in range(4, len(total_sample)): # from 4 historic periods
    for j in range(0, i):
        in_sample.append(total_sample[j])
    live_quarter = total_sample[i]
    export_quarters.append(live_quarter)

    # HISTORIC
    best_geometric_mean = 0
    best_combination = []
    before_combination = []
    for combi in combination_array:
        historic_result = combination(in_sample=in_sample, w_l_uebit=combi[0], w_s_uebit=combi[1], w_upside=combi[2], rtr=combi[3], rtr_str=combi[4], before_combination=before_combination)
        print(historic_result)
        if historic_result[0] > best_geometric_mean:
            best_geometric_mean = historic_result[0]
            best_combination = [historic_result[1], historic_result[2], historic_result[3], historic_result[4], historic_result[5]]
            # ^ w_l_uebit, w_s_uebit, w_upside, rtr_str, lever_ratio_mean
            print(f"{live_quarter}'s BEST: {best_geometric_mean}")
            print(f"{live_quarter}'s BEST: {best_combination}")

    export_expected_geo_return.append(best_geometric_mean)
    export_long_percentile.append(best_combination[0])
    export_short_percentile.append(best_combination[1])
    export_upside_percentile.append(best_combination[2])
    export_holding_period.append(best_combination[3])
    export_mean_lever_ratio_historic.append(best_combination[4])

    before_combination = best_combination

    # lIVE PAPER TRADING

    rtr_dict = {"rtr30": rtr30,
                "rtr40": rtr40,
                "rtr50": rtr30}

    live_result = liveTrade(quarter=live_quarter, w_l_uebit=best_combination[0], w_s_uebit=best_combination[1], w_upside=best_combination[2], rtr=rtr_dict[best_combination[3]])
    # ^ long_mean, short_adj_mean, lever_ratio, spread
    export_long_leg.append(live_result[0])
    export_short_leg.append(live_result[1])
    export_lever_ratio.append(live_result[2])
    export_returns.append(live_result[3])

    in_sample = []  # reset in_sample array

export = pd.DataFrame()
export["Quarter"] = export_quarters
export["Holding Period"] = export_holding_period
export["Long Percentile"] = export_long_percentile
export["Short Percentile"] = export_short_percentile
export["Upside Percentile"] = export_upside_percentile
export["Long-leg"] = export_long_leg
export["Short-leg"] = export_short_leg
export["Expected Geometric Return"] = export_expected_geo_return
export["Spread"] = export_returns
export["Historic Mean Leverage Ratio"] = export_mean_lever_ratio_historic
export["Leverage Ratio"] = export_lever_ratio
export.to_excel("results_UEBIT_UPSIDE_RNN.xlsx")


