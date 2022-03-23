from .functions import BDP, BDH, MEMB
from pdblpi import BDP, BDH, MEMB
import pandas as pd


def get_portfolio(ticker, pcs):# {{{

    port = MEMB(ticker, all_cols=True)
    port = port.loc[port['weight']>0].copy()
    port['bbg_id'] = BDP(port['ticker'], 'PARSEKYABLE_DES_SOURCE', pricing_source=pcs)
    port['quote_type'] = BDP(port['bbg_id'], 'PCS_QUOTE_TYP')
    # first reweight to 100%
    port['actual_weight'] = port['weight'].copy()
    port['weight'] = port['weight'] * (1 / port['weight'].sum())

    print(ticker)
    print(port['quote_type'].value_counts())

    # port = port.loc[(~port['quote_type'].isna()) & (port['quote_type']!='security not found')]
    # port['weight'] = port['weight'] * (1 / port['weight'].sum())
    # find valid securities

    port['include'] = 'N'
    port.loc[(~port['quote_type'].isna()) & (port['quote_type']!='security not found'), 'include'] = 'Y'


    # now reweight
    port['weight'] = port['weight'] * (1 / port.loc[port['include']=='Y', 'weight'].sum())




    return port# }}}

def get_prices(port, timeperiod, quote_type):# {{{

    if quote_type == 1:
        bid = 'PX_BID'
        ask = 'PX_ASK'
    elif quote_type == 2:
        bid = 'PX_DISC_BID'
        ask = 'PX_DISC_ASK'
    else:
        raise ValueError(f'quote type of {quote_type} not yet supported')


    ed = (pd.to_datetime('today') - pd.Timedelta(days=1)).strftime('%Y%m%d')
    sd = (pd.to_datetime('today') - pd.Timedelta(days=timeperiod)).strftime('%Y%m%d')

    bid_df = BDH(port['bbg_id'].unique().tolist(), bid, sd, ed)
    ask_df = BDH(port['bbg_id'].unique().tolist(), ask, sd, ed)

    comb = bid_df.merge(ask_df, how='outer', on=['ticker', 'date'], validate='1:1')


    comb['spread'] = comb[ask] - comb[bid]
    comb['mid'] = (comb[ask] + comb[bid]) / 2
    comb['spread_bps'] = (comb['spread'] / comb['mid']) * 10000

    comb = comb.rename(columns={bid: 'bid', ask: 'ask'})

    comb = comb.merge(port[['bbg_id', 'weight', 'actual_weight']], how='left', left_on='ticker', right_on='bbg_id')

    comb['weight_spread_bps'] = comb['spread_bps'] * comb['weight']

    return comb# }}}

def get_basket_spread(ticker, pcs, timeperiod):# {{{

    port = get_portfolio(ticker, pcs)

    res = pd.DataFrame()
    for qt in port['quote_type'].unique().tolist():
        if pd.isna(qt) or qt == 'security not found':
            continue
        tmp = port.loc[port['quote_type']==qt]
        prices = get_prices(tmp, 20, qt)
        res = res.append(prices, ignore_index=True)


    # reweight again
    res['weight'] = res['weight'] * (1 / res.loc[~res['spread_bps'].isna(), 'weight'].sum())
    twa_spread = res.groupby('date')['weight_spread_bps'].sum().mean()

    return twa_spread, res# }}}



# x = get_basket_spread('JEST LN Equity', 'BVAL', 20)



tickers = ['JPST', 'JPHY', 'JAGG', 'JPIB', 'JCPB', 'JSCP', 'JPIE', 'JMUB', 'JIGB', 'JPMB', 'BBSA']
tickers = [x + ' US Equity' for x in tickers]


final = pd.DataFrame()

for t in tickers:
    print(f'getting data for {t}')
    x = get_basket_spread(t, 'BVAL', 20)
    x = x[1]
    x['etf_ticker'] = t
    final = final.append(x)
    print(f'got data for {t}')





valuation = load_holdings_from_val_file('JPST', '2022-02-08', 'emea')

val_cons = create_bbg_id_column(valuation[0], valuation[1])


val_cons['bval_id'] = BDP(val_cons['bbg_identifier'], 'PARSEKYABLE_DES_SOURCE', pricing_source='BVAL')


