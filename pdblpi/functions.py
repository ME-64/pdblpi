from .main import _BDP, _BDH, _BDS, _BEQS, _SECF, _BCDE, _HDS, _BQL, _MEMB, _EPRX, _BBAT, _BDIT


def BDP(tickers, field, **field_ovrds):# {{{
    """
    Parameters
    ----------
    tickers: one ticker or a list of tickers
        tickers should be specified in the same format as the excel API.
        Currently Bloomberg Identifiers, ISINs, FIGIs, and SEDOLs are supported.
        Valid Format Examples:
            > BBEG LN Equity
            > JRUD GR Equity
            > IE00BYVZV757 ISIN
            > IE00BYVZV757@BVAL ISIN
            > IE00BYVZV757 LN ISIN
            > 2046251 SEDOL
            > 2046251@BGN SEDOL
            > 2046251 US SEDOL

    field: string
        The bloomberg field mnemonic for the datapoint you wish to use.
        Use FLDS<GO> on the terminal to find valid bloomberg fields.
        Only one field can be specified per query

    field_ovrds: string or list
        - There can be multiple field overides in a query
        - Any one of the valid overrides for the given fields
        - To find valid field overrides, use FLDS<GO>, select your field and 
          look at the bottom table.
        - For example: "PX_LAST" has the optional overrides "TIME_ZONE_OVERRIDE"
          and "DIVIDEND_ADJ_LAST_PX_OVERRIDE"
        - to use this in a query you would say time_zone_override=12
        - If you want to use a different override value for each ticker in the query
          you will need to supply a list instead of one value. See examples below.


    Returns
    -------
    data: list
        - A list of the datapoints is returned. 1 datapoint is returned per security queried.
        - If one of the tickers you input is not valid the value for that ticker will be
          "security not found"
        - If there is no data for the field you specified - the list will contain a None value


    Examples
    --------

    # Intial setup for our examples
    >>> from pdblpi import BDP
    >>> import pandas as pd
    >>> # define a list of tickers
    >>> tickers = ['JPST LN Equity', 'JREG LN Equity', 'MBILX IX Equity', 'IE00BJK9H753 ISIN']


    # BASIC USAGE - No overrides
        here we are simply getting the Fund NAV for our 4 securities
        that we defined above
    >>> data = BDP(tickers, 'FUND_NET_ASSET_VAL')
    >>> print(data)
    [101.07, 35.038, 2099.168, 36.078]

    # INTERMEDIATE USAGE - Overriding a field
        here we are getting the Fund NAV again, but we
        are changing the Fund NAV with the "NAV_CRNCY" override
    >>> data = BDP(tickers, 'FUND_NET_ASSET_VAL', nav_crncy='USD')
    >>> print(data)
    [101.07, 35.038, 102.694, 36.078]


    # ADVANCED USAGE - Using a different override for each ticker
        Again we want to get the FUND NAV, and we are also overriding
        it with "NAV_CRNCY". But rather than return every NAV in USD
        we want to use a different currency for each of our 4 tickers
    >>> currencies = ['MXN', 'JPY', 'HKD', 'CNY']
    >>> data = BDP(tickers, 'FUND_NET_ASSET_VAL', nav_crncy=currencies)
    >>> print(data)
    [2065.921, 3876.955, 798.3883, 236.4137]


    # ADVANCED USAGE - Specifiying Multiple Overrides
        here we want to find the return from November 2020 to January 2021
        we are going to use bloomberg's "CUST_TRR_RETURN_HOLDING_PER" field
        to do this. Look in FLDS<GO> - it has lot's of overrides!
    >>> data = BDP(tickers, "CUST_TRR_RETURN_HOLDING_PER",
    ...         cust_trr_crncy='USD',             # all returns from USD investor perspective
    ...         cust_trr_dvd_typ=0,               # Gross Dividends, not Net
    ...         cust_trr_start_dt='20201101',   # start date, using the format specified in FLDS
    ...         cust_trr_end_dt='20210131')     # end date, using the format specified in FLDS
    >>> print(data)
    [0.2933577, 15.90333, 5.385714, 14.91376]


    # ADVANCED USAGE - Specifying Multiple Overrides with different values per ticker
        We are going to build on the example above, except rather
        than use USD as the price for every ticker, we want some returns
        in EUR terms and others in USD terms. We will also use gross dividends
        for some tickers and net dividends for others to demonstrate using multiple overrides

    >>> currencies = ['USD', 'USD', 'EUR', 'EUR'] # specifying 4 currencies for our 4 tickers
    >>> dvd_typ = [0, 1, 0, 1]                    # using gross dividends for some, net for others
    >>> data = BDP(tickers, "CUST_TRR_RETURN_HOLDING_PER",
    ...         cust_trr_crncy=currencies,        # passing in our list of currencies
    ...         cust_trr_dvd_typ=dvd_typ,         # passing in our list of dividend types
    ...         cust_trr_start_dt='20201101',     # start date same as before
    ...         cust_trr_end_dt='20210131')       # end date same as before
    >>> print(data) # notice how returns differ from our last example
    [0.2933577, 15.90333, 1.164031, 10.31039]



    # ALTERNATIVE USAGE - Using Pandas DataFrames
        In previous examples we were using lists to define our tickers
        and overrides. However, more commonly we would have a dataframe
        that we want to add additional columns to.
    >>> tickers = ['JPST LN Equity', 'JREG LN Equity', 'MBILX IX Equity', 'IE00BJK9H753 ISIN']
    >>> # converting our list to a dataframe
    >>> df = pd.DataFrame({'ticker': tickers})
    >>> print(df)
                  ticker
    0     JPST LN Equity
    1     JREG LN Equity
    2    MBILX IX Equity
    3  IE00BJK9H753 ISIN
    >>> # now we are going to create a new column called 'nav' which will retrieve the NAV like before
    >>> df['nav'] = BDP(df['ticker'], 'FUND_NET_ASSET_VAL')
    >>> print(df)
                  ticker       nav
    0     JPST LN Equity   101.070
    1     JREG LN Equity    35.038
    2    MBILX IX Equity  2099.168
    3  IE00BJK9H753 ISIN    36.078
    >>> # we can also use overrides exactly like before, lets get the USD NAVs
    >>> df['usd_nav'] = BDP(df['ticker'], 'FUND_NET_ASSET_VAL', nav_crncy='USD')
    >>> print(df)
                  ticker       nav   usd_nav
    0     JPST LN Equity   101.070  101.0700
    1     JREG LN Equity    35.038   35.0380
    2    MBILX IX Equity  2099.168  102.5936
    3  IE00BJK9H753 ISIN    36.078   36.0780
    >>> # finally, we can also use different overrides for each ticker by giving a list
    >>> currencies = ['MXN', 'JPY', 'HKD', 'CNY']
    >>> df['multi_nav'] = BDP(df['ticker'], 'FUND_NET_ASSET_VAL', nav_crncy=currencies)
    >>> print(df)
                  ticker       nav   usd_nav  multi_nav
    0     JPST LN Equity   101.070  101.0700  2067.8920
    1     JREG LN Equity    35.038   35.0380  3879.2320
    2    MBILX IX Equity  2099.168  102.5936   797.6428
    3  IE00BJK9H753 ISIN    36.078   36.0780   236.4137
    """
    return _BDP(tickers, field, **field_ovrds)# }}}

def BDH(tickers, field, start_date, end_date, cdr=None, fx=None, fill='B',# {{{
        usedpdf=True, period='D', **field_ovrds):
    """
    Parameters
    ----------
    tickers: one ticker or a list of tickers
        tickers should be specified in the same format as the excel API.
        Currently Bloomberg Identifiers, ISINs, FIGIs, and SEDOLs are supported.
        Valid Format Examples:
            > BBEG LN Equity
            > JRUD GR Equity
            > IE00BYVZV757 ISIN
            > IE00BYVZV757@BVAL ISIN
            > IE00BYVZV757 LN ISIN
            > 2046251 SEDOL
            > 2046251@BGN SEDOL
            > 2046251 US SEDOL
        examples:
        tickers='JPST LN Equity'
        tickers=['JPST LN Equity', '2046251 US SEDOL']

    field: string
        The bloomberg field mnemonic for the datapoint you wish to use.
        Use FLDS<GO> on the terminal to find valid bloomberg fields.
        Only one field can be specified per query
        examples: 
        field='FUND_NET_ASSET_VAL'
        field='PX_LAST'

    start_date: YYYYmmdd
        the date to begin the historical request specified as a string in YYYYmmdd format
        examples:
        start_date='20200101'
        start_date='19991231'

    end_date: YYYYmmdd
        the date to begin the historical request specified as a string in YYYYmmdd format
        examples:
        end_date='20200101'
        end_date='19991231'

    cdr: string
        DEFAULT: cdr=None (uses per-security calendar)
        the two letter code to specify the calendar to use. The historical request
        will only return data for days that are included in the CDR calendar.
        To see available CDR codes, and their holidays use CDR<GO> in the terminal
        examples:
        cdr='5D' # this will return all weekdays (i.e. not saturday and sunday)
        cdr='7D' # this will return everyday of the week
        cdr='US' # this will return all valid days in the U.S. calendar (i.e. excludes weekends and holidays)

    fx: string
        DEFAULT: fx=None (no currency conversion)
        The 3 letter Currency ISO code to convert the time series to
        examples:
        fx='USD'
        fx='CNY'

    fill: string ('B' or 'P')
        DEFAULT: fill='B'
        This will determine what to fill missing values with if there is no datapoint for a given
        day. For example if you are using cdr='7D' then even weekends will be returned. Obviously
        exchanges are closed on the weekend so there is no value.
        'B' - Blank fill. i.e. leave it as a missing value
        'P' - Previous fill. i.e. fill data with the previous day value. Note if the previous value
              is more than 30 days before the empty date, bloomberg API will still return a blank
        examples:
        fill='B'
        fill='P'


    usedpdf: boolean (True/False)
        This determines whether the historical pricing will follow user settings in DPDF<GO>
        For example if the user has set NAVs to be historically adjusted for dividends, 
        setting usedpdf=True will respect this. With usedpdf=False - the historical NAVs
        will be clean and not adjusted for dividend distributions
        examples:
        usedpdf=True
        usedpdf=False


    period: string ('D' or 'W' or 'M' or 'S' or 'Q' or 'A')
        The periodicity to return dates for (default = 'D')
        Note, this argument will override CDR. CDR only works with daily periodicity
        D : DAILY
        W : WEEKLY
        M : MONTHLY
        Q : QUARTERLY
        S : SEMI-ANNUALLY
        A : ANNUALLY
        examples:
        period='M'
        period='S'

    field_ovrds: string or list
        - There can be multiple field overides in a query
        - Any one of the valid overrides for the given fields
        - To find valid field overrides, use FLDS<GO>, select your field and 
          look at the bottom table.
        - For example: "PX_LAST" has the optional overrides "TIME_ZONE_OVERRIDE"
          and "DIVIDEND_ADJ_LAST_PX_OVERRIDE"
        - to use this in a query you would say: time_zone_override=12
        - If you want to use a different override value for each ticker in the query
          you will need to supply a list instead of one value. See examples below.


    Examples
    --------

    # Intial setup for our examples
    >>> from pdblpi import BDH
    >>> import pandas as pd
    >>> # define a list of tickers
    >>> tickers = ['JPST LN Equity', 'JREG LN Equity', 'JE13 LN Equity', 'IE00BJK9H753 ISIN']


    # BASIC USAGE - Simple query
        We will start by demonstrating the difference between BDH when a date range is given
        (i.e. there are multiple days between 'start_date' and 'end_date').
        We will start by showing the difference between what BDH returns when you query with
        a date range, or you are just loooking for a single date
    >>> # single date - a simple list is returned
    >>> data = BDH(tickers, 'PX_LAST', start_date='20200115', end_date='20200115')
    >>> print(data)
    [98.8879, 98.8879, 28.873, 98.8879]
    >>> # a date range - now we get a full dataframe back
    >>> data = BDH(tickers, 'PX_LAST', start_date='20200115', end_date='20200116')
    >>> print(data)
                  ticker       date   PX_LAST
    0     JPST LN Equity 2020-01-15   98.8879
    1     JPST LN Equity 2020-01-16   98.9051
    2     JREG LN Equity 2020-01-15   28.8730
    3     JREG LN Equity 2020-01-16   28.9400
    4     JE13 LN Equity 2020-01-15  100.1500
    5     JE13 LN Equity 2020-01-16  100.1180
    6  IE00BJK9H753 ISIN 2020-01-15   29.0830
    7  IE00BJK9H753 ISIN 2020-01-16   29.1700
    >>> # same query, but now using a currency override
    >>> data = BDH(tickers, 'PX_LAST', start_date='20200115', end_date='20200116', fx='JPY')
    >>> print(data)
                  ticker       date      PX_LAST
    0     JPST LN Equity 2020-01-15  10866.79014
    1     JPST LN Equity 2020-01-16  10894.39154
    2     JREG LN Equity 2020-01-15   3172.79902
    3     JREG LN Equity 2020-01-16   3187.74100
    4     JE13 LN Equity 2020-01-15  12279.91849
    5     JE13 LN Equity 2020-01-16  12279.61411
    6  IE00BJK9H753 ISIN 2020-01-15   3195.87592
    7  IE00BJK9H753 ISIN 2020-01-16   3213.07550
    >>> # now lets look at what happens for weekends (or non-trading days)
    >>> # we've set the CDR to 7D - but we still don't get a value back for January first as it's
    >>> # a holiday, and there was no last price
    >>> data = BDH(tickers, 'PX_LAST', start_date='20200101', end_date='20200103', cdr='7D')
    >>> print(data)
                  ticker       date   PX_LAST
    0     JPST LN Equity 2020-01-02   98.8057
    1     JPST LN Equity 2020-01-03   98.8228
    2     JREG LN Equity 2020-01-02   28.5330
    3     JREG LN Equity 2020-01-03   28.4380
    4     JE13 LN Equity 2020-01-02  100.1800
    5     JE13 LN Equity 2020-01-03  100.2400
    6  IE00BJK9H753 ISIN 2020-01-02   28.5650
    7  IE00BJK9H753 ISIN 2020-01-03   28.5250
    >>> # now if we set the fill value to 'P' (i.e. previous value) we will see data for Jan 1st
    >>> data = BDH(tickers, 'PX_LAST', start_date='20200101', end_date='20200103', cdr='7D', fill='P')
    >>> print(data)
                   ticker       date   PX_LAST
    0      JPST LN Equity 2020-01-01   98.7690
    1      JPST LN Equity 2020-01-02   98.8057
    2      JPST LN Equity 2020-01-03   98.8228
    3      JREG LN Equity 2020-01-01   28.3300
    4      JREG LN Equity 2020-01-02   28.5330
    5      JREG LN Equity 2020-01-03   28.4380
    6      JE13 LN Equity 2020-01-01  100.2130
    7      JE13 LN Equity 2020-01-02  100.1800
    8      JE13 LN Equity 2020-01-03  100.2400
    9   IE00BJK9H753 ISIN 2020-01-01   28.3900
    10  IE00BJK9H753 ISIN 2020-01-02   28.5650
    11  IE00BJK9H753 ISIN 2020-01-03   28.5250
    >>> # now lets use the period ("periodicity") parameter to return less dates
    >>> # we are using period='A' - which equates to annual.
    >>> # when using a non-daily periodicity - the date returned will always be the end of the period
    >>> data = BDH(tickers, 'PX_LAST', start_date='20190101', end_date='20200131', period='A')
    >>> print(data)
                  ticker       date   PX_LAST
    0     JPST LN Equity 2019-12-31   98.7690
    1     JPST LN Equity 2020-12-31  101.0767
    2     JREG LN Equity 2019-12-31   28.3300
    3     JREG LN Equity 2020-12-31   33.2050
    4     JE13 LN Equity 2019-12-31  100.2130
    5     JE13 LN Equity 2020-12-31  100.1530
    6  IE00BJK9H753 ISIN 2019-12-31   28.3900
    7  IE00BJK9H753 ISIN 2020-12-31   34.1050
    """
    return _BDH(tickers, field, start_date, end_date, cdr, fx, fill, usedpdf, period, **field_ovrds)# }}}

def BDS(tickers, field, **field_ovrds):# {{{
    """
    Parameters
    ----------
    tickers: one ticker or a list of tickers
        tickers should be specified in the same format as the excel API.
        Currently Bloomberg Identifiers, ISINs, FIGIs, and SEDOLs are supported.
        Valid Format Examples:
            > BBEG LN Equity
            > JRUD GR Equity
            > IE00BYVZV757 ISIN
            > IE00BYVZV757@BVAL ISIN
            > IE00BYVZV757 LN ISIN
            > 2046251 SEDOL
            > 2046251@BGN SEDOL
            > 2046251 US SEDOL

    field: string
        The bloomberg field mnemonic for the datapoint you wish to use.
        Use FLDS<GO> on the terminal to find valid bloomberg fields.
        Only one field can be specified per query

    field_ovrds: string or list
        - There can be multiple field overides in a query
        - Any one of the valid overrides for the given fields
        - To find valid field overrides, use FLDS<GO>, select your field and 
          look at the bottom table.
        - For example: "PX_LAST" has the optional overrides "TIME_ZONE_OVERRIDE"
          and "DIVIDEND_ADJ_LAST_PX_OVERRIDE"
        - to use this in a query you would say time_zone_override=12
        - If you want to use a different override value for each ticker in the query
          you will need to supply a list instead of one value. See examples below.


    Returns
    -------
    data: list
        - A list of the datapoints is returned. 1 datapoint is returned per security queried.
        - If one of the tickers you input is not valid the value for that ticker will be
          "security not found"
        - If there is no data for the field you specified - the list will contain a None value

    """
    return _BDS(tickers, field, **field_ovrds)# }}}

def BEQS(eqs_screen_name, as_of_date=None):# {{{
    """
    Parameters
    ----------
    eqs_screen_name: string
        This is the name of the screen you want to pull data for.
        The screen must be defined by the user (or shared with) in EQS<GO>
        on the terminal beforehand

    as_of_date: string (YYYYmmdd)
        The date as of which to run the screen for.
        If not specified by the user, defaults to today


    Returns
    -------
    tickers: list
        Returns a list of tickers that meet the criteria on the as of date


    Examples
    --------
    Assume the user has a screen called "Europe ETFs"

    >>> from pdblpi import BEQS
    >>> etfs = BEQS("Europe ETFs", as_of_date='20200101')

    """
    return _BEQS(eqs_screen_name, as_of_date=as_of_date)# }}}

def BDIT(tickers, events, sd=None, ed=None, cond_codes=False, qrm=False, #{{{
        action_codes=False, exch_codes=False, broker_codes=False,
        indicator_codes=False, trade_time=True):
    """
    Parameters
    ----------
    tickers: one ticker or a list of tickers
        tickers should be specified in the same format as the excel API.
        Currently Bloomberg Identifiers, ISINs, FIGIs, and SEDOLs are supported.
        Valid Format Examples:
            > BBEG LN Equity
            > JRUD GR Equity
            > IE00BYVZV757 ISIN
            > IE00BYVZV757@BVAL ISIN
            > IE00BYVZV757 LN ISIN
            > 2046251 SEDOL
            > 2046251@BGN SEDOL
            > 2046251 US SEDOL
        examples:
        tickers='JPST LN Equity'
        tickers=['JPST LN Equity', '2046251 US SEDOL']

    events: string or list
        the name of the bloomberg events you want to include.
        Possible values include:
        - BID
        - ASK
        - TRADE
        - MID_PRICE
        - AT_TRADE
        - BEST_BID
        - BEST_ASK

    sd: YYYYmmdd HH:MM:SS
        the date to begin the historical request specified as a string in YYYYmmdd format
        examples:
        sd='20200101'
        sd='19991231'

    ed: YYYYmmdd HH:MM:SS
        the date to begin the historical request specified as a string in YYYYmmdd format
        examples:
        ed='20200101'
        ed='19991231'

    cond_codes: bool
        display the condition codes column for each quote/trade

    qrm: bool
        import additional trades that only show in QRM

    action_codes: bool
        display the action codes column

    broker_codes: bool
        display the broker codes column

    indicator_codes: bool
        display indicator codes column

    trade_time: bool
        display the time of a trade. This can differ from the time the trade was printed
        to the ticker in the case of delayed reporting


    Examples
    --------

    # Intial setup for our examples
    >>> from pdblpi import BDIT
    >>> import pandas as pd
    >>> BDIT('JPST LN Equity', ['BEST_BID', 'BEST_ASK'], '20220125 10:00:00', '20220125 12:00:00')
    """
    return _BDIT(tickers, events, sd, ed, cond_codes, qrm, action_codes, exch_codes, broker_codes,
            indicator_codes, trade_time)
    # }}}

def SECF(queries, filt=None, max_results=10):# {{{
    """
    Parameters
    ----------

    queries: str or list
        the search terms to be quiried. Identical functionality to using
        SECF<GO> in the terminal

    filt: str
        Filter the search results by bloomberg asset class.
        Possible values are:
        - EQTY
        - GOVT
        - CMDT
        - INDX
        - CURR
        - CORP
        - MUNI
        - PRFD
        - CLNT
        - MMKT
        - MTGE

    max_results: int
        the maximum number of matches to return

    Returns
    -------
    Pandas dataframe with columns: query, ticker, description, position
    """
    return _SECF(queries, filt, max_results)# }}}

def BCDE(df):# {{{
    """Upload new data to bloomberg fields defined in CDE<GO>
    Parameters
    ----------
    df: pd.DataFrame
        The Dataframe must have the following columns:
        - `ticker`: the bloomberg identifier for the upload. it is reccomended to use ID_BB_GLOBAL + "FIGI"
            for this identifier. Bloomberg sometimes has issues with other identifiers such as Ticker
        - `date`: the as of date for the field update. Must be a datetime rather than string
        - one column with the correct values for each field you wish to upload for
    Return
    ------
    None

    """
    return _BCDE(df)# }}}

def HDS(tickers):# {{{
    """Return information about the holders of a given security. Mirrors HDS<GO> functionality

    Parameters
    ----------
    tickers: one ticker or a list of tickers
        tickers should be specified in the same format as the excel API.
        Currently Bloomberg Identifiers, ISINs, FIGIs, and SEDOLs are supported.
        Valid Format Examples:
            > BBEG LN Equity
            > JRUD GR Equity
            > IE00BYVZV757 ISIN
            > IE00BYVZV757@BVAL ISIN
            > IE00BYVZV757 LN ISIN
            > 2046251 SEDOL
            > 2046251@BGN SEDOL
            > 2046251 US SEDOL

    Returns
    -------
    df: DataFrame
        dataframe with each holder in the portfolio
        To view parent level - filter `insider_status` for 'N-P'
    """
    return _HDS(tickers)# }}}

def BQL(universe=None, expression=None, query=None, show_dates=None, show_headers=None, # {{{
        show_query=None, show_ids=None, transpose=None,
        sort_dates_desc=None, group_by_fields=None, show_all_cols=None):
    """
    """
    return _BQL(universe, expresion, query, show_dates, show_headers, show_query, show_ids,
            transpose, sort_dates_desc, group_by_fields, show_all_cols)# }}}

def MEMB(tickers, all_cols=False, reweight=False, add_cash=False, valid_reweight=False):# {{{
    """
    Parameters
    ----------
    all_cols: bool
        Whether to return all columns in the BQL query or just ticker, portfolio ticker, position, weight
    reweight: bool
        Return a dataframe with the added column 'reweight' which will always sum to 100%
    add_cash: bool
        add a line item for cash when weight doesn't add up to 100%
    valid_reweight: bool
        Further reweighting column named 'valid_reweight' to return weights only for those that are recognised
        bloomberg tickers
    """
    return _MEMB(tickers, all_cols, reweight, add_cash, valid_reweight)# }}}

def EPRX(tickers, subset=None, decomp=False):# {{{

    """
    Parameters
    ----------

    tickers: str or list-like
        the list of tickers to return exchagne information for

    subset: str or list-like
        the subset of datapoints to return information for.
        Possible values include:
        - country_iso_code
        - country_name
        - region
        - composite_flag
        - bbg_exch_code
        - mic
        - asset_classes
        - exchange_name
        - acronym
        - venue_type
        - cdr_code
        - website_url
        - trading_days
        - timezone
        - pytz_timezone
        - data_delivery
        - ats
        - exch_company_ticker
        - rulebook_url
        - parent_flag
        - composite_exch_code
        - update_time
        - sample_equity_ticker
        - equity_close_price
        - equity_market_open
        - equity_market_close
        - equity_settlement_cycle
        - volume_ticker
        - turnover_ticker
        - eu_comp_exch_code
        - equity_delay_time
        - equity_entitlement_id
        - Foreign Security(Eq)
        - equity_trading_currency
        - AM Block Trade(Eq)
        - AM Closing Auction Call(Eq)
        - AM Closing Match Auction(Eq)
        - AM Continuous Trading Period(Eq)
        - AM Negotiated Market(Eq)
        - AM Opening Auction Call(Eq)
        - AM Opening Match Auction(Eq)
        - AM Preopening(Eq)
        - After Hours(Eq)
        - Block Trade(Eq)
        - Closing Auction Call Phase(Eq)
        - Cash Market(Eq)
        - Closing Match Auction(Eq)
        - Continuous Trading Period(Eq)
        - Lunch Break(Eq)
        - Mid-Day Auction(Eq)
        - Odd-Lot(Eq)
        - Opening Auction Call Phase(Eq)
        - Opening Match Auction(Eq)
        - PM Block Trade(Eq)
        - PM Closing Auction Call(Eq)
        - PM Closing Match Auction(Eq)
        - PM Continuous Trading Period(Eq)
        - PM Negotiated Market(Eq)
        - PM Opening Match Auction(Eq)
        - PM Opening Auction Call(Eq)
        - PM Preopening(Eq)
        - Post Close(Eq)
        - Preopening(Eq)
        - Scheduled Auction Call(Eq)
        - Trade at Close(Eq)

    decomp: bool
        Whether to find the primary exchange when a composite is passed through or
        not. Setting decomp to true will mean composite securities will return data
        for their primary exchange while setting it to false means they will return
        the limited information available about a composite

    """
    return _EPRX(tickers, subset, decomp)# }}}

def BBAT(tickers, sd, ed=None, inav=True, fair_value=None, summary=False):# {{{
    """Bloomberg Bid, Ask, Trade data (BBAT)
    This will take intraday bids, asks, and trades and pivot them into a nice timeseries.
    In addition, lots of common statistics are calculated and trading periods can be filtered out

    Parameters
    ----------
    tickers: one ticker or a list of tickers
        tickers should be specified in the same format as the excel API.
        Currently Bloomberg Identifiers, ISINs, FIGIs, and SEDOLs are supported.
        Valid Format Examples:
            > BBEG LN Equity
            > JRUD GR Equity
            > IE00BYVZV757 ISIN
            > IE00BYVZV757@BVAL ISIN
            > IE00BYVZV757 LN ISIN
            > 2046251 SEDOL
            > 2046251@BGN SEDOL
            > 2046251 US SEDOL
        examples:
        tickers='JPST LN Equity'
        tickers=['JPST LN Equity', '2046251 US SEDOL']

    sd: YYYYmmdd
        the date to begin the historical request specified as a string in YYYYmmdd format
        examples:
        start_date='20200101'
        start_date='19991231'

    sd: YYYYmmdd
        the date to begin the historical request specified as a string in YYYYmmdd format
        examples:
        end_date='20200101'
        end_date='19991231'

    """
    return _BBAT(tickers=tickers, sd=sd, ed=ed, inav=inav, fair_value=fair_value, qrm=qrm, summary=summary)# }}}
