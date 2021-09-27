import xlwings as xw
import time
import pandas as pd

def create_excel_app(path=r"C:\Program Files (x86)\BLP\API\Office Tools\BloombergUI.xla"):# {{{
    """function to create a new excel instance, with the bloomberg add-in running"""
    app = xw.App(visible=False)
    app.display_alerts = False
    bbg_addin = app.books.open(path)
    bk = app.books.add()
    sh = bk.sheets['Sheet1']
    return sh# }}}

def run_bql(sh, universe=None, expression=None, local_variables=None, query=None, timeout=15,# {{{
        show_dates=None, show_headers=None, show_query=None, show_ids=None, transpose=None,
        sort_dates_desc=None, group_by_fields=None, show_all_cols=None):

    optionals = []
    if show_dates == True:
        optionals.append('"showDates=True"')
    elif show_dates == False:
        optionals.append('"ShowDates=False"')
    if show_headers == True:
        optionals.append('"showHeaders=True"')
    elif show_headers == False:
        optionals.append('"ShowHeaders=False"')
    if show_query == True:
        optionals.append('"showquery=True"')
    elif show_query == False:
        optionals.append('"Showquery=False"')
    if show_ids == True:
        optionals.append('"showids=True"')
    elif show_ids == False:
        optionals.append('"Showids=False"')
    if transpose == True:
        optionals.append('"transpose=True"')
    elif transpose == False:
        optionals.append('"transpose=False"')
    if group_by_fields == True:
        optionals.append('"groupbyfields=True"')
    elif group_by_fields == False:
        optionals.append('"groupbyfields=False"')
    if sort_dates_desc == True:
        optionals.append('"xlsort=DESC"')
    elif sort_dates_desc == False:
        optionals.append('"xlsort=ASC"')
    if show_all_cols == True:
        optionals.append('"showallcols=True"')
    elif show_all_cols == False:
        optionals.append('"showallcols=False"')

    if optionals:
        optional_strings = ','.join(optionals)
    else:
        optional_strings = ''

    if local_variables == None: local_variables = ''

    if query:
        query = f'=BQL.Query("{query}", {optional_strings})'
    elif universe and expression:
        query = f'=BQL("{universe}", "{expression}", {optional_strings}, "{local_variables}")'
    else:
        raise ValueError("Either argument `query` must be given or `universe` and expression`")

    # clear any existing formulas
    sh.clear()
    sh.range('A1').value = query

    for t in range(timeout * 2):
        if sh.range('a1').value == '#N/A Requesting Data...':
            time.sleep(0.5)
        else:
            break

    rng = sh.used_range
    try:
        df = rng.options(pd.DataFrame).value
        df = df.reset_index()
    except:
        df = rng.value

    return df# }}}

