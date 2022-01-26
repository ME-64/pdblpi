from .functions import BDP, BBAT, BDIT
import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots

pio.renderers.default = 'browser'



def _retrieve_data(self, ticker, date, fair_value=True, inav=False):

    summary = BBAT(ticker, date, fair_value=fair_value, inav=inav, summary=True)
    pass




