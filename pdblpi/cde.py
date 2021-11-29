import pandas as pd
import numpy as np
from datetime import date, datetime
from functools import singledispatch
import enum

import collections
import six
import datetime as dt
import dateutil

import blpapi


def get_upload_type(series):# {{{
    # Must check if Date first because content_type='Date' is data type STRING
    # according to CDE.

    data = type(series.iloc[0])
    if data == str:
        return NullableStringArrayVal
    elif np.issubdtype(data, np.number):
        return NullableDoubleArrayVal
    elif data == pd.Timestamp:
        return NullableDateTimeArrayVal
    else:
        raise Exception(f'Unsupported content type {data} for {series.name}')
    # }}}



class StringArrayVal:
    def __init__(self, string_array):# {{{
        self.stringArrayVal = string_array# }}}

    def to_dict(self):# {{{
        return {'stringArrayVal': self.stringArrayVal}# }}}


class Tickers:
    def __init__(self, security_list):# {{{
        self.name = 'Ticker'
        self.value = StringArrayVal(security_list)# }}}

    def to_dict(self):# {{{
        return {'name': self.name, 'value': self.value.to_dict()}# }}}


class AsOfDates:
    def __init__(self, datetime_array):# {{{
        self.name = 'AsOfDate'
        self.value = NullableDateTimeArrayVal(datetime_array)# }}}

    def to_dict(self):# {{{
        return {'name': self.name, 'value': self.value.to_dict()}# }}}


class Nullable:
    def __init__(self, value):# {{{
        self.value = value# }}}

    def to_dict(self):# {{{
        return {'nullable': self.value}# }}}


@singledispatch# {{{
def serialize_date(date_value):
    raise TypeError('Unsupported type.')# }}}

@serialize_date.register(str)# {{{
def _(date_value):
    return date_value# }}}

@serialize_date.register(date)# {{{
def _(date_value):
    return date_value.isoformat()# }}}

@serialize_date.register(datetime)# {{{
@serialize_date.register(pd.Timestamp)
def _(date_value):
    return date_value.date().isoformat()# }}}


class NullableDateTimeArrayVal:
    def __init__(self, values):# {{{
        self.values = [Nullable(value.strftime('%Y-%m-%d')) for value in values]# }}}

    def to_dict(self):# {{{
        return {'nullableDateTimeArrayVal': [value.to_dict()
                                             for value in self.values]}# }}}


class NullableDoubleArrayVal:
    def __init__(self, values):# {{{
        self.values = [Nullable(value) for value in values]# }}}

    def to_dict(self):# {{{
        return {'nullableDoubleArrayVal': [value.to_dict()
                                           for value in self.values]}# }}}


class NullableStringArrayVal:
    def __init__(self, values):# {{{
        self.values = [Nullable(value) for value in values]# }}}

    def to_dict(self):# {{{
        return {'nullableStringArrayVal': [value.to_dict()
                                           for value in self.values]}# }}}


class GenericValues:
    def __init__(self, field):# {{{
        self._field = field.name
        value_type = get_upload_type(field)
        self.values = value_type(field)# }}}

    def to_dict(self):# {{{
        return {'name': self._field, 'value': self.values.to_dict()}# }}}


class UploadRequest:
    def __init__(self, sess_id, security_list, as_of_date, field# {{{
                 ):
        self.sess_id = sess_id
        self.ticker = Tickers(security_list)
        self.as_of_date = AsOfDates(as_of_date)
        self.values_list = GenericValues(field)# }}}

    def to_dict(self):# {{{
        y = {
                'sessionId': self.sess_id,
                'tail': 'UPDATECDE',
                'dealStructureOverride': {
                        'param': [
                            {
                                'name': 'AppName',
                                'value': {'stringVal': 'BQUANT_BQCDE'}
                            },
                            self.ticker.to_dict(),
                            self.as_of_date.to_dict(),
                            self.values_list.to_dict()]
                }
        }
        return y# }}}


class CDEDataType(enum.Enum):# {{{
    DATE = 0
    DOUBLE = 2
    STRING = 3
    ENUM = 4# }}}

def fill_element(element, value):# {{{
    """Fill a BLPAPI element from native python types.

    This function maps dict-like objects (mappings), list-like
    objects (iterables) and scalar values to the BLPAPI element
    structure and fills `element` with the structure given
    in `value`.
    """

    def prepare_value(value):
        if isinstance(value, dt.datetime):
            # Convert datetime objects to UTC for all API requests
            return value.astimezone(dateutil.tz.tzutc())
        else:
            return value

    if isinstance(value, collections.Mapping):
        for name, val in six.iteritems(value):
            fill_element(element.getElement(name), val)
    elif isinstance(value, collections.Iterable) and not isinstance(value, six.string_types):
        for val in value:
            # Arrays of arrays are not allowed
            if isinstance(val, collections.Mapping):
                fill_element(element.appendElement(), val)
            else:
                element.appendValue(prepare_value(val))
    else:
        if element.datatype() == blpapi.DataType.CHOICE:
            element.setChoice(prepare_value(value))
        else:
            element.setValue(prepare_value(value))# }}}

def _cut_into_chunks_gen(iterable, chunk_size):# {{{
    """method to cut down dataframe into list of correctly sized chunks"""
    return (iterable.iloc[i:i + chunk_size]
            for i in range(0, len(iterable), chunk_size))# }}}

def _make_upload_requests(data_chunks, session_id, field):# {{{
    """method to create an individual upload request for each chunk"""
    return [UploadRequest(session_id, chunk['ticker'], chunk['date'], chunk[field])
            for chunk in data_chunks]# }}}

def _attempt_upload_request(con, upload_request,# {{{
                            num_retries=4):
    """method to attempt an upload of a chunk"""
    attempts_remaining = num_retries
    result_status_code = None
    while result_status_code != 'S_SUCCESS' and attempts_remaining:
        attempts_remaining -= 1
        print(f'retries remaining: {attempts_remaining}')
        try:
            request = con.cdeService.createRequest('uploadRequest')
            fill_element(request.asElement(), upload_request.to_dict())
            con._session.sendRequest(request, identity=con._identity)
        except Exception as err:
            print('failure to upload chunk')
            raise
        resp = None
        for msg in con._receive_events():
            resp = msg
        result_status_code = resp['element']['uploadResponse']['returnStatus']['returnStatus']['status']

    return resp, result_status_code# }}}

def _send_upload_requests(con, upload_requests):# {{{
    # enumerate requests for in case of error
    for num, upload_request in enumerate(upload_requests):
        result, status = _attempt_upload_request(con, upload_request)
        try:
            partial = result['element']['uploadResponse']['returnStatus']['returnStatus']['notifications'][0]['notifications']['message']
            print(partial)
            if partial == 'WRITE_PARTIALLY_FAILED':
                partial = True
            else:
                partial = False
        except Exception as e:
            print(e)
            partial = False

        # partial upload error - not sure why some tickers don't like uploads
        if (status != 'S_SUCCESS') and not partial:
            raise ValueError(f'Upload request failed with msg {result} and status {status}')# }}}

def _run_field_upload(con, ticker, as_of_date, field):# {{{
    """method to co-ordinate process of uploading a single field"""

    field_name = field.name
    df = ticker.to_frame().join(as_of_date).join(field)
    chunks = _cut_into_chunks_gen(df, 1000)

    requests = _make_upload_requests(chunks, con._sess_id, field_name)
    _send_upload_requests(con, requests)# }}}

def _run_df_upload(con, df):# {{{

    cols = df.columns.tolist()

    fields = [x for x in cols if x not in ['ticker', 'date']]

    if (len(fields) < 1) or ('ticker' not in cols) or ('date' not in cols):
        raise Exception(f'DataFrame must have columns: `ticker`, `date`, and'
                        f'atleast one CDE field. Instead df had: {cols}')

    for field in fields:
        tmp = df[['ticker', 'date', field]].copy()
        tmp = tmp.loc[~tmp[field].isna()].reset_index(drop=True)
        _run_field_upload(con, tmp['ticker'], tmp['date'], tmp[field])
        print(f'upload for field {field} complete with {tmp.shape[0]} records')
        # }}}
