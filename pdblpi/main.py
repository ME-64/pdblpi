import logging
import contextlib
import time
import json
import re
import collections.abc
from pathlib import Path
from functools import lru_cache
import collections
import six
import datetime
import dateutil

import numpy as np
import pandas as pd
import xlwings as xw
import pytz

import pyparsing as pp

import blpapi
from .cde import _run_df_upload
from .bql import create_excel_app, run_bql



__glob_pid = None

_RESPONSE_TYPES = [blpapi.Event.RESPONSE, blpapi.Event.PARTIAL_RESPONSE]

# partial lookup table for events used from blpapi.Event
_EVENT_DICT = {# {{{
              blpapi.Event.SESSION_STATUS: 'SESSION_STATUS',
              blpapi.Event.RESPONSE: 'RESPONSE',
              blpapi.Event.PARTIAL_RESPONSE: 'PARTIAL_RESPONSE',
              blpapi.Event.SERVICE_STATUS: 'SERVICE_STATUS',
              blpapi.Event.TIMEOUT: 'TIMEOUT',
              blpapi.Event.REQUEST: 'REQUEST'
}# }}}

def _get_logger(debug):# {{{
    logger = logging.getLogger(__name__)
    if (logger.parent is not None) and logger.parent.hasHandlers() and debug:
        logger.warning('"pdblp.BCon.debug=True" is ignored when user '
                       'specifies logging event handlers')
    else:
        if not logger.handlers:
            formatter = logging.Formatter('%(name)s:%(levelname)s:%(message)s')
            sh = logging.StreamHandler()
            sh.setFormatter(formatter)
            logger.addHandler(sh)
        debug_level = logging.INFO if debug else logging.WARNING
        logger.setLevel(debug_level)

    return logger# }}}

@contextlib.contextmanager# {{{
def bopen(**kwargs):
    """
    Open and manage a BCon wrapper to a Bloomberg API session

    Parameters
    ----------
    **kwargs:
        Keyword arguments passed into pdblp.BCon initialization
    """
    con = BCon(**kwargs)
    con.start()
    try:
        yield con
    finally:
        con.stop()# }}}

class BCon(object):
    def __init__(self, host='localhost', port=8194, debug=False, timeout=500,# {{{
                 session=None, identity=None):
        """
        Create an object which manages connection to the Bloomberg API session

        Parameters
        ----------
        host: str
            Host name
        port: int
            Port to connect to
        debug: Boolean {True, False}
            Boolean corresponding to whether to log Bloomberg Open API request
            and response messages to stdout
        timeout: int
            Number of milliseconds before timeout occurs when parsing response.
            See blp.Session.nextEvent() for more information.
        session: blpapi.Session
            A custom Bloomberg API session. If this is passed the host and port
            parameters are ignored. This is exposed to allow the user more
            customization in how they instantiate a session.
        identity: blpapi.Identity
            Identity to use for request authentication. This should only be
            passed with an appropriate session and should already by
            authenticated. This is only relevant for SAPI and B-Pipe.
        """

        if session is None:
            sessionOptions = blpapi.SessionOptions()
            sessionOptions.setServerHost(host)
            sessionOptions.setServerPort(port)
            session = blpapi.Session(sessionOptions)
        else:
            ev = session.nextEvent(timeout)
            if ev.eventType() != blpapi.Event.TIMEOUT:
                raise ValueError('Flush event queue of blpapi.Session prior '
                                 'to instantiation')

        self.timeout = timeout
        self._session = session
        self._identity = identity
        # initialize logger
        self.debug = debug# }}}

    @property# {{{
    def debug(self):
        """
        When True, print all Bloomberg Open API request and response messages
        to stdout
        """
        return self._debug# }}}

    @debug.setter# {{{
    def debug(self, value):
        """
        Set whether logging is True or False
        """
        self._debug = value# }}}

    def start(self):# {{{
        """
        Start connection and initialize session services
        """

        # flush event queue in defensive way
        logger = _get_logger(self.debug)
        started = self._session.start()
        if started:
            ev = self._session.nextEvent()
            ev_name = _EVENT_DICT[ev.eventType()]
            logger.info('Event Type: {!r}'.format(ev_name))
            for msg in ev:
                logger.info('Message Received:\n{}'.format(msg))
            if ev.eventType() != blpapi.Event.SESSION_STATUS:
                raise RuntimeError('Expected a "SESSION_STATUS" event but '
                                   'received a {!r}'.format(ev_name))
            ev = self._session.nextEvent()
            ev_name = _EVENT_DICT[ev.eventType()]
            logger.info('Event Type: {!r}'.format(ev_name))
            for msg in ev:
                logger.info('Message Received:\n{}'.format(msg))
            if ev.eventType() != blpapi.Event.SESSION_STATUS:
                raise RuntimeError('Expected a "SESSION_STATUS" event but '
                                   'received a {!r}'.format(ev_name))
        else:
            ev = self._session.nextEvent(self.timeout)
            if ev.eventType() == blpapi.Event.SESSION_STATUS:
                for msg in ev:
                    logger.warning('Message Received:\n{}'.format(msg))
                raise ConnectionError('Could not start blpapi.Session')
        self._init_services()
        return self# }}}

    def _init_services(self):# {{{
        """
        Initialize blpapi.Session services
        """
        logger = _get_logger(self.debug)

        # flush event queue in defensive way
        opened = self._session.openService('//blp/refdata')
        ev = self._session.nextEvent()
        ev_name = _EVENT_DICT[ev.eventType()]
        logger.info('Event Type: {!r}'.format(ev_name))
        for msg in ev:
            logger.info('Message Received:\n{}'.format(msg))
        if ev.eventType() != blpapi.Event.SERVICE_STATUS:
            raise RuntimeError('Expected a "SERVICE_STATUS" event but '
                               'received a {!r}'.format(ev_name))
        if not opened:
            logger.warning('Failed to open //blp/refdata')
            raise ConnectionError('Could not open a //blp/refdata service')
        self.refDataService = self._session.getService('//blp/refdata')

        opened = self._session.openService('//blp/exrsvc')
        ev = self._session.nextEvent()
        ev_name = _EVENT_DICT[ev.eventType()]
        logger.info('Event Type: {!r}'.format(ev_name))
        for msg in ev:
            logger.info('Message Received:\n{}'.format(msg))
        if ev.eventType() != blpapi.Event.SERVICE_STATUS:
            raise RuntimeError('Expected a "SERVICE_STATUS" event but '
                               'received a {!r}'.format(ev_name))
        if not opened:
            logger.warning('Failed to open //blp/exrsvc')
            raise ConnectionError('Could not open a //blp/exrsvc service')
        self.exrService = self._session.getService('//blp/exrsvc')

        opened = self._session.openService("//blp/instruments")
        ev = self._session.nextEvent()
        ev_name = _EVENT_DICT[ev.eventType()]
        logger.info("Event Type: %s" % ev_name)
        for msg in ev:
            logger.info("Message Received:\n%s" % msg)
        if ev.eventType() != blpapi.Event.SERVICE_STATUS:
            raise RuntimeError("Expected a SERVICE_STATUS event but "
                               "received a %s" % ev_name)
        if not opened:
            logger.warning("Failed to open //blp/instruments")
            raise ConnectionError("Could not open a //blp/instruments service")
        self.instrService = self._session.getService('//blp/instruments')


        opened = self._session.openService("//blp/cdeuupl")
        ev = self._session.nextEvent()
        ev_name = _EVENT_DICT[ev.eventType()]
        logger.info("Event Type: %s" % ev_name)
        for msg in ev:
            logger.info("Message Received:\n%s" % msg)
        if ev.eventType() != blpapi.Event.SERVICE_STATUS:
            raise RuntimeError("Expected a SERVICE_STATUS event but "
                               "received a %s" % ev_name)
        if not opened:
            logger.warning("Failed to open //blp/instruments")
            raise ConnectionError("Could not open a //blp/cdeuupl service")
        self.cdeService = self._session.getService('//blp/cdeuupl')

        # getting a session id
        req = self.cdeService.createRequest('startSession')
        req.set('showDealType', True)
        self._session.sendRequest(req, identity=self._identity)

        try:
            self._sess_id = None
            for msg in self._receive_events():
                self._sess_id = msg['element']['startSession']['sessionId']
        except:
            logger.error('Failed to get CDE session id')
            raise ConnectionError("Failed to get CDE session id")

        return self# }}}

    def _create_req(self, rtype, tickers, flds, ovrds, setvals):# {{{
        # flush event queue in case previous call errored out
        while(self._session.tryNextEvent()):
            pass

        request = self.refDataService.createRequest(rtype)
        for t in tickers:
            request.getElement('securities').appendValue(t)
        for f in flds:
            request.getElement('fields').appendValue(f)
        for name, val in setvals:
            request.set(name, val)

        overrides = request.getElement('overrides')
        for ovrd_fld, ovrd_val in ovrds:
            ovrd = overrides.appendElement()
            ovrd.setElement('fieldId', ovrd_fld)
            ovrd.setElement('value', ovrd_val)

        return request# }}}

    def secf(self, query, max_results=10, yk_filter=None):# {{{
        """
        This function uses the Bloomberg API to retrieve Bloomberg
        SECF Data queries. Returns list of tickers.
        Parameters
        ----------
        query: string
            A character string representing the desired query. Example "IBM"
        max_results: int
            Maximum number of results to return. Default 10.
        yk_filter: string
            A character string respresenting a Bloomberg yellow-key to limit
            search results to. Valid values are: CMDT, EQTY, MUNI,
            PRFD, CLNT, MMKT, GOVT, CORP, INDX, CURR, MTGE. Default NONE.
        Returns
        -------
        data: pandas.DataFrame
            List of bloomberg tickers from the SECF function
        """
        logger = _get_logger(self.debug)
        request = self.instrService.createRequest("instrumentListRequest")
        request.set("query", query)
        if max_results: request.set("maxResults", max_results)
        if yk_filter: request.set("yellowKeyFilter", "YK_FILTER_%s" % yk_filter)

        logger.info("Sending Request:\n%s" % request)
        self._session.sendRequest(request, identity=self._identity)
        data = {}
        data['ticker'] = []
        data['description'] = []
        data['position'] = []
        try:
            for msg in self._receive_events():
                for i, r in enumerate(msg['element']['InstrumentListResponse']['results']):
                    dat = r['results']
                    tckr = dat['security']
                    tckr = tckr.split('<')[0] + ' ' + tckr.split('<')[1].replace('>', '').title()
                    data['ticker'].append(tckr)
                    data['description'].append(dat['description'])
                    data['position'].append(i)
        except Exception as e:
            print('---WARNING----')
            # print(e)
            raise(e)
            # return pd.DataFrame(columns=['ticker', 'description', 'position'])

            # for r in msg.getElement("results").values():
            #     ticker = r.getElementAsString("security")
            #     descr = r.getElementAsString("description")
            #     data.append((ticker, descr))
        # return pd.DataFrame(data, columns=['ticker', 'description', 'position'])
        return pd.DataFrame(data)# }}}

    def _receive_events(self, sent_events=1, to_dict=True):# {{{
        logger = _get_logger(self.debug)
        while True:
            ev = self._session.nextEvent(self.timeout)
            ev_name = _EVENT_DICT[ev.eventType()]
            logger.info('Event Type: {!r}'.format(ev_name))
            if ev.eventType() in _RESPONSE_TYPES:
                for msg in ev:
                    logger.info('Message Received:\n{}'.format(msg))
                    if to_dict:
                        yield message_to_dict(msg)
                    else:
                        yield msg

            # deals with multi sends using CorrelationIds
            if ev.eventType() == blpapi.Event.RESPONSE:
                sent_events -= 1
                if sent_events == 0:
                    break
            # guard against unknown returned events
            elif ev.eventType() not in _RESPONSE_TYPES:
                logger.warning('Unexpected Event Type: {!r}'.format(ev_name))
                for msg in ev:
                    logger.warning('Message Received:\n{}'.format(msg))
                if ev.eventType() == blpapi.Event.TIMEOUT:
                    raise RuntimeError('Timeout, increase BCon.timeout '
                                       'attribute')
                else:
                    raise RuntimeError('Unexpected Event Type: {!r}'
                                       .format(ev_name))# }}}

    def bdh(self, tickers, flds, start_date, end_date, elms=None,# {{{
            ovrds=None, longdata=False):
        """
        Get tickers and fields, return pandas DataFrame with columns as
        MultiIndex with levels "ticker" and "field" and indexed by "date".
        If long data is requested return DataFrame with columns
        ["date", "ticker", "field", "value"].

        Parameters
        ----------
        tickers: {list, string}
            String or list of strings corresponding to tickers
        flds: {list, string}
            String or list of strings corresponding to FLDS
        start_date: string
            String in format YYYYmmdd
        end_date: string
            String in format YYYYmmdd
        elms: list of tuples
            List of tuples where each tuple corresponds to the other elements
            to be set, e.g. [("periodicityAdjustment", "ACTUAL")].
            Refer to the HistoricalDataRequest section in the
            'Services & schemas reference guide' for more info on these values
        ovrds: list of tuples
            List of tuples where each tuple corresponds to the override
            field and value
        longdata: boolean
            Whether data should be returned in long data format or pivoted
        """
        ovrds = [] if not ovrds else ovrds
        elms = [] if not elms else elms

        elms = list(elms)

        data = self._bdh_list(tickers, flds, start_date, end_date,
                              elms, ovrds)

        df = pd.DataFrame(data, columns=['date', 'ticker', 'field', 'value'])
        df.loc[:, 'date'] = pd.to_datetime(df.loc[:, 'date'])
        if not longdata:
            cols = ['ticker', 'field']
            df = df.set_index(['date'] + cols).unstack(cols)
            df.columns = df.columns.droplevel(0)

        return df# }}}

    def _bdh_list(self, tickers, flds, start_date, end_date, elms,# {{{
                  ovrds):
        logger = _get_logger(self.debug)
        if type(tickers) is not list:
            tickers = [tickers]
        if type(flds) is not list:
            flds = [flds]

        setvals = elms
        setvals.append(('startDate', start_date))
        setvals.append(('endDate', end_date))

        request = self._create_req('HistoricalDataRequest', tickers, flds,
                                   ovrds, setvals)
        logger.info('Sending Request:\n{}'.format(request))
        # Send the request
        self._session.sendRequest(request, identity=self._identity)
        data = []
        # Process received events
        for msg in self._receive_events():
            d = msg['element']['HistoricalDataResponse']
            try:
                has_security_error = 'securityError' in d['securityData']
                has_field_exception = len(d['securityData']['fieldExceptions']) > 0
            except KeyError as e:
                print(e)
                print(d)
                raise e
            if has_security_error or has_field_exception:
                raise ValueError(d)
            ticker = d['securityData']['security']
            fldDatas = d['securityData']['fieldData']
            for fd in fldDatas:
                for fname, value in fd['fieldData'].items():
                    if fname == 'date':
                        continue
                    data.append(
                        (fd['fieldData']['date'], ticker, fname, value)
                    )
        return data# }}}

    def beqs(self, screen_name, screen_type='PRIVATE', language_id=None, group=None, date=None, return_df=False):# {{{

        if date:
            ovrds = [('PiTDate', pd.to_datetime(date).strftime('%Y%m%d'))]
        else:
            ovrds = []
        request = self.refDataService.createRequest('BeqsRequest')

        request.set('screenName', screen_name)
        request.set('screenType', screen_type)
        if language_id:
            request.set('languageId', language_id)
        if group:
            request.set('Group', group)


        overrides = request.getElement('overrides')
        for ovrd_fld, ovrd_val in ovrds:
            ovrd = overrides.appendElement()
            ovrd.setElement('fieldId', ovrd_fld)
            ovrd.setElement('value', ovrd_val)
        self._session.sendRequest(request, identity=self._identity)
        data = []

        for msg in self._receive_events():
            try:
                res = msg['element']['BeqsResponse']['data']['securityData']

            except Exception as e:
                print('likely a timeout error')
                print(msg)
                raise e

            for ticker in res:
                data.append(ticker['securityData']['security'])


        if return_df:
            data = [d + ' Equity' for d in data]
            data = pd.DataFrame(data, columns=['ticker'])

        return data# }}}

    def ref(self, tickers, flds, ovrds=None):# {{{
        """
        Make a reference data request, get tickers and fields, return long
        pandas DataFrame with columns [ticker, field, value]

        Parameters
        ----------
        tickers: {list, string}
            String or list of strings corresponding to tickers
        flds: {list, string}
            String or list of strings corresponding to FLDS
        ovrds: list of tuples
            List of tuples where each tuple corresponds to the override
            field and value

        Example
        -------
        >>> import pdblp
        >>> con = pdblp.BCon()
        >>> con.start()
        >>> con.ref("CL1 Comdty", ["FUT_GEN_MONTH"])

        Notes
        -----
        This returns reference data which has singleton values. In raw format
        the messages passed back contain data of the form

        fieldData = {
                FUT_GEN_MONTH = "FGHJKMNQUVXZ"
        }
        """
        ovrds = [] if not ovrds else ovrds

        logger = _get_logger(self.debug)
        if type(tickers) is not list:
            tickers = [tickers]
        if type(flds) is not list:
            flds = [flds]
        request = self._create_req('ReferenceDataRequest', tickers, flds,
                                   ovrds, [])
        logger.info('Sending Request:\n{}'.format(request))
        self._session.sendRequest(request, identity=self._identity)
        data = self._parse_ref(flds)
        data = pd.DataFrame(data)
        data.columns = ['ticker', 'field', 'value']
        return data# }}}

    def _parse_ref(self, flds, keep_corrId=False, sent_events=1):# {{{
        data = []
        # Process received events
        for msg in self._receive_events(sent_events):
            if keep_corrId:
                corrId = msg['correlationIds']
            else:
                corrId = []
            d = msg['element']['ReferenceDataResponse']
            for security_data_dict in d:
                secData = security_data_dict['securityData']
                ticker = secData['security']
                if 'securityError' in secData:
                    na_val = 'security not found'
                    print(f'unknown security {ticker} -- careful')
                    # print(secData)
                else:
                    na_val = np.NaN
                    # new way to handle security errors
                    #datum = [ticker, fld, val]
                    #datum.extend(corrId)
                    #data.append(datum)
                    #continue
                    # raise ValueError('Unknow security {!r}'.format(ticker))
                    # continue
                self._check_fieldExceptions(secData['fieldExceptions'])
                fieldData = secData['fieldData']['fieldData']
                for fld in flds:
                    # avoid returning nested bbg objects, fail instead
                    # since user should use bulkref()
                    if (fld in fieldData) and isinstance(fieldData[fld], list):
                        raise ValueError('Field {!r} returns bulk reference '
                                         'data which is not supported'
                                         .format(fld))
                    # this is a slight hack but if a fieldData response
                    # does not have the element fld and this is not a bad
                    # field (which is checked above) then the assumption is
                    # that this is a not applicable field, thus set NaN
                    # see https://github.com/matthewgilbert/pdblp/issues/13
                    if fld not in fieldData:
                        datum = [ticker, fld, na_val]
                        datum.extend(corrId)
                        data.append(datum)
                    else:
                        val = fieldData[fld]
                        datum = [ticker, fld, val]
                        datum.extend(corrId)
                        data.append(datum)
        return data# }}}

    def bulkref(self, tickers, flds, ovrds=None):# {{{
        """
        Make a bulk reference data request, get tickers and fields, return long
        pandas DataFrame with columns [ticker, field, name, value, position].
        Name refers to the element name and position is the position in the
        corresponding array returned.

        Parameters
        ----------
        tickers: {list, string}
            String or list of strings corresponding to tickers
        flds: {list, string}
            String or list of strings corresponding to FLDS
        ovrds: list of tuples
            List of tuples where each tuple corresponds to the override
            field and value

        Example
        -------
        >>> import pdblp
        >>> con = pdblp.BCon()
        >>> con.start()
        >>> con.bulkref('BCOM Index', 'INDX_MWEIGHT')

        Notes
        -----
        This returns bulk reference data which has array values. In raw format
        the messages passed back contain data of the form

        fieldData = {
            INDX_MWEIGHT[] = {
                INDX_MWEIGHT = {
                    Member Ticker and Exchange Code = "BON8"
                    Percentage Weight = 2.410000
                }
                INDX_MWEIGHT = {
                    Member Ticker and Exchange Code = "C N8"
                    Percentage Weight = 6.560000
                }
                INDX_MWEIGHT = {
                    Member Ticker and Exchange Code = "CLN8"
                    Percentage Weight = 7.620000
                }
            }
        }
        """
        ovrds = [] if not ovrds else ovrds

        logger = _get_logger(self.debug)
        if type(tickers) is not list:
            tickers = [tickers]
        if type(flds) is not list:
            flds = [flds]
        setvals = []
        request = self._create_req('ReferenceDataRequest', tickers, flds,
                                   ovrds, setvals)
        logger.info('Sending Request:\n{}'.format(request))
        self._session.sendRequest(request, identity=self._identity)
        data = self._parse_bulkref(flds)
        data = pd.DataFrame(data)
        data.columns = ['ticker', 'field', 'name', 'value', 'position']
        return data# }}}

    def _parse_bulkref(self, flds, keep_corrId=False, sent_events=1):# {{{
        data = []
        # Process received events
        for msg in self._receive_events(sent_events):
            if keep_corrId:
                corrId = msg['correlationIds']
            else:
                corrId = []
            d = msg['element']['ReferenceDataResponse']
            for security_data_dict in d:
                secData = security_data_dict['securityData']
                ticker = secData['security']
                # if 'securityError' in secData:
                #     raise ValueError('Unknow security {!r}'.format(ticker))

                if 'securityError' in secData:
                    na_val = 'security not found'
                    print(f'unknown security {ticker} -- careful')
                    # print(secData)
                else:
                    na_val = np.NaN

                self._check_fieldExceptions(secData['fieldExceptions'])
                fieldData = secData['fieldData']['fieldData']
                for fld in flds:
                    # fail coherently instead of while parsing downstream
                    if (fld in fieldData) and not isinstance(fieldData[fld], list): # NOQA
                        raise ValueError('Cannot parse field {!r} which is '
                                         'not bulk reference data'.format(fld))
                    elif fld in fieldData:
                        for i, data_dict in enumerate(fieldData[fld]):
                            for name, value in data_dict[fld].items():
                                datum = [ticker, fld, name, value, i]
                                datum.extend(corrId)
                                data.append(datum)
                    else:  # field is empty or NOT_APPLICABLE_TO_REF_DATA
                        datum = [ticker, fld, na_val, na_val, na_val]
                        datum.extend(corrId)
                        data.append(datum)
        return data# }}}

    @staticmethod# {{{
    def _check_fieldExceptions(field_exceptions):
        # iterate over an array of field_exceptions and check for a
        # INVALID_FIELD error
        for fe_dict in field_exceptions:
            fe = fe_dict['fieldExceptions']
            if fe['errorInfo']['errorInfo']['subcategory'] == 'INVALID_FIELD':
                raise ValueError('{}: INVALID_FIELD'.format(fe['fieldId']))# }}}

    def ref_hist(self, tickers, flds, dates, ovrds=None,# {{{
                 date_field='REFERENCE_DATE'):
        """
        Make iterative calls to ref() and create a long DataFrame with columns
        [date, ticker, field, value] where each date corresponds to overriding
        a historical data override field.

        Parameters
        ----------
        tickers: {list, string}
            String or list of strings corresponding to tickers
        flds: {list, string}
            String or list of strings corresponding to FLDS
        dates: list
            list of date strings in the format YYYYmmdd
        ovrds: list of tuples
            List of tuples where each tuple corresponds to the override
            field and value. This should not include the date_field which will
            be iteratively overridden
        date_field: str
            Field to iteratively override for requesting historical data,
            e.g. REFERENCE_DATE, CURVE_DATE, etc.

        Example
        -------
        >>> import pdblp
        >>> con = pdblp.BCon()
        >>> con.start()
        >>> dates = ["20160625", "20160626"]
        >>> con.ref_hist("AUD1M CMPN Curncy", "SETTLE_DT", dates)

        """
        ovrds = [] if not ovrds else ovrds

        if type(tickers) is not list:
            tickers = [tickers]
        if type(flds) is not list:
            flds = [flds]

        self._send_hist(tickers, flds, dates, date_field, ovrds)

        data = self._parse_ref(flds, keep_corrId=True, sent_events=len(dates))
        data = pd.DataFrame(data)
        data.columns = ['ticker', 'field', 'value', 'date']
        data = data.sort_values(by='date').reset_index(drop=True)
        data = data.loc[:, ['date', 'ticker', 'field', 'value']]
        return data# }}}

    def bulkref_hist(self, tickers, flds, dates, ovrds=None,# {{{
                     date_field='REFERENCE_DATE'):
        """
        Make iterative calls to bulkref() and create a long DataFrame with
        columns [date, ticker, field, name, value, position] where each date
        corresponds to overriding a historical data override field.

        Parameters
        ----------
        tickers: {list, string}
            String or list of strings corresponding to tickers
        flds: {list, string}
            String or list of strings corresponding to FLDS
        dates: list
            list of date strings in the format YYYYmmdd
        ovrds: list of tuples
            List of tuples where each tuple corresponds to the override
            field and value. This should not include the date_field which will
            be iteratively overridden
        date_field: str
            Field to iteratively override for requesting historical data,
            e.g. REFERENCE_DATE, CURVE_DATE, etc.

        Example
        -------
        >>> import pdblp
        >>> con = pdblp.BCon()
        >>> con.start()
        >>> dates = ["20160625", "20160626"]
        >>> con.bulkref_hist("BVIS0587 Index", "CURVE_TENOR_RATES", dates,
        ...                  date_field="CURVE_DATE")

        """
        ovrds = [] if not ovrds else ovrds

        if type(tickers) is not list:
            tickers = [tickers]
        if type(flds) is not list:
            flds = [flds]
        self._send_hist(tickers, flds, dates, date_field, ovrds)
        data = self._parse_bulkref(flds, keep_corrId=True,
                                   sent_events=len(dates))
        data = pd.DataFrame(data)
        data.columns = ['ticker', 'field', 'name', 'value', 'position', 'date']
        data = data.sort_values(by=['date', 'position']).reset_index(drop=True)
        data = data.loc[:, ['date', 'ticker', 'field', 'name',
                            'value', 'position']]
        return data# }}}

    def _send_hist(self, tickers, flds, dates, date_field, ovrds):# {{{
        logger = _get_logger(self.debug)
        setvals = []
        request = self._create_req('ReferenceDataRequest', tickers, flds,
                                   ovrds, setvals)

        overrides = request.getElement('overrides')
        if len(dates) == 0:
            raise ValueError('dates must by non empty')
        ovrd = overrides.appendElement()
        for dt in dates:
            ovrd.setElement('fieldId', date_field)
            ovrd.setElement('value', dt)
            # CorrelationID used to keep track of which response coincides with
            # which request
            cid = blpapi.CorrelationId(dt)
            logger.info('Sending Request:\n{}'.format(request))
            self._session.sendRequest(request, identity=self._identity,
                                      correlationId=cid)# }}}

    def bdib(self, ticker, start_datetime, end_datetime, event_type, interval,# {{{
             elms=None):
        """
        Get Open, High, Low, Close, Volume, and numEvents for a ticker.
        Return pandas DataFrame

        Parameters
        ----------
        ticker: string
            String corresponding to ticker
        start_datetime: string
            UTC datetime in format YYYY-mm-ddTHH:MM:SS
        end_datetime: string
            UTC datetime in format YYYY-mm-ddTHH:MM:SS
        event_type: string {TRADE, BID, ASK, BID_BEST, ASK_BEST, BEST_BID,
                           BEST_ASK}
            Requested data event type
        interval: int {1... 1440}
            Length of time bars
        elms: list of tuples
            List of tuples where each tuple corresponds to the other elements
            to be set. Refer to the IntradayBarRequest section in the
            'Services & schemas reference guide' for more info on these values
        """
        elms = [] if not elms else elms

        # flush event queue in case previous call errored out
        logger = _get_logger(self.debug)
        while(self._session.tryNextEvent()):
            pass

        # Create and fill the request for the historical data
        request = self.refDataService.createRequest('IntradayBarRequest')
        request.set('security', ticker)
        request.set('eventType', event_type)
        request.set('interval', interval)  # bar interval in minutes
        request.set('startDateTime', start_datetime)
        request.set('endDateTime', end_datetime)
        for name, val in elms:
            request.set(name, val)

        logger.info('Sending Request:\n{}'.format(request))
        # Send the request
        self._session.sendRequest(request, identity=self._identity)
        # Process received events
        data = []
        flds = ['open', 'high', 'low', 'close', 'volume', 'numEvents']
        for msg in self._receive_events():
            d = msg['element']['IntradayBarResponse']
            for bar in d['barData']['barTickData']:
                data.append(bar['barTickData'])
        data = pd.DataFrame(data).set_index('time').sort_index().loc[:, flds]
        return data# }}}

    def bdit(self, ticker, start_datetime, end_datetime, event_type,# {{{
             elms=None, wide=False):
        """
        Get Open, High, Low, Close, Volume, and numEvents for a ticker.
        Return pandas DataFrame

        Parameters
        ----------
        ticker: string
            String corresponding to ticker
        start_datetime: string
            UTC datetime in format YYYY-mm-ddTHH:MM:SS
        end_datetime: string
            UTC datetime in format YYYY-mm-ddTHH:MM:SS
        event_type: string {TRADE, BID, ASK, BID_BEST, ASK_BEST, BEST_BID,
                           BEST_ASK}
            Requested data event type
        elms: list of tuples
            List of tuples where each tuple corresponds to the other elements
            to be set. Refer to the IntradayBarRequest section in the
            'Services & schemas reference guide' for more info on these values
        """
        elms = [] if not elms else elms
        if isinstance(event_type, str):
            event_type = [event_type]

        # flush event queue in case previous call errored out
        logger = _get_logger(self.debug)
        while(self._session.tryNextEvent()):
            pass

        # Create and fill the request for the historical data
        request = self.refDataService.createRequest('IntradayTickRequest')
        request.set('security', ticker)
        # request.append('eventTypes', event_type)
        events = request.getElement('eventTypes')
        for event in event_type:
            # request.append('eventTypes', event)
            events.appendValue(event)
        # request.set('interval', interval)  # bar interval in minutes
        request.set('startDateTime', start_datetime)
        request.set('endDateTime', end_datetime)
        for name, val in elms:
            request.set(name, val)

        logger.info('Sending Request:\n{}'.format(request))
        # Send the request
        self._session.sendRequest(request, identity=self._identity)
        # Process received events
        data = []
        # flds = ['open', 'high', 'low', 'close', 'volume', 'numEvents']
        for msg in self._receive_events():
            try:
                d = msg['element']['IntradayTickResponse']
                for tick in d['tickData']['tickData']:
                    data.append(tick['tickData'])
            except Exception as e:
                print(msg)
                raise(e)
        data = pd.DataFrame(data)
        if wide:
            data = data.set_index(['time', 'type'], append=True).unstack()
            data.columns = [str(p[1]).upper() + '_' + str(p[0]).upper() for p in data.columns]
            data = data.droplevel(0).sort_index()
        return data# }}}

    def bsrch(self, domain):# {{{
        """
        This function uses the Bloomberg API to retrieve 'bsrch' (Bloomberg
        SRCH Data) queries. Returns list of tickers.

        Parameters
        ----------
        domain: string
            A character string with the name of the domain to execute.
            It can be a user defined SRCH screen, commodity screen or
            one of the variety of Bloomberg examples. All domains are in the
            format <domain>:<search_name>. Example "COMDTY:NGFLOW"

        Returns
        -------
        data: pandas.DataFrame
            List of bloomberg tickers from the BSRCH
        """
        logger = _get_logger(self.debug)
        request = self.exrService.createRequest('ExcelGetGridRequest')
        request.set('Domain', domain)
        logger.info('Sending Request:\n{}'.format(request))
        self._session.sendRequest(request, identity=self._identity)
        data = []
        for msg in self._receive_events(to_dict=False):
            for v in msg.getElement("DataRecords").values():
                for f in v.getElement("DataFields").values():
                    data.append(f.getElementAsString("StringValue"))
        return pd.DataFrame(data)# }}}

    def stop(self):# {{{
        """
        Close the blp session
        """
        self._session.stop()# }}}

def _element_to_dict(elem):# {{{
    if isinstance(elem, str):
        return elem
    dtype = elem.datatype()
    if dtype == blpapi.DataType.CHOICE:
        return {str(elem.name()): _element_to_dict(elem.getChoice())}
    elif elem.isArray():
        return [_element_to_dict(v) for v in elem.values()]
    elif dtype == blpapi.DataType.SEQUENCE:
        return {str(elem.name()): {str(e.name()): _element_to_dict(e) for e in elem.elements()}}  # NOQA
    else:
        if elem.isNull():
            value = None
        else:
            try:
                value = elem.getValue()
            except:  # NOQA
                value = None
        return value# }}}

def message_to_dict(msg):# {{{
    return {
        'correlationIds': [cid.value() for cid in msg.correlationIds()],
        'messageType': "{}".format(msg.messageType()),
        'topicName': msg.topicName(),
        'element': _element_to_dict(msg.asElement())
    }# }}}

def _parse(mystr):# {{{

    LBRACE, RBRACE, EQUAL = map(pp.Suppress, "{}=")
    field = pp.Word(pp.printables + ' ', excludeChars='[]=')
    field.addParseAction(pp.tokenMap(str.rstrip))
    string = pp.dblQuotedString().setParseAction(pp.removeQuotes)
    number = pp.pyparsing_common.number()
    date_expr = pp.Regex(r'\d\d\d\d-\d\d-\d\d')
    time_expr = pp.Regex(r'\d\d:\d\d:\d\d\.\d\d\d')
    nan = pp.Keyword('nan')
    scalar_value = (string | date_expr | time_expr | number | nan)

    list_marker = pp.Suppress("[]")
    value_list = pp.Forward()
    jobject = pp.Forward()

    memberDef1 = pp.Group(field + EQUAL + scalar_value)
    memberDef2 = pp.Group(field + EQUAL + jobject)
    memberDef3 = pp.Group(field + list_marker + EQUAL + LBRACE + value_list +
                          RBRACE)
    memberDef = memberDef1 | memberDef2 | memberDef3

    value_list <<= (pp.delimitedList(scalar_value, ",") |
                    pp.ZeroOrMore(pp.Group(pp.Dict(memberDef2))))
    value_list.setParseAction(lambda t: [pp.ParseResults(t[:])])

    members = pp.OneOrMore(memberDef)
    jobject <<= pp.Dict(LBRACE + pp.ZeroOrMore(memberDef) + RBRACE)
    # force empty jobject to be a dict
    jobject.setParseAction(lambda t: t or {})

    parser = members
    parser = pp.OneOrMore(pp.Group(pp.Dict(memberDef)))

    return parser.parseString(mystr)# }}}

def to_dict_list(mystr):# {{{
    """
    Translate a string representation of a Bloomberg Open API Request/Response
    into a list of dictionaries.return

    Parameters
    ----------
    mystr: str
        A string representation of one or more blpapi.request.Request or
        blp.message.Message, these should be '\\n' seperated
    """
    res = _parse(mystr)
    dicts = []
    for res_dict in res:
        dicts.append(res_dict.asDict())
    return dicts# }}}

def to_json(mystr):# {{{
    """
    Translate a string representation of a Bloomberg Open API Request/Response
    into a JSON string

    Parameters
    ----------
    mystr: str
        A string representation of one or more blpapi.request.Request or
        blp.message.Message, these should be '\\n' seperated
    """
    dicts = to_dict_list(mystr)
    json.dumps(dicts, indent=2)# }}}

def custom_req(session, request):# {{{
    """
    Utility for sending a predefined request and printing response as well
    as storing messages in a list, useful for testing

    Parameters
    ----------
    session: blpapi.session.Session
    request: blpapi.request.Request
        Request to be sent

    Returns
    -------
        List of all messages received
    """
    # flush event queue in case previous call errored out
    while(session.tryNextEvent()):
        pass

    print("Sending Request:\n %s" % request)
    session.sendRequest(request)
    messages = []
    # Process received events
    while(True):
        # We provide timeout to give the chance for Ctrl+C handling:
        ev = session.nextEvent(500)
        for msg in ev:
            print("Message Received:\n %s" % msg)
            messages.append(msg)
        if ev.eventType() == blpapi.Event.RESPONSE:
            # Response completely received, so we could exit
            break
    return messages# }}}

    def fill_element(self, element, value):# {{{
        """Fill a BLPAPI element from native python types.

        This function maps dict-like objects (mappings), list-like
        objects (iterables) and scalar values to the BLPAPI element
        structure and fills `element` with the structure given
        in `value`.
        """

        def prepare_value(value):
            if isinstance(value, datetime.datetime):
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



# extending pandas dataframe to make methods easily accessible
@pd.api.extensions.register_series_accessor('bbg')
class BBG:
    def __init__(self, pandas_obj):
        self._obj = pandas_obj

    def bdp(self, fields, ovrds=None):# {{{

        con = BCon(timeout=50000)
        con.start()

        # getting a df from the column

        df = self._obj.to_frame(name='ticker')

        data = con.ref(df['ticker'].to_list(), flds=fields, ovrds=ovrds) \
            .drop_duplicates() \
            .pivot(index='ticker', columns='field', values='value') \
            .reset_index()


        df = df.merge(data, how='left', left_on='ticker', right_on='ticker')
        con.stop()
        return df[[fields]]# }}}

    def bdi(self, field, agg_func, start_date, end_date, ovrds=[], fillna=0):# {{{

        con = BCon(timeout=50000)
        con.start()

        df = self._obj.to_frame(name='ticker')

        all_ovrds = [
            ('MARKET_DATA_OVERRIDE', field),
            ('START_DATE_OVERRIDE', pd.to_datetime(start_date).strftime('%Y%m%d')),
            ('END_DATE_OVERRIDE', pd.to_datetime(end_date).strftime('%Y%m%d'))
        ] + ovrds

        x = con.ref(df['ticker'].to_list(), 'INTERVAL_' + agg_func.upper(), ovrds=all_ovrds) \
        .drop_duplicates() \
        .pivot(index='ticker', columns='field', values='value') \
        .reset_index()

        x = x.rename(columns={'INTERVAL_' + agg_func.upper(): field})
        x[field] = x[field].fillna(fillna)

        df = df.merge(x, how='left', left_on='ticker', right_on='ticker')


        con.stop()

        return df[[field]]# }}}

def _find_bbg_excel(pid=None):# {{{
    """function to try determine which excel instance has a fully working bloomberg"""

    if pid:
        return pid

    if len(xw.apps) == 0:
        raise Exception("You need a running instance of excel with the bbg add-in loaded and working")

    app_pid = _test_bbg_addin(xw.apps)

    if not app_pid:
        raise Exception("You need a running instance of excel with the bbg add-in loaded and working")

    return app_pid# }}}

def _test_bbg_addin(apps):# {{{
    """function to test the bloomberg add-in with a basic formula"""
    for app in apps:
        bk = app.books.add()
        sh = bk.sheets[0]
        txt = '=BDP("JPST LN Equity", "FUND_ASSET_CLASS_FOCUS")'
        sh.range('a1').value = txt
        time.sleep(1)
        val = sh.range('a1').value
        bk.close()
        if val == 'Fixed Income':
            return app.pid
    return False# }}}

def get_etf_memb(ticker):# {{{
    # first get the running excel instance

    global __glob_pid
    pid = _find_bbg_excel(__glob_pid)

    try:
        app = xw.apps[pid]
        __glob_pid = pid

    except:

        try:
            pid = _find_bbg_excel(None)
            app = xw.apps[pid]
            __glob_pid = pid
        except:
            raise Exception(r"Excel Instance with PID {pid} has been closed")

        raise Exception(r"Excel Instance with PID {pid} has been closed")


    bk = app.books.add()
    sh = bk.sheets[0]
    txt = f'''=BQL("members('{ticker}', type=HOLDINGS)","ID().WEIGHTS, ID_ISIN()")'''
    sh.range('A1').value = txt
    i = sh.cells(1,1)
    for h in range(10):
        if sh.range('A1').value == '#N/A Requesting Data...':
            time.sleep(0.5)
    i = sh.range('a1').expand('right').expand('down')
    hds = i.options(pd.DataFrame).value
    hds = hds.reset_index()
    hds.columns = ['bbg_ticker', 'weight', 'isin']
    hds['weight'] = hds['weight'] / 100
    bk.close()
    return hds# }}}

# ticker = 'JPST LN Equity'{{{

# app = xw.apps[33196]
# for app in xw.apps:
#     for addin in list(app.api.COMAddIns):
#         print(f'''APP: {app.pid} addin: {addin.Description} ({addin.ProgId}) ''')
# 
# for app in xw.apps:
#     for addin in list(app.api.AddIns):
#         print(f'''APP: {app.pid} addin: {addin.Name} ({addin.IsOpen}) ''')
# 
# 
# addin = r"C:\Program Files (x86)\BLP\API\Office Tools\BloombergUI.xla"
# comm =  r"C:\Program Files (x86)\BLP\API\Office tools\bofaddin.dll"
# app = xw.App(visible=True)
# 
# # app.api.RegisterXLL(addin)
# a = app.api.AddIns.Add(addin)
# c = app.api.AddIns.Add(comm)
# 
# a.Installed = True
# c.Installed = True
# 
# ticker = 'JPST LN Equity'
# 
# bk = app.books.add()
# sh = bk.sheets[0]
# txt = f'''=BQL("members('{ticker}', type=HOLDINGS)","ID().WEIGHTS, ID_ISIN()")'''
# sh.range('A1').value = txt
# i = sh.cells(1,1)
# i = sh.range('a1').expand('right').expand('down')
# hds = i.options(pd.DataFrame).value
# 
#     found = False
#     for app in xw.apps:
#         for addin in list(app.api.AddIns):
#             if (addin.Name == 'BloombergUI.xla') and (addin.IsOpen):
#                 found = app.pid
#                 break
#         if found:
#             break
#     if not found:
#         raise Exception('No running excel instance with excel addin found')}}}



# easy and intuitive functions that mirror excel

def _BDP(tickers, field, **field_ovrds):# {{{

    if isinstance(tickers, str):
        tickers = [tickers]

    trans_tickers = _parse_tickers(tickers)


    ovrds2 = {}
    numb_tickers = len(trans_tickers)

    # this is the easiest way to deal with situations where we don't give overrides
    field_ovrds['dummy'] = 'dummy'

    #import pdb; pdb.set_trace()
    for k, v in field_ovrds.items():
        if (not isinstance(v, collections.abc.Sequence) and not isinstance(v, pd.Series)) or isinstance(v, str):
            ovrds2[k] = [v] * numb_tickers
        else:
            if len(v) != numb_tickers:
                raise ValueError(f"Invalid number of overrides specified for {k}")
            ovrds2[k] = v

    params_df = pd.DataFrame({'trans_ticker': trans_tickers})
    for k, v in ovrds2.items():
        params_df[k] = v

    ovrd_names = list(ovrds2.keys())

    params_df = params_df.groupby(ovrd_names)['trans_ticker'].agg('unique').reset_index()

    con = BCon(timeout=50000)
    con.start()
    all_data = pd.DataFrame()
    for row in params_df.to_dict(orient='records'):
        kwargs = []
        for col in ovrd_names:
            if col != 'dummy':
                kwargs.append((col, row[col]))
        data = con.ref(list(row['trans_ticker']), field, kwargs)
        for col in ovrd_names:
            data[col] = row[col]
        all_data = all_data.append(data, ignore_index=True)
    con.stop()
    all_data = all_data.rename(columns={'ticker': 'trans_ticker'})

    inp_df = pd.DataFrame({'trans_ticker': trans_tickers, 'orig_ticker': tickers})


    for k, v in ovrds2.items():
        inp_df[k] = v

    inp_df = inp_df.merge(all_data, how='left', validate='m:1', on=['trans_ticker'] + ovrd_names)

    # if len(data['ticker']) != len(tickers):
    #         raise ValueError('uneven length of response tickers to request tickers')

    # data['ticker'] = tickers.tolist()

    if inp_df.shape[0] != len(tickers):
        raise ValueError('uneven length of response tickers to request tickers')

    # dealing with single datapoint scenario
    # if len(tickers) == 1:
    #     return inp_df['value'].tolist()[0]

    output = inp_df['value'].tolist()

    if isinstance(tickers, pd.Series):
        output = pd.Series(output)
        output.index = tickers.index

    return output
    # return inp_df['value'].tolist()
    # return data['value'].tolist()# }}}

def _BDH(tickers, field, start_date, end_date, cdr=None, fx=None, fill='B', # {{{
        usedpdf=True, period='D', **field_ovrds):

    if isinstance(tickers, str):
        tickers = [tickers]

    if isinstance(start_date, str):
        start_date = [start_date]

    if isinstance(end_date, str):
        end_date = [end_date]

    if isinstance(cdr, str):
        cdr = [cdr]

    if isinstance(fill, str):
        fill = [fill]

    if isinstance(fx, str):
        fx = [fx]

    if isinstance(usedpdf, bool):
        usedpdf = [usedpdf]

    if isinstance(period, str):
        period = [period]

    trans_tickers = _parse_tickers(tickers)

    elms_dict1 = {}

    numb_tickers = len(trans_tickers)

    clean_fill = []
    for f in fill:
        if f.upper() == 'B':
            f = 'NIL_VALUE'
        elif f.upper() == 'P':
            f = 'PREVIOUS_VALUE'
        else:
            raise ValueError(f'Fill must be B or P, not {fill}')
        clean_fill.append(f)

    elms_dict1['nonTradingDayFillMethod'] = clean_fill

    elms_dict1['adjustmentFollowDPDF'] = usedpdf

    if fx:
        elms_dict1['currency'] = fx

    elms_dict1['start_date'] = start_date
    elms_dict1['end_date'] = end_date


    per_lookup = {'D': 'DAILY', 'W': 'WEEKLY', 'M': 'MONTHLY', 'S': 'SEMI_ANNUALLY',
            'Q': 'QUARTERLY', 'A': 'YEARLY'}

    clean_period = []
    for p in period:
        p = per_lookup[p.upper()]
        clean_period.append(p)

    elms_dict1['periodicitySelection'] = clean_period

    if (period == 'DAILY') and cdr:
        clean_cdr = []
        for c in cdr:
            clean_cdr.append(c.upper())
        elms_dict1['calendarCodeOverride'] = clean_cdr #cdr only works with daily

    # elms_dict1['nonTradingDayFillOption'] = 'ACTIVE_DAYS_ONLY'

    # import pdb; pdb.set_trace()

    elms_dict = {}
    for k, v in elms_dict1.items():
        if isinstance(v, collections.abc.Sequence) and len(v) == 1:
            elms_dict[k] = v * numb_tickers
        elif (not isinstance(v, collections.abc.Sequence) and not isinstance(v, pd.Series)) or isinstance(v, str):
            elms_dict[k] = [v] * numb_tickers
        else:
            if len(v) != numb_tickers:
                raise ValueError(f"Invalid number of overrides specified for {k}")
            elms_dict[k] = v


    ovrds2 = {}

    # this is the easiest way to deal with situations where we don't give overrides
    field_ovrds['dummy'] = 'dummy'

    #import pdb; pdb.set_trace()
    for k, v in field_ovrds.items():
        if (not isinstance(v, collections.abc.Sequence) and not isinstance(v, pd.Series)) or isinstance(v, str):
            ovrds2[k] = [v] * numb_tickers
        else:
            if len(v) != numb_tickers:
                raise ValueError(f"Invalid number of overrides specified for {k}")
            ovrds2[k] = v

    params_df = pd.DataFrame({'trans_ticker': trans_tickers})
    for k, v in ovrds2.items():
        params_df[k] = v

    for k, v in elms_dict.items():
        params_df[k] = v

    ovrd_names = list(ovrds2.keys())
    elms_names = list(elms_dict.keys())
    all_names = ovrd_names + elms_names

    params_df = params_df.groupby(all_names)['trans_ticker'].agg('unique').reset_index()

    con = BCon(timeout=50000)
    con.start()
    all_data = pd.DataFrame()
    for row in params_df.to_dict(orient='records'):
        kwargs = []
        final_ovrds = []
        final_elms = []
        for col in all_names:
            if col in ['dummy', 'start_date', 'end_date']:
                continue
            elif col in elms_names:
                final_elms.append((col, row[col]))
            elif col in ovrd_names:
                final_ovrds.append((col, row[col]))

        # print(final_elms)
        # print(final_ovrds)
        data = con.bdh(list(row['trans_ticker']), start_date=row['start_date'],
                end_date=row['end_date'], flds=field, elms=final_elms, ovrds=final_ovrds,
                longdata=True)
        for col in all_names:
            data[col] = row[col]
        all_data = all_data.append(data, ignore_index=True)
    con.stop()
    all_data = all_data.rename(columns={'ticker': 'trans_ticker'})

    inp_df = pd.DataFrame({'trans_ticker': trans_tickers, 'orig_ticker': list(tickers)})

    for k, v in ovrds2.items():
        inp_df[k] = v

    for k, v in elms_dict.items():
        inp_df[k] = v

    # if all_data.shape[0] < 1:
    #     raise ValueError(f'no data returned in query, {all_data}, {inp_df}')

    if isinstance(tickers, pd.Series):
        inp_df['index'] = list(tickers.index)
        # print(inp_df['index'])
        # print(inp_df)
        # inp_df = inp_df.merge(params_ind_df, how='left', validate='m:m', on=['trans_ticker'] + ovrd_names + elms_names)
        # inp_df = inp_df.drop(columns = ovrd_names + elms_names)

    inp_df = inp_df.merge(all_data, how='left', validate='m:m', on=['trans_ticker'] + ovrd_names + elms_names)


    # if inp_df.shape[0] and (len(tickers)==1):
    # for when a single data point is requested

    output = inp_df

    if list(start_date) == list(end_date):
        if isinstance(tickers, pd.Series):
            output =  inp_df['value']
        else:
            output =  inp_df['value'].tolist()
            return output

    if isinstance(tickers, pd.Series):
        if isinstance(output, pd.Series):
            output.index = inp_df['index']
        else:
            output = output.set_index(output['index'])

    output = output[['orig_ticker', 'date', 'value']]
    output = output.rename(columns={'orig_ticker': 'ticker', 'value': field})


    return output# }}}

def _BEQS(eqs_screen_name, as_of_date='today'):# {{{

    con = BCon(timeout=1000000)
    con.start()
    tickers = con.beqs(eqs_screen_name, date=as_of_date)
    con.stop()

    tickers = [t + ' Equity' for t in tickers]

    return tickers# }}}

def _BDS(tickers, field, **field_ovrds):# {{{

    trans_tickers = _parse_tickers(tickers)

    ovrds2 = {}
    numb_tickers = len(trans_tickers)

    # this is the easiest way to deal with situations where we don't give overrides
    field_ovrds['dummy'] = 'dummy'

    #import pdb; pdb.set_trace()
    for k, v in field_ovrds.items():
        if (not isinstance(v, collections.abc.Sequence) and not isinstance(v, pd.Series)) or isinstance(v, str):
            ovrds2[k] = [v] * numb_tickers
        else:
            if len(v) != numb_tickers:
                raise ValueError(f"Invalid number of overrides specified for {k}")
            ovrds2[k] = v

    params_df = pd.DataFrame({'trans_ticker': trans_tickers})
    for k, v in ovrds2.items():
        params_df[k] = v

    ovrd_names = list(ovrds2.keys())

    params_df = params_df.groupby(ovrd_names)['trans_ticker'].agg('unique').reset_index()

    con = BCon(timeout=50000)
    con.start()
    all_data = pd.DataFrame()
    for row in params_df.to_dict(orient='records'):
        kwargs = []
        for col in ovrd_names:
            if col != 'dummy':
                kwargs.append((col, row[col]))
        data = con.bulkref(list(row['trans_ticker']), field, kwargs)
        for col in ovrd_names:
            data[col] = row[col]
        all_data = all_data.append(data, ignore_index=True)
    con.stop()
    all_data = all_data.rename(columns={'ticker': 'trans_ticker'})


    inp_df = pd.DataFrame({'trans_ticker': trans_tickers, 'orig_ticker': tickers})

    for k, v in ovrds2.items():
        inp_df[k] = v

    inp_df = inp_df.merge(all_data, how='left', validate='m:m', on=['trans_ticker'] + ovrd_names)

    kp = inp_df.columns.tolist()
    kp = [x for x in inp_df.columns.tolist() if x not in ['name', 'value']]

    inp_df = inp_df.pivot_table(index=kp, columns='name', values='value',
            aggfunc=lambda x: x).reset_index()

    inp_df = inp_df.drop(columns=['trans_ticker', 'dummy'])
    inp_df = inp_df.rename(columns={'orig_ticker': 'ticker'})

    # if len(data['ticker']) != len(tickers):
    #         raise ValueError('uneven length of response tickers to request tickers')

    # data['ticker'] = tickers.tolist()

    # if inp_df.shape[0] != len(tickers):
    #    raise ValueError('uneven length of response tickers to request tickers')


    # return inp_df['value'].tolist()
    return inp_df
    # return data['value'].tolist()# }}}

def _BDIT(tickers, events, sd=None, ed=None, cond_codes=False, qrm=False, #{{{
        action_codes=False, exch_codes=False, broker_codes=False,
        indicator_codes=False, trade_time=True):

    start_date = pd.to_datetime(sd, format='%Y%m%d %H:%M:%S').strftime('%Y-%m-%dT%H:%M:%S')
    end_date = pd.to_datetime(ed, format='%Y%m%d %H:%M:%S').strftime('%Y-%m-%dT%H:%M:%S')

    if isinstance(tickers, str):
        tickers = [tickers]

    trans_tickers = _parse_tickers(tickers)

    elms = []

    numb_tickers = len(trans_tickers)

    elms.append(('includeConditionCodes', cond_codes))
    elms.append(('includeNonPlottableEvents', qrm))
    elms.append(('includeActionCodes', action_codes))
    elms.append(('includeExchangeCodes', exch_codes))
    elms.append(('includeIndicatorCodes', indicator_codes))
    elms.append(('includeBrokerCodes', broker_codes))
    elms.append(('includeTradeTime', trade_time))

    data = pd.DataFrame(columns=['ticker', 'location', 'time', 'type', 'value'])

    con = BCon(timeout=50000)
    con.start()

    for i, ticker in enumerate(trans_tickers):
        dat = con.bdit(ticker, event_type=events, start_datetime=start_date, end_datetime=end_date, elms=elms)
        if len(dat) == 0:
            continue
        dat['ticker'] = ticker
        dat['location'] = i
        # giving the return dataframe timezone information
        dat['time'] = dat['time'].dt.tz_localize('utc')

        data = data.append(dat, ignore_index=True, sort=True)

    con.stop()

    data = data.rename(columns={'ticker': 'trans_ticker'})

    inp_df = pd.DataFrame({'trans_ticker': trans_tickers, 'orig_ticker': tickers})
    inp_df['location'] = np.arange(len(inp_df))

    inp_df = inp_df.merge(data, how='left', validate='m:m', on=['trans_ticker', 'location'])
    inp_df = inp_df.drop(columns=['location','trans_ticker'])
    inp_df['type'] = inp_df['type'].astype('str')
    inp_df = inp_df.drop_duplicates(subset=['orig_ticker', 'time', 'type', 'value'])

    inp_df = inp_df.rename(columns={'orig_ticker': 'ticker',
        'tradeTime': 'trade_time',
        'conditionCodes': 'cond_codes',
        'exchangeCode': 'exch_codes',
        'actionCodes': 'action_codes'})
    if 'trade_time' in inp_df.columns.tolist():
        inp_df['trade_time'] = inp_df['trade_time'].dt.tz_localize('utc')

    return inp_df# }}}

def _SECF(queries, filt=None, max_results=10):# {{{

    if isinstance(queries, str):
        queries = [queries]

    queries = list(set(queries))

    data = pd.DataFrame()
    con = BCon(timeout=500000)
    con.start()
    for q in queries:
        dat = con.secf(q, yk_filter=filt, max_results=max_results)
        dat['search'] = q
        data = data.append(dat)
    con.stop()

    return data# }}}

def _BBAT(tickers, sd, ed=None, inav=True, fair_value=None, qrm=True, summary=False):# {{{

    if isinstance(tickers, str):
        tickers = [tickers]

    if ed == None:
        ed = sd

    start = pd.to_datetime(sd, format='%Y%m%d')
    end = pd.to_datetime(ed, format='%Y%m%d')

    dates = pd.date_range(start, end)
    dates = [x.strftime('%Y%m%d') for x in dates]

    df = pd.DataFrame()
    for ticker in tickers:
        for date in dates:
            tmp = _bbat(ticker=ticker, date=date, inav=inav, fair_value=fair_value, qrm=qrm, summary=summary)
            if isinstance(tmp, pd.DataFrame):
                df = df.append(tmp, ignore_index=True)

    if df.shape == (0, 0):
        raise ValueError("No data found for the period")

    return df# }}}

def _BCDE(df):# {{{
    con = BCon(timeout=500000)
    con.start()
    _run_df_upload(con, df)
    # }}}

def _HDS(tickers):# {{{

    hds = _BDS(tickers, 'ALL_HOLDERS_PUB_FILINGS_WITH_TKR', all_hldrs_pub_filings_view_ovr='DETAILED_MULTI')
    cst = _BDS(tickers, 'ALL_HOLDERS_COST_BASIS', all_hldrs_pub_filings_view_ovr='DETAILED_MULTI')

    hds.rename(columns={'Ticker': 'client_ticker', 'Position': 'shs_held'}, inplace=True)
    hds.columns = [x.strip().replace(' ', '_').lower() for x in hds.columns.tolist()]
    cst.columns = [x.strip().replace(' ', '_').lower() for x in cst.columns.tolist()]

    cols = ['all_hldrs_pub_filings_view_ovr', 'field', 'position', 'filing_date',
            'holder_name', 'institution_type', 'portfolio_name']

    cst = cst.drop(columns=cols)

    hds = hds.merge(cst, how='left', on=['ticker', 'holder_id', 'portfolio_number'], validate='1:1')

    hds['nav'] = _BDP(hds['ticker'], 'fund_net_asset_val', nav_crncy='USD')
    hds['position_usd'] = hds['nav'] * hds['shs_held']
    hds['share_class_aum'] = _BDP(hds['ticker'], 'FUND_CRNCY_ADJ_CLASS_ASSETS', FUND_CLASS_ASSETS_CRNCY='USD')
    hds['share_class_aum'] = hds['share_class_aum'] * 1000000
    hds['pcn_of_aum'] = hds['position_usd'] / hds['share_class_aum']

    hds = hds.drop(columns=['all_hldrs_pub_filings_view_ovr', 'field', 'percent_outstanding',
        'market_value', 'nav', 'position'])

    return hds# }}}

def _BQL(universe=None, expression=None, query=None, show_dates=None, show_headers=None, # {{{
        show_query=None, show_ids=None, transpose=None,
        sort_dates_desc=None, group_by_fields=None, show_all_cols=None):

    sh = create_excel_app()
    try:
        data = run_bql(sh, universe=universe, expression=expression, query=query, show_dates=show_dates,
                show_headers=show_headers, show_query=show_query, show_ids=show_ids,
                transpose=transpose, sort_dates_desc=sort_dates_desc, group_by_fields=group_by_fields,
                show_all_cols=show_all_cols)
    except Exception as e:
        raise
    finally:
        # always close the running excel instance
        sh.book.app.kill()
    return data# }}}

def _MEMB(tickers, all_cols=False, reweight=False, add_cash=False, valid_reweight=False):# {{{

    if isinstance(tickers, str):
        tickers = [tickers]

    # unique values only
    tickers = list(set(tickers))
    tst = [x.lower().strip() for x in tickers]
    if len(set(tst)) != len(tst):
        raise ValueError("Duplicate securities in query")

    etfs = [x for x in tickers if 'equity' in x.lower()]
    indexes = [x for x in tickers if 'index' in x.lower()]

    df = pd.DataFrame()

    univ = "members('{ticker}', {opt})"

    for etf in etfs:
        univ = f"members('{etf}', type=HOLDINGS)"
        expression = "id"
        tmp = _BQL(univ, expression, show_headers=True, show_all_cols=True)
        if not isinstance(tmp, pd.DataFrame):
            raise Exception(f'dataframe not returned. Message received was: {tmp}')
        if 'id.ORIG_IDS' not in tmp.columns.tolist():
            raise Exception(f'dataframe not returned. Message received was: {tmp}')
        df = df.append(tmp, ignore_index=True, sort=True)

    if indexes:
        if len(indexes) == 1:
            ind_str = f"'{indexes[0]}'"
        else:
            indexes = ["'" + i + "'" for i in indexes]
            ind_str = ','.join(indexes)
            ind_str = '[' + ind_str + ']'
        univ = f"members({ind_str})"
        expression = "id"
        tmp = _BQL(univ, expression, show_headers=True, show_all_cols=True)

        if not isinstance(tmp, pd.DataFrame):
            raise Exception(f'dataframe not returned. Message received was: {tmp}')
        if 'id.ORIG_IDS' not in tmp.columns.tolist():
            raise Exception(f'dataframe not returned. Message received was: {tmp}')

        tmp = tmp.rename(columns={'id.Positions': 'id.POSITION', 'id.Weights': 'id.WEIGHTS'})
        df = df.append(tmp, ignore_index=True, sort=True)

    df = df.rename(columns=
            {'id.ORIG_IDS': 'portfolio_ticker',
                'id': 'ticker',
                'id.POSITION': 'position',
                'id.WEIGHTS': 'weight',
                'id.AS_OF_DATE': 'as_of_date',
                'id.CURRENCY': 'currency',
                'id.REPORTED_MKT_VAL': 'reported_mkt_val',
                'id.LATEST_CHANGE': 'position_change'})

    df['weight'] = df['weight'] / 100

    df = df.drop(columns=['ID'])

    if not all_cols:
        try:
            df = df.drop(columns=['as_of_date', 'currency', 'reported_mkt_val', 'position_change'])
        except:
            pass

    if add_cash and reweight:
        raise ValueError("Cannot both add cash and reweight portfolio")

    if add_cash:
        totals = df.groupby('portfolio_ticker', as_index=False)['weight'].sum()
        totals['cash_pcn'] = (1 - totals['weight'])

        for i, row in totals.iterrows():
            if round(row['weight'], 3) != 1:
                df = df.append({'portfolio_ticker': row['portfolio_ticker'],
                    'weight': row['cash_pcn'],
                    'ticker': 'Cash Position'}, ignore_index=True)

    if reweight:
        totals = df.groupby('portfolio_ticker', as_index=False)['weight'].sum()
        totals['factor'] = (1 / totals['weight'])
        totals = totals.drop(columns=['weight'])
        df = df.merge(totals, how='left', on='portfolio_ticker', validate='m:1')
        # print(df.columns)
        df['reweight'] = df['weight'] * df['factor']
        df = df.drop(columns=['factor'])
        # df = df.rename(columns={'new_weight', 'weight'})

    if valid_reweight:
        if reweight:
            col = 'reweight'
        else:
            col = 'weight'
        df['identifier'] = _BDP(df['ticker'], 'PARSEKYABLE_DES_SOURCE')
        df['valid_reweight'] = df[col] * ( 1 / df.loc[df['identifier']!='security not found', col].sum())
        df.loc[df['identifier']=='security not found', 'valid_reweight'] = 0


    return df# }}}

@lru_cache(maxsize=1000)
def _EPRX(tickers, subset=None, decomp=False):# {{{

    exch = re.compile(r" [A-Za-z0-9]{2} ")
    data = load_exchange_data()

    comps = ['GR', 'RM', 'BZ', 'UZ', 'SW', 'EY', 'AR',
            'CB', 'VC', 'ED', 'MM', 'US', 'CI', 'CN',
            'UH', 'VN', 'PA', 'IN', 'KS', 'CH', 'JP',
            'IR', 'AU', 'EU', 'GA', 'RO', 'DC',
            'SM', 'RU', 'CP', 'LR', 'SS', 'CZ']


    if not subset:
        subset = data.columns.tolist()

    if isinstance(tickers, str):
        if len(tickers) == 2:
            loc = [tickers.upper()]
        else:
            loc = exch.findall(tickers)
        if not loc:
            raise ValueError(f'Could not derive exchange from {tickers}')

        loc = loc[0].strip().upper()

        if (loc in comps) and decomp:
            loc = _BDP(tickers, 'EQY_PRIM_EXCH_SHRT')[0]
            print(f'composite security given - using primary exchange for  {tickers}')


        data = data.loc[data['bbg_exch_code']==loc]


        if isinstance(subset, str) or (len(subset)==1):
            if len(data) == 0:
                raise ValueError(f'No exchange data found for {tickers}')
            return data[subset].iloc[0]
        elif subset:
            data = data[subset]


        if len(data) == 0:
            raise ValueError(f'No exchange data found for {tickers}')
        return data.to_dict(orient='records')[0]


    res = pd.DataFrame()
    for ticker in tickers:
        if len(ticker)==2:
            loc = [ticker.upper()]
        else:
            loc = exch.findall(ticker)
        if not loc:
            raise ValueError(f'Could not derive exchange from {tickers}')

        loc = loc[0].strip().upper()

        if (loc in comps) and decomp:
            loc = _BDP(ticker, 'EQY_PRIM_EXCH_SHRT')[0]
            print(f'composite security given - using primary exchange for  {ticker}')

        tmp = data.loc[data['bbg_exch_code']==loc].copy()

        if len(data) == 0:
            raise ValueError(f'No exchange data found for {ticker}')

        res = res.append(tmp, ignore_index=True)

    if isinstance(subset, str) or (len(subset)==1):
        return res[subset].tolist()
    elif subset:
        res = res[subset]

    res['ticker'] = tickers

    res = res[['ticker'] + res.columns.tolist()[0:-1]]

    return res# }}}

@lru_cache(maxsize=32)
def load_exchange_data(cache=True):# {{{
    path = Path(__file__).parent.absolute() / 'exchanges.xlsx'
    df = pd.read_excel(path, sheet_name='exchange_data',
            converters={'equity_market_open': str, 'equity_market_close': str})

    return df# }}}

def _parse_tickers(tickers):# {{{

    if isinstance(tickers, str):
        tickers = [tickers]

    new_tick = []
    tickers = [str(t).upper().strip() for t in tickers]
    isin = re.compile(r"^[A-Z]{2}([A-Z0-9]){9}[0-9]")
    exch = re.compile(r" [A-Z0-9]{2} ")
    sedol = re.compile(r"^[A-Z0-9]{7}(?:@|\s)")

    for ticker in tickers:
        if isin.match(ticker):
            # i = isin.match(ticker)[0][:-1]
            i = ticker.split()[0]
            # i = [str(x) for x in i] # not sure if this is needed now
            e = exch.findall(ticker)
            if e:
                e = ' ' + e[0].strip()
            else:
                e = ''
            new_tick.append(f'/isin/{i}{e}')

        elif ' SEDOL' in ticker:
            # s = sedol.match(ticker)[0][:-1]
            s = ticker.split()[0]
            # s = [str(x) for x in s]
            e = exch.findall(ticker)
            if e:
                e = ' ' + e[0].strip()
            else:
                e = ''
            new_tick.append(f'/sedol/{s}{e}')
        else:
            new_tick.append(ticker)


    return new_tick# }}}

def _bbat(ticker, date=None, inav=True, fair_value=None, qrm=True, summary=False):# {{{
    """
    unvectorised version of the function
    Bloomberg Bid, Ask Trade data. This pivots intraday bid, ask and spread
    data to provide common statistics and filter out trading periods
    """

    if not isinstance(ticker, str):
        raise ValueError(f'ticker must be string not {type(ticker)}')

    # by default, will query today
    if not date:
        date = pd.to_datetime('today').strftime('%Y%m%d')


    df = pd.DataFrame()
    inav_df = pd.DataFrame()

    tz_info = _EPRX(ticker, 'pytz_timezone')
    # open and close are given in local market time
    opn = _EPRX(ticker, 'equity_market_open')
    cls = _EPRX(ticker, 'equity_market_close')

    # assumption is that no exchange will have overnight session in local timezone
    start_date = pd.to_datetime(date, format='%Y%m%d').tz_localize(tz_info).tz_convert('utc').strftime('%Y%m%d %H:%M:%S')
    end_date = (pd.to_datetime(date, format='%Y%m%d') + pd.Timedelta(hours=23, minutes=59, seconds=59)) \
            .tz_localize(tz_info).tz_convert('utc') \
            .strftime('%Y%m%d %H:%M:%S')

    events = ['BEST_BID', 'BEST_ASK', 'TRADE']
    try:
        data = _BDIT(ticker, events=events, sd=start_date, ed=end_date, cond_codes=True, exch_codes=False, qrm=qrm)
    except Exception as e:
        print(e)
        return None

    evts = data['type'].unique().tolist()
    if ('BEST_BID' not in evts) or ('BEST_ASK' not in evts):
        print(f'No data found for {date}')
        return None

    if 'trade_time' not in data.columns.tolist():
        data['trade_time'] = None
    else:
        data['trade_time'] = data['trade_time'].dt.tz_convert(tz_info)
    data['time'] = data['time'].dt.tz_convert(tz_info)

    data.loc[~data['trade_time'].isna(), 'time'] = data['trade_time']

    data = data.drop_duplicates()

    data = data.loc[
            (data['time'].dt.time >= pd.to_datetime(opn, format='%H:%M').time()) & \
            (data['time'].dt.time <= \
            (pd.to_datetime(cls, format='%H:%M') + pd.Timedelta(seconds=59, milliseconds=999)).time())]

    if inav:
        # get the inav ticker for the etf
        inav_ticker = _BDP(ticker, 'ETF_INAV_TICKER')[0] + ' Index'
        # return the "trades data"
        inav_data = _BDIT(inav_ticker, events='TRADE', sd=start_date, ed=end_date)
        # rename columns to avoid colisions
        inav_data = inav_data.rename(columns={'ticker': 'inav_ticker', 'value': 'inav_value'})
        # drop unnecessary columns
        inav_data = inav_data.drop(columns=['size', 'type'])
        # insert the etf ticker for joining later
        inav_data['ticker'] = ticker
        # we are going to do the time filtering here rather than later
        inav_data['time'] = inav_data['time'].dt.tz_convert(tz_info)
        # filter for market hours only
        inav_data = inav_data.loc[
                (inav_data['time'].dt.time >= pd.to_datetime(opn, format='%H:%M').time()) & \
                (inav_data['time'].dt.time <= \
                (pd.to_datetime(cls, format='%H:%M') + pd.Timedelta(seconds=59, milliseconds=999)).time())]

        inav_df = inav_df.append(inav_data, ignore_index=True)
        # imerge(inav_data, how='outer', on='ticker')

    if fair_value:
        fv_data = _BDIT(fair_value, events='TRADE', sd=start_date, ed=end_date)
        fv_data = fv_data.rename(columns={'ticker': 'fv_ticker', 'value': 'fair_value'})
        fv_data = fv_data.drop(columns=['size', 'type'])
        fv_data['ticker'] = ticker
        fv_data['time'] = fv_data['time'].dt.tz_convert(tz_info)
        fv_data = fv_data.drop_duplicates(subset='time', keep='last')
        fv_data = fv_data.loc[
                (fv_data['time'].dt.time >= pd.to_datetime(opn, format='%H:%M').time()) & \
                        (fv_data['time'].dt.time <= \
                        (pd.to_datetime(cls, format='%H:%M') + pd.Timedelta(seconds=59, milliseconds=999)).time())]


    data = data.drop_duplicates()
    df = df.append(data)

    df = df.set_index(['ticker', 'time', 'type'], append=True).unstack()
    df.columns = [str(p[1]).upper() + '_' + str(p[0]).upper() for p in df.columns]
    df = df.droplevel(0).reset_index().sort_values(by=['ticker', 'time']).reset_index(drop=True)
    df = df.drop_duplicates().reset_index(drop=True)

    if inav:
        inav_df = inav_df.sort_values(by=['ticker', 'time'])
        df = df.merge(inav_df, how='outer', on=['ticker', 'time'])
        df = df.sort_values(by=['ticker', 'time']).reset_index(drop=True)
        df = df.drop(columns=['inav_ticker'])

    if fair_value:
        fv_df = fv_data.sort_values(by=['ticker', 'time'])
        df = df.merge(fv_df, how='outer', on=['ticker', 'time'])
        df = df.sort_values(by=['ticker', 'time']).reset_index(drop=True)
        df = df.drop(columns=['fv_ticker'])


    for col in df.columns.tolist():
        if ('ASK' in col) or ('BID' in col) or (col == 'inav_value') or (col == 'fair_value'):
            df[col] = df[col].groupby([df['ticker'], df['time'].dt.date]).fillna(method='ffill')
            # df[col] = df[col].fillna(0)

    df = df.drop_duplicates().reset_index(drop=True)

    for col in ['BEST_BID_VALUE', 'BEST_ASK_VALUE']:
        if col not in df.columns:
            df[col] = np.NaN

    df.loc[df['BEST_ASK_VALUE']==0, 'BEST_ASK_VALUE'] = np.NaN
    df.loc[df['BEST_BID_VALUE']==0, 'BEST_BID_VALUE'] = np.NaN
    df['bid_ask_spread'] = df['BEST_ASK_VALUE'] - df['BEST_BID_VALUE']
    df['MID_VALUE'] = (df['BEST_ASK_VALUE'] + df['BEST_BID_VALUE']) / 2
    df['bid_ask_spread_bps'] = (df['bid_ask_spread'] / df['MID_VALUE']) * 10000


    # number of seconds quote was valid for
    df['quote_life_secs'] = (df.groupby([df['ticker'], df['time'].dt.date])['time'].diff().shift(-1)).dt.total_seconds()
    df['adj_quote_life_secs'] = df['quote_life_secs'].copy()
    df.loc[df['bid_ask_spread_bps'].isna(), 'adj_quote_life_secs'] = 0
    df['adj_quote_life_secs_cumsum'] = df['adj_quote_life_secs'].cumsum()

    # for col in ['ASK_VALUE', 'MID_VALUE', 'ASK_SIZE', 'BID_SIZE', 'BID_VALUE', 'bid_ask_spread_bps', 'bid_ask_spread']:
    #     df[col] = df[col].fillna(0)

    if 'TRADE_SIZE' not in df.columns.tolist():
        df['TRADE_SIZE'] = np.NaN
        df['TRADE_VALUE'] = np.NaN

    df['spread_times_time'] = df['bid_ask_spread_bps'] * df['adj_quote_life_secs']
    df['spread_times_time'] = df['spread_times_time'].fillna(0)
    df['twas_bps'] = df['spread_times_time'].expanding().sum() / df['adj_quote_life_secs_cumsum']
    df['twas_bps'] = df['twas_bps'].fillna(method='ffill')

    df['twas_bps'] = df['spread_times_time'].expanding().sum() / df['adj_quote_life_secs_cumsum']
    df['twas_bps'] = df['twas_bps'].fillna(method='ffill')

    df['vwap'] = (df['TRADE_SIZE'].expanding().sum() * df['TRADE_VALUE'].expanding().sum()) / df['TRADE_SIZE'].expanding().sum()
    df['vwap'] = df['vwap'].fillna(method='ffill')
    df['vwap'] = df['vwap'].fillna(0)

    if inav:
        df['premium_bps'] = (df['MID_VALUE'] / df['inav_value'] - 1) * 10000
    if fair_value:
        df['fv_premium_bps'] = (df['MID_VALUE'] / df['fair_value'] - 1) * 10000


    # number of seconds per day
    df['day_secs'] = df['quote_life_secs'].sum()



    if summary==True:
        df['bid_ask_spread_bps'] = df['bid_ask_spread_bps'].fillna(0)
        df['adj_quote_life_secs'] = df['adj_quote_life_secs'].fillna(0)
        df['sum_bas'] = df['bid_ask_spread_bps'] * df['adj_quote_life_secs']
        twas = df['sum_bas'].sum() / df['adj_quote_life_secs'].sum()
        pres = df['adj_quote_life_secs'].sum() / df['quote_life_secs'].sum()
        df['sum_bid'] = df['BEST_BID_VALUE'] * df['adj_quote_life_secs']
        twab = df['sum_bid'].sum() / df['adj_quote_life_secs'].sum()
        df['sum_ask'] = df['BEST_ASK_VALUE'] * df['adj_quote_life_secs']
        twaa = df['sum_ask'].sum() / df['adj_quote_life_secs'].sum()
        df['sum_mid'] = df['MID_VALUE'] * df['adj_quote_life_secs']
        twam = df['sum_mid'].sum() / df['adj_quote_life_secs'].sum()

        largs = df['TRADE_SIZE'].max()

        if inav:
            df['premium_bps'] = df['premium_bps'].fillna(0)
            df['sum_prem'] = df['premium_bps'] * df['adj_quote_life_secs']
            twapd = df['sum_prem'].sum() / df['adj_quote_life_secs'].sum()
            df['sum_inav'] = df['inav_value'] * df['adj_quote_life_secs']
            twai = df['sum_inav'].sum() / df['adj_quote_life_secs'].sum()

        if fair_value:
            df['fv_premium_bps'] = df['fv_premium_bps'].fillna(0)
            df['sum_fv_prem'] = df['fv_premium_bps'] * df['adj_quote_life_secs']
            twapdf = df['sum_fv_prem'].sum() / df['adj_quote_life_secs'].sum()
            df['sum_fv'] = df['fair_value'] * df['adj_quote_life_secs']
            twaf = df['sum_fv'].sum() / df['adj_quote_life_secs'].sum()

        # filling with the most recent quote counter errors with twas calculations
        for x in ['BEST_BID_VALUE', 'BEST_ASK_VALUE', 'inav_value', 'MID_VALUE', 'bid_ask_spread_bps', 'fair_value']:
            if x in df.columns.tolist():
                df[x] = df[x].ffill()
                df[x] = df[x].bfill()
        df['TRADE_SIZE'] = df['TRADE_SIZE'].fillna(0)
        df['TRADE_VALUE'] = df['TRADE_VALUE'].fillna(0)
        vwap = df['TRADE_SIZE'].dot(df['TRADE_VALUE']) / df['TRADE_SIZE'].sum()
        bid_vwap = df['TRADE_SIZE'].dot(df['BEST_BID_VALUE']) / df['TRADE_SIZE'].sum()
        ask_vwap = df['TRADE_SIZE'].dot(df['BEST_ASK_VALUE']) / df['TRADE_SIZE'].sum()
        vwas = df['TRADE_SIZE'].dot(df['bid_ask_spread_bps']) / df['TRADE_SIZE'].sum()
        if inav:
            inav_vwap = df['TRADE_SIZE'].dot(df['inav_value']) / df['TRADE_SIZE'].sum()

        if fair_value:
            fv_vwap = df['TRADE_SIZE'].dot(df['fair_value']) / df['TRADE_SIZE'].sum()

        mid_vwap = df['TRADE_SIZE'].dot(df['MID_VALUE']) / df['TRADE_SIZE'].sum()
        share_volume = df['TRADE_SIZE'].sum()
        turnover = (df['TRADE_SIZE'] * df['TRADE_VALUE']).sum()
        numb_trades = df.loc[df['TRADE_SIZE'] > 0, 'TRADE_SIZE'].count()

        dats = {
            'ticker': ticker,
            'date': date,
            'share_volume': share_volume,
            'turnover': turnover,
            'numb_trades': numb_trades,
            'largest_block': largs,
            'largest_block_value': largs * vwap,
            'twa_spread_bps': twas,
            'vwa_spread_bps': vwas,
            'presence': pres,
            'vwap': vwap,
            'vwa_bid': bid_vwap,
            'vwa_ask': ask_vwap,
            'vwa_mid': mid_vwap,
            'twa_mid': twam,
            'twa_ask': twaa,
            'twa_bid': twab
            }
        if fair_value:
            dats.update({'twa_fv_premium': twapdf})
            dats.update({'vwa_fv': fv_vwap, 'twa_fv': twaf})
        if inav:
            dats.update({'vwa_inav': inav_vwap, 'twa_premium': twapd})
            dats.update({'twa_inav': twai})

        if inav and fair_value:
            dats.update({'twa_fv_vs_inav_bps': (twaf / twai - 1) * 10000})
            dats.update({'vwa_fv_vs_inav_bps': (fv_vwap / inav_vwap - 1) * 10000})

        df = pd.DataFrame(dats, index=[0])



    df = df.drop(columns=['BEST_BID_TRADE_TIME', 'BEST_ASK_TRADE_TIME', 'spread_times_time',
        'adj_quote_life_secs_cumsum', 'quote_life_secs', 'day_secs', 'adj_quote_life_secs'],
        errors='ignore')

    # prem/dis on mid, bid, ask
    # trade weighted average spread
    # dealing with no bid or ask when calculating spread
    # presence
    # dealing with no data for a day.
    # aggregated volume from EU ticker

    return df # }}}

