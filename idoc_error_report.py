import hashlib
import json
import os
import pprint
import requests

from datetime import datetime

CURRENT_DT = datetime.now()

class NRInsightsQueryAPI:
  def __init__(self, insights_query_key, account_id):
    self.account_id = account_id
    self.headers = {
      'X-Query-Key': '{}'.format(insights_query_key)
      }
    self.query_url = 'https://insights-api.newrelic.com/v1/accounts/{}/query'.format(account_id)    
    
  def query(self, query):
    _qstring = "?nrql={}".format(query)
    return requests.get(self.query_url + _qstring, headers=self.headers)


class NRLogInsertAPI:
  def __init__(self, insights_insert_key, account_id):
    self.account_id = account_id
    self.headers = {
      'content-type': 'application/json',
      'X-Insert-Key': '{}'.format(insights_insert_key)
      }
    self.insert_url = 'https://log-api.newrelic.com/log/v1'

  def insert(self, events):
    return requests.post(self.insert_url, data=json.dumps(events), headers=self.headers)

def get_idoc_errors(account_id, insights_query_key, time_lookback_minutes=1440, fields_to_facet = 'UNAME, STATXT', error_codes=['51']):
    query_string = "FROM `WE02_INFORWARDER:INT_EDIDS` select count(*) where STATUS = '51' and STATXT is NOT NULL facet {} since {} minutes ago".format(
        fields_to_facet,
        time_lookback_minutes,
      )
    _api = NRInsightsQueryAPI(account_id=account_id, insights_query_key=insights_query_key)
    response = _api.query(query_string)
    if response.status_code != 200:
        raise Exception(
          "Error getting IDOC error data. Response code: {}.  Error text: {}".format(
                response.status_code,
                response.text)
            )    
    else:
        _json = response.json()
        if 'facets' in _json:
            return _json['facets']
        else:
            return []

def main():
    idc_error_report = get_idoc_errors(
      '2901317',
      os.getenv('NR_TRACEGEN_INSIGHTS_QUERY_KEY'),
      time_lookback_minutes=1440,
      fields_to_facet= ",".join(
          ['UNAME', 'STATXT']
         ),
       error_codes = ['51']
      )
    print('{},{},{}'.format('UNAME', 'STATXT', 'COUNT'))
    for facet in idc_error_report:
        print('{},{},{}'.format(facet['name'][0], facet['name'][1], facet['results'][0]['count']))
       
 
if __name__== "__main__": 
    main()
