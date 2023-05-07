#!/usr/bin/python
#
# Copyright 2021 Google LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Retrieves a saved report or generates a new one.

To get saved reports, run get_all_saved_reports.py.

Tags: accounts.reports.generate
"""
import schedule
import time
import adsense_util
import argparse
import sys
import google.auth.exceptions
from googleapiclient import discovery
import gspread
import pandas as pd
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from datetime import date 

def func():

  todays_date = date.today()
  scopes = ['https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive']

  credentials = Credentials.from_service_account_file('creds.json', scopes=scopes)

  #authorize gsheet
  gc = gspread.authorize(credentials)
  gauth = GoogleAuth()
  drive = GoogleDrive(gauth)

  #authorize gsheet
  #gc = gspread.service_account(filename="client_secrets.json")
  #sh = gc.open("jamespy").sheet1

  # open a google sheet
  gs = gc.open_by_key('1hwS42iUCRdlXafWt48Vop4vTR1Eo9dxbBFMrrcIjYwY')

  # select a work sheet from its name
  worksheet1 = gs.worksheet('Sheet1')


  # Declare command-line flags.
  argparser = argparse.ArgumentParser(add_help=True)
  argparser.add_argument(
      '--report_id',
      help='The ID of the saved report to generate.')

  args = argparser.parse_args()
  saved_report_id = args.report_id

  def main(argv):
    # Authenticate and construct service.
    credentials = adsense_util.get_adsense_credentials()
    with discovery.build('adsense', 'v2', credentials = credentials) as service:
      try:
        # Let the user pick account if more than one.
        account_id = adsense_util.get_account_id(service)

        # Retrieve report.
        if saved_report_id:
          result = service.accounts().reports().saved().generate(
              name=saved_report_id, dateRange='LAST_7_DAYS').execute()
        else:
          result = service.accounts().reports().generate(
              account=account_id, dateRange='CUSTOM',
              startDate_year=2023, startDate_month=1, startDate_day=10,
              endDate_year=todays_date.year, endDate_month=todays_date.month, endDate_day=todays_date.day,
              metrics=['PAGE_VIEWS', 'ESTIMATED_EARNINGS', 'IMPRESSIONS', 'CLICKS', 'IMPRESSIONS_CTR', 'COST_PER_CLICK', 'IMPRESSIONS_RPM'			
                      ],
              dimensions=['DATE'],
              orderBy=['+DATE']).execute()

        print(type(result))
        #print(result)

        # Display headers.
        #for header in result['headers']:
        #  print('%25s' % header['name'], end=''),
        #print()

        # Display results.
        #if 'rows' in result:
        #  for row in result['rows']:
        #   for cell in row['cells']:
        #    print('%25s' % cell['value'], end='')
        #
        df = pd.DataFrame(columns=list(map(lambda x:x['name'],result['headers'])))
        for i in range(int(result['totalMatchedRows'])):
            df.loc[i]=list(map(lambda x:x['value'],result['rows'][i]['cells']))

        #print(df)
        
        #(result)
        #print(new)
        
        # write to dataframe
        set_with_dataframe(worksheet=worksheet1, dataframe=df, include_index=False,
        include_column_header=True, resize=True)
        
        # dataframe (create or import it)
        #df = pd.DataFrame({'a':['astronaut', 'ant'], 'b':['bingo', 'bee'], 'c':['candy', 'cake']})
        #df_values = df.values.tolist()
        #print(df_values)
        
        #gs.values_append('Sheet1', {'valueInputOption': 'USER_ENTERED'}, {'values': df_values})



        # Display date range.
        #print('Report from %s to %s.' % (result['startDate'], result['endDate']))
        #print()

      except google.auth.exceptions.RefreshError:
        print('The credentials have been revoked or expired, please delete the '
              '"%s" file and re-run the application to re-authorize.' %
              adsense_util.CREDENTIALS_FILE)


  if __name__ == '__main__':
    main(sys.argv)

schedule.every(1).minutes.do(func)

while True:
    schedule.run_pending()
    time.sleep(1)
