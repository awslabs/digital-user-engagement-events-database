#!/usr/bin/python
# -*- coding: utf-8 -*-
##############################################################################
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this
# software and associated documentation files (the "Software"), to deal in the Software
# without restriction, including without limitation the rights to use, copy, modify,
# merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
# PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
##############################################################################

import json
import boto3
import logging
import traceback
import datetime
import json
import os
import urllib.request, urllib.error, urllib.parse

client = boto3.client('athena')
send_anonymous_data = str(os.environ.get('SEND_ANONYMOUS_DATA')).upper()

def lambda_handler(event, context):
    log_level = str(os.environ.get('LOG_LEVEL')).upper()
    if log_level not in [
                        'DEBUG', 'INFO',
                        'WARNING', 'ERROR',
                        'CRITICAL'
                    ]:
      log_level = 'DEBUG'
    logging.getLogger().setLevel(log_level)

    logging.debug(event)

    try:
        for record in event['Records']:
            key = record['s3']['object']['key']
            parts = key.split('/')

            s3Bucket = 's3://' + record['s3']['bucket']['name']

            query = "ALTER TABLE all_events ADD IF NOT EXISTS PARTITION (ingest_timestamp='%s-%s-%s %s:00:00') LOCATION '%s/%s/%s/%s/%s'" % (parts[1], parts[2], parts[3], parts[4], s3Bucket + '/events', parts[1], parts[2], parts[3], parts[4])

            logging.debug(query)

            response = client.start_query_execution(
              QueryString=query,
              QueryExecutionContext={
                  'Database': os.environ.get('DATABASE_NAME')
              },
              ResultConfiguration={
                  'OutputLocation': s3Bucket + '/temp'
              }
            )

            logging.debug(response)

        # Send Anonymous data IF configured
        if send_anonymous_data == "YES":
            sendAnonymousData(len(event['Records']))
        else:
            logging.info('Anonymous usage metrics collection disabled.')

        result = {
            'statusCode': '200',
            'body':  {'message': 'success'}
        }
        return json.dumps(result)

    except Exception as error:
        logging.error('lambda_handler error: %s' % (error))
        logging.error('lambda_handler trace: %s' % traceback.format_exc())
        result = {
            'statusCode': '500',
            'body':  {'message': 'error'}
        }
        return json.dumps(result)


# This function sends anonymous usage data, if enabled
def sendAnonymousData(cnt):
    try:
        logging.debug("Sending Anonymous Data")
        metric_data = {}
        metric_data['S3PartitionTriggers'] = cnt
        postDict = {}
        postDict['TimeStamp'] = str(datetime.datetime.utcnow().isoformat())
        postDict['Data'] = metric_data
        postDict['Solution'] = os.environ.get('SOLUTION_ID')
        postDict['UUID'] = os.environ.get('SOLUTION_UUID')
        # API Gateway URL to make HTTP POST call
        url = 'https://metrics.awssolutionsbuilder.com/generic'
        data=json.dumps(postDict)
        data_utf8 = data.encode('utf-8')
        logging.debug('sendAnonymousData data: %s', data)
        headers = {
            'content-type': 'application/json; charset=utf-8',
            'content-length': len(data_utf8)
        }
        req = urllib.request.Request(url, data_utf8, headers)
        rsp = urllib.request.urlopen(req)
        rspcode = rsp.getcode()
        content = rsp.read()
        logging.debug("Response from APIGateway: %s, %s", rspcode, content)
    except Exception as error:
        logging.error('send_anonymous_data error: %s', error)
