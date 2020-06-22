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

import cfnresponse
import os
import logging
import traceback
import boto3
import json
import time

athena = boto3.client('athena')
sesv2 = boto3.client('sesv2')


def execute_named_queries(namedQueries):
    try:
        response = athena.batch_get_named_query(
            NamedQueryIds=namedQueries
        )

        for q in response['NamedQueries']:
            start_query_response = athena.start_query_execution(
                QueryString=q['QueryString'],
                QueryExecutionContext={
                  'Database': q['Database']
                },
                ResultConfiguration={
                  'OutputLocation': 's3://%s/temp/' % (os.environ.get('S3_DATA_BUCKET'))
                }
            )

            while True:
                time.sleep(2)

                get_query_response = athena.get_query_execution(
                    QueryExecutionId=start_query_response['QueryExecutionId']
                )

                if get_query_response['QueryExecution']['Status']['State'] == 'SUCCEEDED' or get_query_response['QueryExecution']['Status']['State'] == 'FAILED':
                    logging.debug('Query Result for: %s' % (q['Name']), get_query_response)
                    break
    except Exception as error:
        logging.error('execute_named_queries error: %s' % (error))
        logging.error('execute_named_queries trace: %s' % traceback.format_exc())
        raise

def set_pinpoint_event_destination(snames):
    try:
        for sn in snames:
            if sn == '':
                break
            response = sesv2.create_configuration_set_event_destination(
                ConfigurationSetName=sn,
                EventDestinationName='event-database',
                EventDestination={
                  'Enabled': True,
                  'MatchingEventTypes': [
                      'SEND','REJECT','BOUNCE','COMPLAINT','DELIVERY','OPEN','CLICK','RENDERING_FAILURE',
                  ],
                  'PinpointDestination': {
                      'ApplicationArn': os.environ.get('PINPOINT_PROJECT_ARN')
                  }
                }
            )
            logging.debug('SN Response for: %s' % (sn), response)
    except Exception as error:
        logging.error('set_pinpoint_event_destination error: %s' % (error))
        logging.error('set_pinpoint_event_destination trace: %s' % traceback.format_exc())
        raise

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
        if event['ResourceProperties']['CustomResourceAction'] == 'SetupSampleFiles':
            execute_named_queries([os.environ.get('ALL_EVENT_TABLE')])
            execute_named_queries([os.environ.get('SEND_NQ'),
                os.environ.get('HARD_BOUNCE_NQ'),
                os.environ.get('SOFT_BOUNCE_NQ'),
                os.environ.get('COMPLAINT_NQ'),
                os.environ.get('DELIVERY_NQ'),
                os.environ.get('OPEN_NQ'),
                os.environ.get('CLICK_NQ'),
                os.environ.get('UNSUB_NQ'),
                os.environ.get('REJECT_NQ'),
                os.environ.get('SMS_BUFF_NQ'),
                os.environ.get('SMS_SUCCESS_NQ'),
                os.environ.get('SMS_FAILURE_NQ'),
                os.environ.get('SMS_OPTOUT_NQ'),
                os.environ.get('CAMPAIGN_SEND_NQ'),
                os.environ.get('JOURNEY_SEND_NQ')])
            set_pinpoint_event_destination(os.environ.get('EXISTING_CS').split(','))

            cfnresponse.send(event, context, cfnresponse.SUCCESS, {"success": True}, 'SetupSampleFiles')
        else:
            logging.error('Missing CustomResourceAction - no action to perform')
            cfnresponse.send(event, context, cfnresponse.FAILED, {"success": False, "error": "Missing CustomResourceAction"}, "error")

    except Exception as error:
        logging.error('lambda_handler error: %s' % (error))
        logging.error('lambda_handler trace: %s' % traceback.format_exc())
        cfnresponse.send(event, context, cfnresponse.FAILED, {"success": False, "error": "See Lambda Logs"}, "error")
