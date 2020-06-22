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
import uuid
from random import choice
from string import ascii_lowercase, digits

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

        if event['ResourceProperties']['CustomResourceAction'] == 'GenerateUUID':

            id = str(uuid.uuid4())
            logging.debug('Proceeding with GenerateUUID: ' + id)
            cfnresponse.send(event, context, cfnresponse.SUCCESS, {"UUID": id}, 'GenerateUUID')


        elif event['ResourceProperties']['CustomResourceAction'] == 'CreateBucket':
            bucketName = 'due-database-' + ''.join(choice(ascii_lowercase + digits) for x in range(10)) + os.environ.get('AWS_REGION')
            logging.debug('Proceeding with Bucket Name: ' + bucketName)

            cfnresponse.send(event, context, cfnresponse.SUCCESS, {"BucketName": bucketName}, bucketName)

        else:
            logging.error('Missing CustomResourceAction - no action to perform')
            cfnresponse.send(event, context, cfnresponse.FAILED, {"success": False, "error": "Missing CustomResourceAction"}, "error")

    except Exception as error:
        logging.error('lambda_handler error: %s' % (error))
        logging.error('lambda_handler trace: %s' % traceback.format_exc())
        cfnresponse.send(event, context, cfnresponse.FAILED, {"success": False, "error": "See Lambda Logs"}, "error")
