#=============================================================================
# Module for polling and extracting news or research alerts from an AWS SQS Queue
# This module uses boto3 library from Anazon for fetching messages
# It uses pycryptodome - for AES GCM decryption
#-----------------------------------------------------------------------------
#   This source code is provided under the Apache 2.0 license
#   and is provided AS IS with no warranty or guarantee of fit for purpose.
#   Copyright (C) 2021 Refinitiv. All rights reserved.
#=============================================================================

import boto3
import json
import base64
from Crypto.Cipher import AES
import traceback
import os, sys
import time
from loggingFileNoti import get_app_logger, get_error_logger

REGION = 'us-east-1'
METADATA_DIR_NAME = 'metadata'

app_logger = get_app_logger("app_info")
error_logger = get_error_logger("app_error")

#==============================================
def decrypt(key, source):
    #==============================================
    GCM_AAD_LENGTH = 16
    GCM_TAG_LENGTH = 16
    GCM_NONCE_LENGTH = 12
    key = base64.b64decode(key)
    cipherText = base64.b64decode(source)

    aad = cipherText[:GCM_AAD_LENGTH]
    nonce = aad[-GCM_NONCE_LENGTH:]
    tag = cipherText[-GCM_TAG_LENGTH:]
    encMessage = cipherText[GCM_AAD_LENGTH:-GCM_TAG_LENGTH]

    cipher = AES.new(key, AES.MODE_GCM, nonce=nonce)
    cipher.update(aad)
    decMessage = cipher.decrypt_and_verify(encMessage, tag)
    return decMessage


# ==============================================
def processPayload(payloadText, subscriptionId, metadataFolder, callback, destination_folder):
    # ==============================================
    try:
        rMessage = json.loads(payloadText)
        app_logger.info('subscriptionId {}, --------- Received message -----------'.format(subscriptionId))
        distribution_seq_no = 0 if "distributionSeqNo" not in rMessage else rMessage["distributionSeqNo"]
        time_str = time.strftime("%Y%m%d-%H%M%S")
        with open("{}/{}_{}_{}_{}".format(metadataFolder, subscriptionId, time_str, distribution_seq_no, "metadata.json"), 'w') as f:
            f.write(json.dumps(rMessage, indent=2))
            f.close()

        # handover the decoded message to calling module
        if 'payload' in rMessage:
            callback(rMessage, subscriptionId, destination_folder)
        else:
            app_logger.error("subscriptionId {}, skip process cause payload is not found from {}".format(subscriptionId, json.dumps(rMessage)))
            error_logger.error("subscriptionId {}, skip process cause payload is not found from {}".format(subscriptionId, json.dumps(rMessage)))

    except Exception as err:
        error_message = "subscriptionId {}, skip process and could not process message {}, cause {}".format(subscriptionId, str(payloadText), err)
        app_logger.error(error_message, exc_info=True)
        error_logger.error(error_message, exc_info=True)
    app_logger.info("\n")


#==============================================
def startPolling(accessID, secretKey, sessionToken, endpoint, cryptographyKey, subscriptionId, callback, destination_folder):
    #==============================================
    # create a SQS session
    session = boto3.Session(
        aws_access_key_id=accessID,
        aws_secret_access_key=secretKey,
        aws_session_token=sessionToken,
        region_name=REGION
    )

    metadata_folder = "{}/{}".format(METADATA_DIR_NAME, subscriptionId)
    app_logger.info("subscriptionId {}, create metadata folder {} if it does not exist".format(subscriptionId, metadata_folder))
    try:
        os.mkdir(METADATA_DIR_NAME)
    except:
        pass

    try:
        os.mkdir(metadata_folder)
    except:
        pass

    sqs = session.client('sqs')

    app_logger.info('subscriptionId {}, Polling messages from queue '.format(subscriptionId))
    while 1:
        resp = sqs.receive_message(QueueUrl=endpoint, MaxNumberOfMessages=10, WaitTimeSeconds=10)

        if 'Messages' in resp:
            messages = resp['Messages']
            # print and remove all the nested messages
            for message in messages:
                mBody = message['Body']
                # decrypt this message
                m = decrypt(cryptographyKey, mBody)
                processPayload(m, subscriptionId, metadata_folder, callback, destination_folder)
                # *** accumulate and remove all the nested message
                sqs.delete_message(QueueUrl=endpoint, ReceiptHandle=message['ReceiptHandle'])


#==============================================
if __name__ == "__main__":
#==============================================
    print("SQS module cannot run standalone. Please use fileNotiMessages.py")
