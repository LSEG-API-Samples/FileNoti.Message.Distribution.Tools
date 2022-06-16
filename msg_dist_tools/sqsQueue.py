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
import gracefulExiter
import errorMappingUtils

REGION = 'us-east-1'
METADATA_DIR_NAME = 'metadata'
FAILURE_DIR_NAME = 'failure'

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
def processPayload(payloadText, subscriptionId, sqsMessageId, metadataFolder, failureFolder, callback, destination_folder):
    # ==============================================
    time_str = time.strftime("%Y%m%d-%H%M%S")
    is_payload_processing_errors = False
    ecp_message_id = ""
    try:
        rMessage = json.loads(payloadText)
        app_logger.info("********************************************************************************")
        app_logger.info("************************* Get new message from SQS Queue ***********************")
        app_logger.info("********************************************************************************")
        app_logger.info("subscriptionId: {}, sqsMessageId: {}\n".format(subscriptionId, sqsMessageId))
        distribution_seq_no = 0 if "distributionSeqNo" not in rMessage else rMessage["distributionSeqNo"]
        ecp_message_id = sqsMessageId if "ecpMessageID" not in rMessage else rMessage["ecpMessageID"]
        with open("{}/{}_{}_{}_{}".format(metadataFolder, subscriptionId, time_str, distribution_seq_no, "metadata.json"), 'w') as f:
            f.write(json.dumps(rMessage, indent=2))
            f.close()

        # handover the decoded message to calling module
        if 'payload' in rMessage:
            try:
                callback(ecp_message_id, rMessage, subscriptionId, destination_folder)
            except Exception as err:
                is_payload_processing_errors = True
                message_id = ecp_message_id if ecp_message_id != "" and ecp_message_id is not None else sqsMessageId
                handle_failure_messages(err, subscriptionId, message_id, sqsMessageId, payloadText, failureFolder, time_str)
        else:
            app_logger.error("-------------------- Failed to process message --------------------")
            app_logger.error("subscriptionId {}, skip process cause payload is not found from {}".format(subscriptionId, json.dumps(rMessage)))
            error_logger.error("-------------------- Failed to process message --------------------")
            error_logger.error("subscriptionId {}, skip process cause payload is not found from {}".format(subscriptionId, json.dumps(rMessage)))

    except Exception as err:
        if not is_payload_processing_errors:
            message_id = ecp_message_id if ecp_message_id != "" and ecp_message_id is not None else sqsMessageId
            update_failure_messages(subscriptionId, message_id, err)
            handle_failure_messages(err, subscriptionId, message_id, sqsMessageId, payloadText, failureFolder, time_str)

    app_logger.info("\n")
    app_logger.info('subscriptionId {}, Polling messages from queue \n'.format(subscriptionId))


def handle_failure_messages(err, subscriptionId, messageId, sqsMessageId, payloadText, failureFolder, time_str):
    app_logger.error("-------------------- Failed to process message --------------------")
    error_message = "subscriptionId {}, skip process and could not process message {}, cause {}"\
        .format(subscriptionId, str(payloadText), err)
    app_logger.error(error_message, exc_info=True)
    error_logger.error("-------------------- Failed to process message --------------------")
    error_logger.error(error_message, exc_info=True)
    output_file_path = "{}/{}_{}_{}".format(failureFolder, messageId, time_str, "fail.json")
    try:
        with open(output_file_path, 'w') as f:
            rMessage = json.loads(payloadText)
            rMessage['sqsMessageId'] = sqsMessageId
            f.write(json.dumps(rMessage, indent=2))
            f.close()
    except Exception as err:
        app_logger.error("-------------------- Failed to write failure message --------------------")
        error_message = "subscriptionId {}, failed to write messages then write plain text {}, cause {}".format(
            subscriptionId, str(payloadText), err)
        app_logger.error(error_message, exc_info=True)
        error_logger.error("-------------------- Failed to write failure message --------------------")
        error_logger.error(error_message, exc_info=True)
        with open(output_file_path, 'w') as f:
            f.write(payloadText.decode("utf-8"))
            f.close()


def update_failure_messages(subscriptionId, message_id, err):
    try:
        # message_id, last_update, bucket_name, fileset_name, file_id, status_code, response, url
        error_mapping_dict = errorMappingUtils.read_error_mapping()
        error_key = errorMappingUtils.construct_error_key(message_id, "all")

        total_errors = 0
        if error_key in error_mapping_dict and "totalErrors" in error_mapping_dict[error_key]:
            total_errors = error_mapping_dict[error_key]['totalErrors']

        error_mapping_dict[error_key] = {
            "messageId": message_id,
            "totalErrors": total_errors + 1,
            "lastUpdate": errorMappingUtils.get_current_datetime(),
            "bucketName": "",
            "filesetName": "",
            "fileId": "all",
            "statusCode": -1,
            "url": "",
            "response": str(err)
        }

        errorMappingUtils.write_csv_files(errorMappingUtils.INPUT_ERROR_MAPPING_FILE, error_mapping_dict)

    except Exception as err:
        app_logger.error("-------------------- Failed to write error metadata --------------------")
        error_message = "subscriptionId {}, messageId {}, failed to write error metadata, cause {}".format(
            subscriptionId, message_id, err)
        app_logger.error(error_message, exc_info=True)
        error_logger.error("-------------------- Failed to write error metadata --------------------")
        error_logger.error(error_message, exc_info=True)


def create_failure_folder():
    try:
        os.mkdir(FAILURE_DIR_NAME)
        app_logger.info("create failure folder {}".format(FAILURE_DIR_NAME))
    except:
        pass

    return FAILURE_DIR_NAME


def create_metadata_folder(subscriptionId):
    metadata_folder = "{}/{}".format(METADATA_DIR_NAME, subscriptionId)
    try:
        os.mkdir(METADATA_DIR_NAME)
    except:
        pass

    try:
        os.mkdir(metadata_folder)
        app_logger.info("subscriptionId {}, create metadata folder {} if it does not exist".format(subscriptionId, metadata_folder))
    except:
        pass

    return metadata_folder


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

    metadata_folder = create_metadata_folder(subscriptionId)
    failure_folder = create_failure_folder()

    sqs = session.client('sqs')

    app_logger.info('subscriptionId {}, Polling messages from queue\n'.format(subscriptionId))
    flag = gracefulExiter.GracefulExiter()
    while 1:
        resp = sqs.receive_message(QueueUrl=endpoint, MaxNumberOfMessages=10, WaitTimeSeconds=10)

        if 'Messages' in resp:
            messages = resp['Messages']
            # print and remove all the nested messages
            for message in messages:
                sqs_message_id = ""
                mBody = None
                try:
                    mBody = message['Body']
                    sqs_message_id = message['MessageId']
                    # decrypt this message
                    m = decrypt(cryptographyKey, mBody)
                    processPayload(m, subscriptionId, sqs_message_id, metadata_folder, failure_folder, callback, destination_folder)
                except Exception as err:
                    error_logger.error("---------------- Found unexpected error and failed to process messages --------------------")
                    error_message = "subscriptionId {}, sqsMessageId: {}, fail to process messages, cause {}, messages {}".format(subscriptionId, sqs_message_id, err, mBody)
                    error_logger.error(error_message, exc_info=True)
                finally:
                    # *** accumulate and remove all the nested message
                    sqs.delete_message(QueueUrl=endpoint, ReceiptHandle=message['ReceiptHandle'])

                if flag.exit():
                   app_logger.info("program is gracefully exit")
                   sys.exit(0)


# ==============================================
if __name__ == "__main__":
    # ==============================================
    print("SQS module cannot run standalone. Please use fileNotiMessages.py")
