# =============================================================================
# Refinitiv Data Platform demo app to subscribe to Research messages
# -----------------------------------------------------------------------------
#   This source code is provided under the Apache 2.0 license
#   and is provided AS IS with no warranty or guarantee of fit for purpose.
#   Copyright (C) 2021 Refinitiv. All rights reserved.
# =============================================================================
import requests
import json
import rdpToken
import sqsQueue
import glob
import sys
import time
import boto3
import os
from botocore.exceptions import ClientError
import argparse
from urllib.parse import urlparse
from halo import Halo
import errorMappingUtils
from threading import Thread


# Application Constants
base_URL = "https://api.refinitiv.com"
RDP_version = "/v1"
REGION = 'us-east-1'
currentSubscriptionID = None
MAX_RETRY_COUNT = 3
spinner = Halo(text='', spinner='dots')
spinner.start()

from loggingFileNoti import get_app_logger, get_error_logger

app_logger = get_app_logger("app_info")
error_logger = get_error_logger("app_error")

#app_logger = setup_logger("app_info", "app.log")
#error_logger = setup_logger("app_error", "error.log", level=logging.ERROR, log_sys_type=sys.stderr)


# ==============================================
def subscribeToFileNoti(input_file=None):

    input_json = None
    if input_file is not None:
        with open(input_file, 'r', encoding='utf-8') as data_file:
            input_json = json.load(data_file)

    # ==============================================
    # get the latest access token first
    accessToken = rdpToken.getToken()

    category_URL = "/message-services"
    endpoint_URL = "/file-store/subscriptions"
    RESOURCE_ENDPOINT = base_URL + category_URL + RDP_version + endpoint_URL
    requestData = {
        "transport": {
            "transportType": "AWS-SQS"
        }
    }

    if input_json is not None:
        requestData = input_json

    hdrs = {
        "Authorization": "Bearer " + accessToken,
        "Content-Type": "application/json"
    }
    app_logger.info("\n")
    app_logger.info("*************************************************************************")
    app_logger.info("******************* Create new subscription *****************************")
    app_logger.info("*************************************************************************")
    app_logger.info("Request URL: {0}".format(RESOURCE_ENDPOINT))
    app_logger.info("Request Body: {0}".format(requestData))

    dResp = requests.post(RESOURCE_ENDPOINT, headers=hdrs, data=json.dumps(requestData))
    if dResp.status_code != 200:
        #error_logger.error("Unable to subscribe. Code %s, Message: %s" % (dResp.status_code, dResp.text))
        raise ValueError("Unable to subscribe. Code %s, Message: %s" % (dResp.status_code, dResp.text))
    else:
        jResp = json.loads(dResp.text)
        app_logger.info("-------------------- Successfully create subscription ----------------------")
        app_logger.info("subscriptionID: {}".format(jResp["subscriptionID"]))
        app_logger.info("transportEndpoint: {}".format(jResp["transportInfo"]["endpoint"]))
        app_logger.info("cryptographyKey: {}".format(jResp["transportInfo"]["cryptographyKey"]))
        if 'query' in jResp:
            app_logger.info("query: {}".format(jResp["query"]))
        app_logger.debug("Successfully create subscriptionId {} with response {}\n".format(jResp["subscriptionID"], jResp))
        return jResp["transportInfo"]["endpoint"], jResp["transportInfo"]["cryptographyKey"], jResp["subscriptionID"]


def updateSubscription(subscriptionId, input_file):
    with open(input_file, 'r', encoding='utf-8') as data_file:
        input_json = json.load(data_file)

    # ==============================================
    # get the latest access token first
    accessToken = rdpToken.getToken()

    category_URL = "/message-services"
    endpoint_URL = "/file-store/subscriptions/" + subscriptionId
    RESOURCE_ENDPOINT = base_URL + category_URL + RDP_version + endpoint_URL

    hdrs = {
        "Authorization": "Bearer " + accessToken,
        "Content-Type": "application/json"
    }
    app_logger.info("\n")
    app_logger.info("*************************************************************************")
    app_logger.info("*********************** Update subscription *****************************")
    app_logger.info("*************************************************************************")
    app_logger.info("Request URL: {0}".format(RESOURCE_ENDPOINT))
    app_logger.info("subscriptionID: {0}".format(subscriptionId))
    app_logger.info("Request Input file: {0}".format(input_file))
    app_logger.info("Request Body: {0}".format(input_json))

    dResp = requests.put(RESOURCE_ENDPOINT, headers=hdrs, data=json.dumps(input_json))
    if dResp.status_code != 200:
        #error_logger.error("Failed to update subscription {} response {}".format(subscriptionId, dResp))
        raise ValueError("Unable to update subscription %s. Code %s, Message: %s" % (subscriptionId, dResp.status_code, dResp.text))
    else:
        jResp = json.loads(dResp.text)
        app_logger.info("-------------------- Successfully update subscription ----------------------")
        app_logger.info("subscriptionID: {}".format(jResp["subscriptionID"]))
        app_logger.info("transportEndpoint: {}".format(jResp["transportInfo"]["endpoint"]))
        app_logger.info("cryptographyKey: {}".format(jResp["transportInfo"]["cryptographyKey"]))
        if 'query' in jResp:
            app_logger.info("query: {}\n".format(jResp["query"]))
        else:
            app_logger.info("\n")
        app_logger.debug("Successfully update subscriptionId {} with response {}".format(jResp["subscriptionID"], jResp))
        return jResp["transportInfo"]["endpoint"], jResp["transportInfo"]["cryptographyKey"], jResp["subscriptionID"]


# ==============================================
def getCloudCredentials(endpoint):
    # ==============================================
    category_URL = "/auth/cloud-credentials"
    endpoint_URL = "/"
    RESOURCE_ENDPOINT = base_URL + category_URL + RDP_version + endpoint_URL
    requestData = {
        "endpoint": endpoint
    }
    app_logger.info("\n")
    app_logger.info("********************************************************************************")
    app_logger.info("***************** Getting credentials to connect to AWS Queue ******************")
    app_logger.info("********************************************************************************")
    app_logger.info("Request URL: {0}".format(RESOURCE_ENDPOINT))
    app_logger.info("Endpoint: {0}".format(endpoint))

    # get the latest access token
    accessToken = rdpToken.getToken()
    dResp = requests.get(RESOURCE_ENDPOINT, headers={"Authorization": "Bearer " + accessToken}, params=requestData)
    if dResp.status_code != 200:
        #error_logger.error("Failed to get cloud credentials from endpoint {} response {}".format(endpoint, dResp))
        raise ValueError("Unable to get credentials. Code %s, Message: %s" % (dResp.status_code, dResp.text))
    else:
        jResp = json.loads(dResp.text)
        app_logger.info("-------------------- Successfully get credentials ----------------------")
        app_logger.debug("accessKeyId: {}".format(jResp["credentials"]["accessKeyId"]))
        app_logger.debug("secretKey: {}".format(jResp["credentials"]["secretKey"]))
        app_logger.debug("sessionToken: {}\n".format(jResp["credentials"]["sessionToken"]))
        app_logger.debug("Successfully get cloud credential from endpoint {} with response {}".format(endpoint, jResp))
        return jResp["credentials"]["accessKeyId"], jResp["credentials"]["secretKey"], jResp["credentials"]["sessionToken"]


def recoveryFailureMessages(destinationFolder):
    failure_folder_name = sqsQueue.FAILURE_DIR_NAME
    current_pwd = os.getcwd()
    failure_folder_path = os.path.join(current_pwd, failure_folder_name)
    app_logger.info("\n")
    app_logger.info("********************************************************************************************")
    app_logger.info("******************************* Start Recovery Failure Messages ****************************")
    app_logger.info("********************************************************************************************\n")
    app_logger.info("failure path = {}".format(failure_folder_path))
    failure_file_name_list = glob.glob1(failure_folder_path, "*")
    num_success_msg = 0
    num_failure_msg = 0
    unrecoverable_files = []

    # message_id, last_update, bucket_name, fileset_name, file_id, status_code, response, url
    error_mapping_dict = errorMappingUtils.read_error_mapping()

    for idx, failure_file_name in enumerate(failure_file_name_list):
        is_error_metadata_changes = False
        try:
            failure_file_name_path = os.path.join(failure_folder_path, failure_file_name)
            with open(failure_file_name_path, 'r') as f:
                data = json.load(f)
            app_logger.info("\n")

            message_id = data["ecpMessageID"] if "ecpMessageID" in data else data["sqsMessageId"]

            all_file_error_key = errorMappingUtils.construct_error_key(message_id, "all")
            is_recovery_all_files = all_file_error_key in error_mapping_dict
            total_files, success_list, error_list = downloadFile(message_id, data, None, destinationFolder, is_recovery_mode=True)
            app_logger.info("\n")
            app_logger.info("********************************************************************************************")
            app_logger.info("****************************** Successfully recovery file No. {}/{} ************************".format(idx+1, len(failure_file_name_list)))
            app_logger.info("********************************************************************************************")
            app_logger.info("failure_file_name_path {}\n".format(failure_file_name_path))
            app_logger.info("{} total_files, {} success_files, {} error files".format(len(total_files), len(success_list), len(error_list)))

            is_complete_success_recovery_unexpected = is_recovery_all_files and len(error_list) == 0 and len(success_list) == len(total_files) and len(success_list) > 0
            if is_complete_success_recovery_unexpected:
                is_error_metadata_changes = True
                del error_mapping_dict[all_file_error_key]
                success_msg = "------------------------------ Successfully remove records from error mapping, file No. {}/{} ---------------------------"\
                    .format(idx+1, len(failure_file_name_list))
                app_logger.info(success_msg)
                app_logger.info("remove key {}".format(all_file_error_key))

            for success_file_id in success_list:
                success_recovery_key = errorMappingUtils.construct_error_key(message_id, success_file_id)
                if success_recovery_key in error_mapping_dict:
                    is_error_metadata_changes = True
                    del error_mapping_dict[success_recovery_key]
                    success_msg = "------------------------------ Successfully remove records from error mapping, file No. {}/{}, file_id {} ---------------------------"\
                        .format(idx+1, len(failure_file_name_list), success_file_id)
                    app_logger.info(success_msg)
                    app_logger.info("remove key {}".format(success_recovery_key))

            if is_error_metadata_changes:
                is_require_delete_error_mapping = len(error_mapping_dict) == 0
                errorMappingUtils.write_csv_files(errorMappingUtils.INPUT_ERROR_MAPPING_FILE, error_mapping_dict, is_require_delete_error_mapping=is_require_delete_error_mapping)
                app_logger.info("**********************************************************************************************")
                app_logger.info("****************************** Detect changes from {}, file No. {}/{} ************************".format(
                    errorMappingUtils.INPUT_ERROR_MAPPING_FILE, idx + 1, len(failure_file_name_list)))
                app_logger.info("**********************************************************************************************")

            if len(error_list) > 0:
                num_failure_msg = num_failure_msg + 1
                unrecoverable_files.append(failure_file_name)
            else:
                os.remove(failure_file_name_path)

        except Exception as err:
            num_failure_msg = num_failure_msg + 1
            unrecoverable_files.append(failure_file_name)
            error_message = "file No. {}/{}, failure_file_name {}, cause: {}\n".format(idx+1, len(failure_file_name_list), failure_file_name, err)
            app_logger.error("\n")
            app_logger.error("********************************************************************************************")
            app_logger.error("****************************** Failed to recovery file No. {}/{} ***************************".format(idx+1, len(failure_file_name_list)))
            app_logger.error("********************************************************************************************")
            app_logger.error(error_message, exc_info=True)
            error_logger.error("\n")
            error_logger.error("********************************************************************************************")
            error_logger.error("****************************** Failed to recovery file No. {}/{} ***************************".format(idx + 1, len(failure_file_name_list)))
            error_logger.error("********************************************************************************************")
            error_logger.error(error_message, exc_info=True)

    app_logger.info("\n")
    app_logger.info("********************************************************************************************")
    app_logger.info("******************************* Recovery failure message report ****************************")
    app_logger.info("********************************************************************************************")
    app_logger.info("{} total, {} success, {} fail".format(len(failure_file_name_list), num_success_msg, num_failure_msg))
    if num_failure_msg > 0:
        raise Exception("Unable to recovery {} failure messages and file won't be deleted, file list: {}\n".format(num_failure_msg, unrecoverable_files))


def downloadFile(message_id, rMessage, subscriptionId, destination_folder, is_recovery_mode=False):
    # ==============================================
    pl = rMessage['payload']
    current_at = pl['FileStoreNotification']['currentAt']
    bucket_name = pl['FileStoreNotification']['fileset']['bucketName']
    package_id = pl['FileStoreNotification']['fileset']['packageId']
    fileset_name = pl['FileStoreNotification']['fileset']['name']
    package_name = pl['FileStoreNotification']['fileset']['packageName']
    num_files = pl['FileStoreNotification']['fileset']['numFiles']
    file_stream_list = pl['FileStoreNotification']['fileset']['files']['values']

    app_logger.info("********************************************************************************")
    app_logger.info("******************************* Start Download File ****************************")
    app_logger.info("********************************************************************************")
    app_logger.info("subscriptionId: {}, currentAt: {}, message_id: {}".format(subscriptionId, current_at, message_id))
    app_logger.info("packageName: {}, packageId: {}".format(package_name, package_id))
    app_logger.info("bucketName: {}, filesetName: {}, numFiles: {}, destinationFolder: {}\n".format(bucket_name, fileset_name, num_files, destination_folder))

    success_list = []
    error_list = []
    is_found_error = False
    for index, file_stream in enumerate(file_stream_list):
        file_id = file_stream['id']
        file_name = file_stream["filename"]
        file_size_bytes = file_stream["fileSizeInBytes"]
        url = file_stream["href"]
        output_directory = os.path.join(os.path.abspath(destination_folder), bucket_name, fileset_name)

        params = {
            'file_stream_url': url,
            'file_id': file_id,
            'output_directory': output_directory,
            'subscription_id': subscriptionId,
            'file_no': index + 1,
            'num_files': len(file_stream_list),
            'file_size_bytes': file_size_bytes,
            'package_name': package_name,
            'bucket_name': bucket_name,
            'fileset_name': fileset_name
        }

        if download_file_from_url_include_retry(message_id, params):
            is_found_error = True
            error_list.append(file_id)
        else:
            success_list.append(file_id)

    if is_found_error and not is_recovery_mode:
        raise Exception("Download messages are incomplete, saving file to {}".format(sqsQueue.FAILURE_DIR_NAME))
    else:
        return file_stream_list, success_list, error_list


def download_file_from_url_include_retry(message_id, params):
    retry_count = 0
    is_success = True
    error_message = ""
    status_code = 200
    response = ""
    url = ""

    bucket_name = params['bucket_name']
    fileset_name = params['fileset_name']
    file_id = params['file_id']

    while retry_count < MAX_RETRY_COUNT:

        temp_is_success, temp_error_message, temp_status_code, temp_response, temp_url = handle_download_file_from_url(params)
        is_success = temp_is_success
        error_message = temp_error_message
        status_code = temp_status_code
        response = temp_response
        url = temp_url

        if not is_success:
            error_msg = "------------------------------ Failed to retry {}/{}, File No. {}/{} ---------------------------".format(
                retry_count+1, MAX_RETRY_COUNT, params['file_no'], params['num_files'])
            app_logger.warning(error_msg)
            error_logger.warning(error_msg)
            error_detail_msg = "sleep 1 second before next retry for file_id {}, error_message {}, status_code {}".\
                format(file_id, error_message, status_code)
            app_logger.warning(error_detail_msg)
            app_logger.error("subscriptionId {}, messageId {}".format(params['subscription_id'], message_id))
            app_logger.error("bucketName {}, filesetName {}, packageName {}, file_id {}".format(bucket_name, fileset_name, params['package_name'], file_id))
            error_logger.warning("{}, url {}, cause {}".format(error_detail_msg, url, response), exc_info=True)
            time.sleep(1)
        else:
            return False

        retry_count = retry_count + 1

    if retry_count >= MAX_RETRY_COUNT and not is_success:
        asterisk_str = "************************************************************************************************"
        error_msg = "------------------------------ Retry reach limit {}/{}, File No. {}/{} ---------------------------".format(
            retry_count, MAX_RETRY_COUNT, params['file_no'], params['num_files'])
        app_logger.error(asterisk_str)
        app_logger.error(error_msg)
        app_logger.error("subscriptionId {}, messageId {}".format(params['subscription_id'], message_id))
        app_logger.error("bucketName {}, filesetName {}, packageName {}".format(bucket_name, fileset_name, params['package_name']))
        app_logger.error("file_id {}, file size {} (bytes)".format(file_id, params['file_size_bytes']))
        app_logger.error("file_stream_url {}".format(params['file_stream_url']))
        app_logger.error("error_message {}, status_code {}, cause: {}".format(error_message, status_code, str(response)))
        app_logger.error(asterisk_str + "\n")

        error_logger.error(asterisk_str)
        error_logger.error(error_msg)
        error_logger.error("subscriptionId {}, messageId {}".format(params['subscription_id'], message_id))
        error_logger.error("bucketName {}, filesetName {}, packageName {}".format(bucket_name, fileset_name, params['package_name']))
        error_logger.error("file_id {}, file size {} (bytes)".format(file_id, params['file_size_bytes']))
        error_logger.error("file_stream_url {}".format(params['file_stream_url']))
        error_logger.error("error_message {}, status_code {}, url {}, cause: {}".format(error_message, status_code, url, str(response)))
        error_logger.error(asterisk_str + "\n")

        # message_id, last_update, bucket_name, fileset_name, file_id, status_code, response, url
        error_mapping_dict = errorMappingUtils.read_error_mapping()
        error_key = errorMappingUtils.construct_error_key(message_id, file_id)

        total_errors = 0
        if error_key in error_mapping_dict and "totalErrors" in error_mapping_dict[error_key]:
            total_errors = error_mapping_dict[error_key]['totalErrors']

        error_mapping_dict[error_key] = {
            "messageId": message_id,
            "totalErrors": int(total_errors) + 1,
            "lastUpdate": errorMappingUtils.get_current_datetime(),
            "bucketName": bucket_name,
            "filesetName": fileset_name,
            "fileId": file_id,
            "errorMessage": error_message,
            "statusCode": status_code,
            "url": url,
            "response": response
        }

        errorMappingUtils.write_csv_files(errorMappingUtils.INPUT_ERROR_MAPPING_FILE, error_mapping_dict)

        return True

    return False


def handle_download_file_from_url(params):
    try:
        return download_file_from_url(params)
    except Exception as err:
        app_logger.warning("------------------------------ [Unexpected error]: Failed to get file stream file No. {}/{} ---------------------------".format(params['file_no'], params['num_files']))
        app_logger.warning("subscriptionId {}".format(params['subscription_id']))
        app_logger.warning("bucketName {}, filesetName {}, packageName {}".format(params['bucket_name'], params['fileset_name'], params['package_name']))
        app_logger.warning("file_id {}, file size {} (bytes)".format(params['file_id'], params['file_size_bytes']))
        app_logger.warning("file_stream_url {}, cause: {}".format(params['file_stream_url'], str(err)))
        error_message = "subscriptionId {}, [Unexpected error]: Failed to get file stream file No. {}/{}, bucketName {}, filesetName {}, file_id {}, file_stream_url {}, cause {}".format(
            params['subscription_id'], params['file_no'], params['num_files'], params['bucket_name'], params['fileset_name'], params['file_id'], params['file_stream_url'], str(err))
        error_logger.warning(error_message, exc_info=True)

        return False, "unexpected error, failed to get file stream", "unknown", str(err), params['file_stream_url']


def download_file_from_url(params):
    file_stream_url = params['file_stream_url']
    file_id = params['file_id']
    output_directory = params['output_directory']
    subscription_id = params['subscription_id']
    file_no = params['file_no']
    num_files = params['num_files']
    file_size_bytes = params['file_size_bytes']
    package_name = params['package_name']
    bucket_name = params['bucket_name']
    fileset_name = params['fileset_name']

    requestData = {
        "doNotRedirect": True
    }
    # get the latest access token
    accessToken = rdpToken.getToken()
    resp = requests.get(file_stream_url, headers={"Authorization": "Bearer " + accessToken}, params=requestData, stream=True)
    if resp.ok:
        jResp = json.loads(resp.text)
        url = jResp["url"]
        a = urlparse(url)
        s3_path = a.path  # Output: /kyle/09-09-201315-47-571378756077.jpg
        s3_file_name = os.path.basename(s3_path)  # Output: 09-09-201315-47-571378756077.jpg
        final_path = os.path.join(os.path.abspath(output_directory), s3_file_name)

        if not os.path.exists(output_directory):
            app_logger.debug("subscriptionId {}, path {} does not exist then create it".format(subscription_id, output_directory))
            os.makedirs(output_directory)

        asterisk_chars = "*******************************"
        download_file_result_msg = "{} Download file result from BucketName: {}, Fileset: {} {}".format(asterisk_chars, bucket_name, fileset_name, asterisk_chars)
        generate_asterisks = generate_asterisk_upper_lower(download_file_result_msg)

        app_logger.debug(generate_asterisks)
        app_logger.debug(download_file_result_msg)
        app_logger.debug(generate_asterisks)

        fileResp = requests.get(url, stream=True)
        if fileResp.ok:
            with open(final_path, 'wb') as f:
                for chunk in fileResp.iter_content(chunk_size=1024 * 8):
                    if chunk:
                        f.write(chunk)
                        f.flush()
                        os.fsync(f.fileno())
            app_logger.info("------------------------------ Successfully download file No. {}/{} ---------------------------".format(file_no, num_files))
            app_logger.info("subscriptionId {}".format(subscription_id))
            app_logger.info("bucketName {}, filesetName {}, packageName {}".format(bucket_name, fileset_name, package_name))
            app_logger.info("file_id {}, s3_filename {}, file size {} (bytes)".format(file_id, s3_file_name, file_size_bytes))
            app_logger.info("s3_file_path {} into {}\n".format(s3_path, final_path))

            return True, "", 200, "", ""

        else:  # HTTP status code 4XX/5XX
            app_logger.warning("------------------------------ Failed to download file No. {}/{} ---------------------------".format(file_no, num_files))
            app_logger.warning("subscriptionId {}".format(subscription_id))
            app_logger.warning("bucketName {}, filesetName {}, packageName {}".format(bucket_name, fileset_name, package_name))
            app_logger.warning("file_id {}, s3_filename {}, file size {} (bytes)".format(file_id, s3_file_name, file_size_bytes))
            app_logger.warning("file_stream_url {}".format(file_stream_url))
            app_logger.warning("status code {}, response {}\n".format(fileResp.status_code, fileResp.text))

            error_message = "subscriptionId {}, Failed to download {}/{}, bucketName {}, filesetName {}, file_id {}, s3_filename {}, file_stream_url {}, url {}, status code {}\n{}".format(
                subscription_id, file_no, num_files, bucket_name, fileset_name, file_id, s3_file_name, file_stream_url, url, fileResp.status_code, fileResp.text)
            app_logger.debug(error_message, exc_info=True)
            error_logger.warning(error_message, exc_info=True)

            return False, "failed to download s3 url", fileResp.status_code, fileResp.text, url
    else:
        app_logger.warning("------------------------------ Failed to get s3 presigned url file No. {}/{} ---------------------------".format(file_no, num_files))
        app_logger.warning("subscriptionId {}".format(subscription_id))
        app_logger.warning("bucketName {}, filesetName {}, packageName {}".format(bucket_name, fileset_name, package_name))
        app_logger.warning("file_id {}, file size {} (bytes)".format(file_id, file_size_bytes))
        app_logger.warning("file_stream_url {}".format(file_stream_url))
        app_logger.warning("status code {}, response {}\n".format(resp.status_code, resp.text))

        error_message = "subscriptionId {}, Failed to get s3 presigned url to download {}/{}, bucketName {}, filesetName {}, file_id {}, file_stream {}, status code {}\n{}".format(
            subscription_id, file_no, num_files, bucket_name, fileset_name, file_id, file_stream_url, resp.status_code, resp.text)
        app_logger.debug(error_message, stack_info=True)
        error_logger.warning(error_message, stack_info=True)

        return False, "failed to get s3 presigned url", resp.status_code, resp.text, file_stream_url


def generate_asterisk_upper_lower(input):
    generate_asterisks = ""
    for idx in range(0, len(input)):
        generate_asterisks = generate_asterisks + "*"
    return generate_asterisks


def getQueueAttributes(subscriptionId):
    try:
        endpoint, cryptographyKey, currentSubscriptionID = showActiveSubscriptions(subscriptionId)
        if currentSubscriptionID is None:
            raise Exception("subscriptionID {0} is not found".format(subscriptionId))

        app_logger.info("\n")
        app_logger.info("********************************************************************************")
        app_logger.info("*************************** Getting SQS Queue Attributes ***********************")
        app_logger.info("********************************************************************************")
        app_logger.info("subscriptionID: {0}".format(subscriptionId))
        app_logger.info("Endpoint: {0}".format(endpoint))

        accessID, secretKey, sessionToken = getCloudCredentials(endpoint)
        session = boto3.Session(
            aws_access_key_id=accessID,
            aws_secret_access_key=secretKey,
            aws_session_token=sessionToken,
            region_name=REGION
        )

        sqs = session.client('sqs')
        resp = sqs.get_queue_attributes(QueueUrl=endpoint, AttributeNames=['ApproximateNumberOfMessages'])
        approxNumMsgInQueue = resp['Attributes']['ApproximateNumberOfMessages']

        app_logger.info("-------------------- Successfully get sqs queue attributes ----------------------")
        app_logger.info("subscriptionID: {}".format(subscriptionId))
        app_logger.info("endpoint: {}".format(endpoint))
        app_logger.info("ApproximateNumberOfMessages: {}".format(approxNumMsgInQueue))
        app_logger.info("statusCode: {}".format(resp['ResponseMetadata']['HTTPStatusCode']))
        app_logger.info("response: {}\n".format(resp))

        app_logger.debug("subscriptionId {}, Get attributes ApproximateNumberOfMessages: {} from queue endpoint: {}, status_code: {}, response: {}".format(
                subscriptionId, approxNumMsgInQueue, endpoint, resp['ResponseMetadata']['HTTPStatusCode'], resp))
        return endpoint, approxNumMsgInQueue
    except Exception as err:
        error_message = "subscriptionId {}, Could not get queue attributes, reason: {}".format(subscriptionId, err)
        #error_logger.error(error_message, exc_info=True)
        raise Exception(error_message)


# ==============================================
def startFileNotiAlerts(subscriptionId=None, destinationFolder=None, input=None):
    # ==============================================
    global currentSubscriptionID
    try:
        if subscriptionId is None:
            endpoint, cryptographyKey, currentSubscriptionID = subscribeToFileNoti(input)
            sys.exit(0)
            return
        else:
            endpoint, cryptographyKey, currentSubscriptionID = showActiveSubscriptions(subscriptionId)
            if currentSubscriptionID is None:
                raise Exception("subscriptionID {0} is not found".format(subscriptionId))

        app_logger.info("\n")
        app_logger.info("*************************************************************************")
        app_logger.info("******************* Start consuming messages ****************************")
        app_logger.info("*************************************************************************")
        app_logger.info("Endpoint: {}".format(endpoint))
        app_logger.info("SubscriptionID: {}\n".format(currentSubscriptionID))

        while 1:
            try:
                accessID, secretKey, sessionToken = getCloudCredentials(endpoint)
                app_logger.info("Getting file, press BREAK to exit...\n")
                sqsQueue.startPolling(accessID, secretKey, sessionToken, endpoint, cryptographyKey, currentSubscriptionID, downloadFile, destinationFolder)

            except ClientError as e:
                app_logger.warning("Waiting for 1 second, cloud credentials expired!")
                time.sleep(1)
            except KeyboardInterrupt:
                app_logger.info("User requested break, cleaning up...")
                sys.exit(0)
            except Exception as e:
                app_logger.error("Waiting for 20 second for cool down, found unexpected error {}".format(e), exc_info=True)
                error_logger.error("Waiting for 20 second for cool down, found unexpected error {}".format(e), exc_info=True)
                time.sleep(20)
    except KeyboardInterrupt:
        app_logger.info("User requested break, cleaning up...")
        sys.exit(0)


# ==============================================
def removeSubscription(subscription_id=None):
    # ==============================================

    # get the latest access token
    accessToken = rdpToken.getToken()

    category_URL = "/message-services"
    endpoint_URL = "/file-store/subscriptions" if subscription_id is None else "/file-store/subscriptions?subscriptionID={}".format(subscription_id)
    RESOURCE_ENDPOINT = base_URL + category_URL + RDP_version + endpoint_URL

    app_logger.info("\n")
    if subscription_id:
        app_logger.info("************************************************************************************")
        app_logger.info("****************************** Delete subscriptionID *******************************")
        app_logger.info("************************************************************************************")
        app_logger.info("Request URL: {0}".format(RESOURCE_ENDPOINT))
        app_logger.info("SubscriptionID: {0}".format(subscription_id))
        dResp = requests.delete(RESOURCE_ENDPOINT, headers={"Authorization": "Bearer " + accessToken},
                                params={"subscriptionID": subscription_id})
    else:
        app_logger.info("************************************************************************************")
        app_logger.info("**************************** Delete all subscriptions ******************************")
        app_logger.info("************************************************************************************")
        app_logger.info("Request URL: {0}".format(RESOURCE_ENDPOINT))
        dResp = requests.delete(RESOURCE_ENDPOINT, headers={"Authorization": "Bearer " + accessToken})

    if dResp.status_code > 299:
        app_logger.error("--------------------------- Failed to delete subscriptions ------------------------")
        app_logger.error("Unable to remove subscription. Code {}, Message: {}".format(dResp.status_code, dResp.text), stack_info=True)
        error_logger.error("------------------------- Failed to delete subscriptions ------------------------")
        error_logger.error("Unable to remove subscription. Code {}, Message: {}".format(dResp.status_code, dResp.text), stack_info=True)
    else:
        app_logger.info("--------------------------- Successfully delete subscriptions ----------------------\n")


# ==============================================
def showActiveSubscriptions(subscription_id=None, is_verbose=False):
    # ==============================================

    # get the latest access token
    accessToken = rdpToken.getToken()

    category_URL = "/message-services"
    endpoint_URL = "/file-store/subscriptions" if subscription_id is None else "/file-store/subscriptions?subscriptionID={}".format(subscription_id)
    RESOURCE_ENDPOINT = base_URL + category_URL + RDP_version + endpoint_URL

    app_logger.info("\n")
    app_logger.info("*************************************************************************")
    app_logger.info("******************* Get active subscriptions ****************************")
    app_logger.info("*************************************************************************")
    app_logger.info("Request URL: {0}".format(RESOURCE_ENDPOINT))
    if subscription_id is not None:
        app_logger.info("subscriptionID: {0}".format(subscription_id))
    dResp = requests.get(RESOURCE_ENDPOINT, headers={"Authorization": "Bearer " + accessToken})

    if dResp.status_code != 200:
        raise ValueError("Unable to get subscriptions. Code %s, Message: %s" % (dResp.status_code, dResp.text))
    else:
        jResp = json.loads(dResp.text)
        #print(json.dumps(jResp, indent=2))

    app_logger.info("------------------- Get active subscription result ----------------------")
    if 'subscriptions' in jResp:
        subscription_list = jResp['subscriptions']
        if is_verbose:
            app_logger.info(json.dumps(jResp))
        else:
            try:
                for idx, subscription_info in enumerate(subscription_list):
                    user_info = {
                        "endpoint": subscription_info['transportInfo']['endpoint'],
                        "cryptographyKey": subscription_info['transportInfo']['cryptographyKey'],
                        "subscriptionID": subscription_info['subscriptionID'],
                        "query": subscription_info['query']
                    }
                    app_logger.info("############################ [No. {}] ############################".format(idx+1))
                    app_logger.info("subscriptionID: {}".format(user_info["subscriptionID"]))
                    app_logger.info("endpoint: {}".format(user_info["endpoint"]))
                    app_logger.info("cryptographyKey: {}".format(user_info["cryptographyKey"]))
                    if "query" in user_info:
                        app_logger.info("query: {}".format(user_info["query"]))
                    app_logger.info("##################################################################\n")
            except Exception as err:
                app_logger.error(err, exc_info=True)
                error_logger.error(err, exc_info=True)
                app_logger.info(json.dumps(jResp))

        if len(subscription_list) > 0:
            endpoint = subscription_list[0]['transportInfo']['endpoint']
            cryptography_key = subscription_list[0]['transportInfo']['cryptographyKey']
            current_subscription_id = subscription_list[0]['subscriptionID']
            return endpoint, cryptography_key, current_subscription_id
        else:
            app_logger.info("response = {}\n".format(json.dumps(jResp)))
    else:
        app_logger.info("response = {}\n".format(json.dumps(jResp)))

    return None, None, None


def load_current_user():
    user_object = None
    try:
        '''
        {
            "username": "GE-XXX"
        }
        '''
        # read the token from a file
        tf = open("current_user.json", "r+")
        user_object = json.load(tf)["username"]
        tf.close()
        app_logger.info("Successfully get current user: {}".format(user_object))
    except Exception:
        pass

    return user_object


def read_args():
    try:
        # Read arguments from command line
        args = parser.parse_args()

        args_dict = vars(parser.parse_args())
        app_logger.info("input parameter = {}".format(args_dict))

        if args.get:
            if args.subscriptionId:
                showActiveSubscriptions(args.subscriptionId)
            else:
                showActiveSubscriptions()
        elif args.delete:
            if args.subscriptionId:
                removeSubscription(args.subscriptionId)
            else:
                removeSubscription()
        elif args.create or args.poll:
            if args.poll and (args.subscriptionId is None or args.destinationFolder is None):
                raise Exception("subscriptionId and destinationFolder are required fields, please check via 'python fileNotiMessages.py -h'")
            if args.create and args.input is None:
                raise Exception("input is missing please check via 'python fileNotiMessages.py -h'")
            if args.create:
                startFileNotiAlerts(input=args.input)
            elif args.poll:
                startFileNotiAlerts(subscriptionId=args.subscriptionId, destinationFolder=args.destinationFolder)
        elif args.modify:
            if args.subscriptionId is None or args.input is None:
                raise Exception("subscriptionId and input are required fields, please check via 'python fileNotiMessages.py -h'")
            updateSubscription(args.subscriptionId, args.input)
        elif args.queue:
            if args.subscriptionId is None:
                raise Exception("subscriptionId is required field, please check via 'python fileNotiMessages.py -h'")
            else:
                getQueueAttributes(args.subscriptionId)
        elif args.recovery:
            if args.destinationFolder is None:
                raise Exception("destinationFolder is required fields for recovery failure messages, please check via 'python fileNotiMessages.py -h'")
            recoveryFailureMessages(args.destinationFolder)
        else:
            raise Exception("Found invalid command, please check via 'python fileNotiMessages.py -h'")
    except Exception as err:
        app_logger.error(err, exc_info=True)
        error_logger.error(err, exc_info=True)
        app_logger.info("program exit")


# ==============================================
if __name__ == "__main__":
    # ==============================================
    description = """FileNoti Message Tool description
    1) create a new subscription and specify input json file
     - python fileNotiMessages.py -c -i <json file>
    
    2) poll message queue from existing subscription and specify destination folder
     - python fileNotiMessages.py -p -s <subscriptionId> -d <destination folder>
    
    3) Get all subscriptions 
     - python fileNotiMessages.py -g
    
    4) Get specific subscription
     - python fileNotiMessages.py -g -s <subscriptionId>
    
    5) Update user subscription filter
     - python fileNotiMessages.py -m -s <subscriptionId> -i <json file>
    
    5) Delete all subscriptions
     - python fileNotiMessages.py -u
    
    6)) Delete specific subscription
     - python fileNotiMessages.py -u -s <subscriptionId>
     
    7) Get NumberOfAvailableMessages in sqs queue
     - python fileNotiMessages.py -q -s <subscriptionId>
     
    8) Recovery failure messages
     - python fileNotiMessages.py -r -d <destination folder> 
    """

    # Initialize parser
    parser = argparse.ArgumentParser(description=description, formatter_class=argparse.RawTextHelpFormatter)

    # Adding optional argument
    parser.add_argument("-g", "--get", action='store_true',
                        help="get all of subscriptions information")  # required=True

    parser.add_argument("-c", "--create", action='store_true', help="create a new subscription")

    parser.add_argument("-p", "--poll", action='store_true', help="resume polling message queue from existing subscription")

    parser.add_argument("-m", "--modify", action='store_true', help="modify filter")

    parser.add_argument("-u", "--delete", action='store_true', help="delete all subscriptions")

    parser.add_argument("-s", "--subscriptionId", help="specify subscription id")

    parser.add_argument("-i", "--input", help="specify input request body")

    parser.add_argument("-d", "--destinationFolder", help="specify destination folder to store your file and folder must exist")

    parser.add_argument("-q", "--queue", action='store_true', help="get numberOfAvailableMessages from queue attributes ")

    parser.add_argument("-r", "--recovery", action='store_true', help="recovery failure messages from failure folder")

    try:
        print("Program is started")
        username = rdpToken._loadCredentialsFromFile()
        user_results = load_current_user()
        if user_results is None or str(user_results).strip() == '' or str(user_results) != username:
            if os.path.exists("token.txt"):
                app_logger.info("Remove token.txt because user_results {} match with criteria compare with {}".format(user_results, username))
                os.remove("token.txt")
    except Exception as err:
        app_logger.error(err, exc_info=True)
        error_logger.error(err, exc_info=True)

    read_args()
