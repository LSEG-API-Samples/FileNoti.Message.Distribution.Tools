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
import atexit
import sys
import time
import boto3
import os
from botocore.exceptions import ClientError
import traceback
import argparse
from urllib.parse import urlparse
from halo import Halo

# Application Constants
base_URL = "https://api.refinitiv.com"
RDP_version = "/v1"
REGION = 'us-east-1'
currentSubscriptionID = None
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

    dResp = requests.post(RESOURCE_ENDPOINT, headers=hdrs, data=json.dumps(requestData))
    if dResp.status_code != 200:
        #error_logger.error("Unable to subscribe. Code %s, Message: %s" % (dResp.status_code, dResp.text))
        raise ValueError("Unable to subscribe. Code %s, Message: %s" % (dResp.status_code, dResp.text))
    else:
        jResp = json.loads(dResp.text)
        app_logger.info("Successfully create subscriptionId {} with response {}".format(jResp["subscriptionID"], jResp))
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

    print(input_json)
    dResp = requests.put(RESOURCE_ENDPOINT, headers=hdrs, data=json.dumps(input_json))
    if dResp.status_code != 200:
        #error_logger.error("Failed to update subscription {} response {}".format(subscriptionId, dResp))
        raise ValueError("Unable to update subscription %s. Code %s, Message: %s" % (subscriptionId, dResp.status_code, dResp.text))
    else:
        jResp = json.loads(dResp.text)
        app_logger.info("Successfully update subscriptionId {} with response {}".format(jResp["subscriptionID"], jResp))
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

    # get the latest access token
    accessToken = rdpToken.getToken()
    dResp = requests.get(RESOURCE_ENDPOINT, headers={"Authorization": "Bearer " + accessToken}, params=requestData)
    if dResp.status_code != 200:
        #error_logger.error("Failed to get cloud credentials from endpoint {} response {}".format(endpoint, dResp))
        raise ValueError("Unable to get credentials. Code %s, Message: %s" % (dResp.status_code, dResp.text))
    else:
        jResp = json.loads(dResp.text)
        app_logger.info("Successfully get cloud credential from endpoint {} with response {}".format(endpoint, jResp))
        return jResp["credentials"]["accessKeyId"], jResp["credentials"]["secretKey"], jResp["credentials"]["sessionToken"]


def downloadFile(rMessage, subscriptionId, destination_folder):
    try:
        # ==============================================
        pl = rMessage['payload']
        currentAt = pl['FileStoreNotification']['currentAt']
        bucketName = pl['FileStoreNotification']['fileset']['bucketName']
        packageId = pl['FileStoreNotification']['fileset']['packageId']
        filesetName = pl['FileStoreNotification']['fileset']['name']
        packageName = pl['FileStoreNotification']['fileset']['packageName']
        numFiles    = pl['FileStoreNotification']['fileset']['numFiles']
        file_stream_list = pl['FileStoreNotification']['fileset']['files']['values']

        app_logger.info("############################################################################################")
        app_logger.info('subscriptionId {}, Trigger At {}, Start Download {} cfs file from bucketName {}, packageName {}, filesetName {}, destination_folder {}'.format(
            subscriptionId, currentAt, numFiles, bucketName, packageName, filesetName, destination_folder))

        for index, file_stream in enumerate(file_stream_list):
            file_name = file_stream["filename"]
            file_size_bytes = file_stream["fileSizeInBytes"]
            url = file_stream["href"]
            app_logger.info("############################################################################################")
            #app_logger.info("subscriptionId {}, Downloading {}/{},  filename {}, file size {} (bytes) from url {}, into {}".format(subscriptionId, index+1, len(file_stream_list), file_name, file_size_bytes, url, destination_folder))
            output_directory = os.path.join(os.path.abspath(destination_folder), bucketName, filesetName)
            app_logger.info("subscriptionId {}, getting s3 signed url from {} of filename {}, file size {} (bytes), output_folder {}".format(subscriptionId, url, file_name, file_size_bytes, output_directory))
            downloadFileFromURL(url, output_directory, subscriptionId, index + 1, len(file_stream_list), file_size_bytes)
    except Exception as e:
        app_logger.error("subscriptionId {}, could not download file from {}, reason: {}".format(subscriptionId, rMessage, e), exc_info=True)
        error_logger.error("subscriptionId {}, could not download file from {}, reason: {}".format(subscriptionId, rMessage, e), exc_info=True)


def downloadFileFromURL(file_stream_url, output_directory, subscription_id, file_no, num_files, file_size_bytes):
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
            app_logger.info("subscriptionId {}, path {} does not exist then create it".format(subscription_id, output_directory))
            os.makedirs(output_directory)

        app_logger.info("############################################################################################")
        app_logger.info("subscriptionId {}, downloading {}/{}, s3_filename {}, file size {} (bytes), s3_file_path {} into {}".format(
                subscription_id, file_no, num_files, s3_file_name, file_size_bytes, s3_path, final_path))

        fileResp = requests.get(url, stream=True)
        if fileResp.ok:
            with open(final_path, 'wb') as f:
                for chunk in fileResp.iter_content(chunk_size=1024 * 8):
                    if chunk:
                        f.write(chunk)
                        f.flush()
                        os.fsync(f.fileno())
            app_logger.info("############################################################################################")
            app_logger.info("subscriptionId {}, Successfully download {}/{}, s3_filename {}, file size {} (bytes), s3_file_path {} into {}".format(
                    subscription_id, file_no, num_files, s3_file_name, file_size_bytes, s3_path, final_path))

        else:  # HTTP status code 4XX/5XX
            error_message = "subscriptionId {}, Failed to download {}/{}, s3_filename {}, file_stream_url {}, url {}, status code {}\n{}".format(
                subscription_id, file_no, num_files, s3_file_name, file_stream_url, url, fileResp.status_code, fileResp.text)
            app_logger.error(error_message, stack_info=True)
            error_logger.error(error_message, stack_info=True)
    else:
        error_message = "subscriptionId {}, Could not get s3 signed url to download {}/{}, file_stream {}, status code {}\n{}".format(
            subscription_id, file_no, num_files, file_stream_url, resp.status_code, resp.text)
        app_logger.error(error_message, stack_info=True)
        error_logger.error(error_message, stack_info=True)


def getQueueAttributes(subscriptionId):
    try:
        endpoint, cryptographyKey, currentSubscriptionID = showActiveSubscriptions(subscriptionId)
        if currentSubscriptionID is None:
            raise Exception("subscriptionID {0} is not found".format(subscriptionId))

        app_logger.info("subscriptionId {}, Getting credentials to connect to AWS Queue {}".format(subscriptionId, endpoint))
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
        app_logger.info("subscriptionId {}, Get attributes ApproximateNumberOfMessages: {} from queue endpoint: {}, status_code: {}, response: {}".format(
                subscriptionId, approxNumMsgInQueue, endpoint, resp.status_code, resp.text))
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
        app_logger.info("Subscribing to file stream ...")
        if subscriptionId is None:
            endpoint, cryptographyKey, currentSubscriptionID = subscribeToFileNoti(input)
            return
        else:
            endpoint, cryptographyKey, currentSubscriptionID = showActiveSubscriptions(subscriptionId)
            if currentSubscriptionID is None:
                raise Exception("subscriptionID {0} is not found".format(subscriptionId))

        app_logger.info("Queue endpoint: {}, Subscription ID: {}".format(endpoint, currentSubscriptionID))

        while 1:
            try:
                app_logger.info("Getting credentials to connect to AWS Queue...")
                accessID, secretKey, sessionToken = getCloudCredentials(endpoint)
                app_logger.info("Queue access ID: %s" % (accessID))
                app_logger.info("Getting file, press BREAK to exit...")
                sqsQueue.startPolling(accessID, secretKey, sessionToken, endpoint, cryptographyKey, currentSubscriptionID, downloadFile, destinationFolder)

            except ClientError as e:
                app_logger.info("Cloud credentials expired!")
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

    if subscription_id:
        app_logger.info("Deleting file-store subscription")
        dResp = requests.delete(RESOURCE_ENDPOINT, headers={"Authorization": "Bearer " + accessToken},
                                params={"subscriptionID": subscription_id})
    else:
        app_logger.info("Deleting all file-store subscriptions")
        dResp = requests.delete(RESOURCE_ENDPOINT, headers={"Authorization": "Bearer " + accessToken})

    if dResp.status_code > 299:
        app_logger.error("Warning: unable to remove subscription. Code {}, Message: {}".format(dResp.status_code, dResp.text), stack_info=True)
        error_logger.error("Warning: unable to remove subscription. Code {}, Message: {}".format(dResp.status_code, dResp.text), stack_info=True)
    else:
        app_logger.info("Successfully remove file-store subscriptions")


# ==============================================
def showActiveSubscriptions(subscription_id=None, is_verbose=False):
    # ==============================================

    # get the latest access token
    accessToken = rdpToken.getToken()

    category_URL = "/message-services"
    endpoint_URL = "/file-store/subscriptions" if subscription_id is None else "/file-store/subscriptions?subscriptionID={}".format(subscription_id)
    RESOURCE_ENDPOINT = base_URL + category_URL + RDP_version + endpoint_URL

    app_logger.info("Getting active subscriptions from file-store subscriptions {0}".format(RESOURCE_ENDPOINT))
    dResp = requests.get(RESOURCE_ENDPOINT, headers={"Authorization": "Bearer " + accessToken})

    if dResp.status_code != 200:
        raise ValueError("Unable to get subscriptions. Code %s, Message: %s" % (dResp.status_code, dResp.text))
    else:
        jResp = json.loads(dResp.text)
        #print(json.dumps(jResp, indent=2))

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
                    app_logger.info("[{}] ###########################################################".format(idx+1))
                    app_logger.info("subscriptionID: {}".format(user_info["subscriptionID"]))
                    app_logger.info("endpoint: {}".format(user_info["endpoint"]))
                    app_logger.info("cryptographyKey: {}".format(user_info["cryptographyKey"]))
                    app_logger.info("query: {}".format(user_info["query"]))
                    app_logger.info("################################################################")
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
            app_logger.info(json.dumps(jResp))
    else:
        app_logger.info(json.dumps(jResp))

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
        app_logger.info("successfully get current user: {}".format(user_object))
    except Exception:
        pass

    return user_object


def read_args():
    try:
        # Read arguments from command line
        args = parser.parse_args()

        args_dict = vars(parser.parse_args())
        app_logger.info(args_dict)

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
                raise Exception("subscriptionId and destinationFolder are required fields, please check via 'python fileStoreMessages.py -h'")
            if args.create and args.input is None:
                raise Exception("input is missing please check via 'python fileStoreMessages.py -h'")
            if args.create:
                startFileNotiAlerts(input=args.input)
            elif args.poll:
                startFileNotiAlerts(subscriptionId=args.subscriptionId, destinationFolder=args.destinationFolder)
        elif args.modify:
            if args.subscriptionId is None or args.input is None:
                raise Exception(
                    "subscriptionId and input are required fields, please check via 'python fileStoreMessages.py -h'")
            updateSubscription(args.subscriptionId, args.input)
        elif args.queue:
            if args.subscriptionId is None:
                raise Exception("subscriptionId is required field, please check via 'python fileStoreMessages.py -h'")
            else:
                getQueueAttributes(args.subscriptionId)
        else:
            raise Exception("Found invalid command, please check via 'python fileStoreMessages.py -h'")
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
     - python fileNotiMessages.py --q -s <subscriptionId>
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

    try:
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
