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
import boto3
import os
from botocore.exceptions import ClientError
import traceback
import time
import argparse, textwrap
from urllib.parse import urlparse

# Application Constants
base_URL = "https://api.refinitiv.com"
RDP_version = "/v1"
REGION = 'us-east-1'
currentSubscriptionID = None


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

    print(requestData)
    dResp = requests.post(RESOURCE_ENDPOINT, headers=hdrs, data=json.dumps(requestData))
    if dResp.status_code != 200:
        raise ValueError("Unable to subscribe. Code %s, Message: %s" % (dResp.status_code, dResp.text))
    else:
        jResp = json.loads(dResp.text)
        print("Successfully create subscriptionId {}".format(jResp["subscriptionID"]))
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
        raise ValueError("Unable to update subscription. Code %s, Message: %s" % (dResp.status_code, dResp.text))
    else:
        jResp = json.loads(dResp.text)
        print("Successfully update subscriptionId {}".format(jResp["subscriptionID"]))
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
        raise ValueError("Unable to get credentials. Code %s, Message: %s" % (dResp.status_code, dResp.text))
    else:
        jResp = json.loads(dResp.text)
        return jResp["credentials"]["accessKeyId"], jResp["credentials"]["secretKey"], jResp["credentials"][
            "sessionToken"]


def downloadFile(rMessage, subscriptionId, destination_folder):
    try:
        # ==============================================
        pl = rMessage['payload']
        currentAt = pl['FileStoreNotification']['currentAt']
        bucketName = pl['FileStoreNotification']['fileset']['bucketName']
        packageId = pl['FileStoreNotification']['fileset']['packageId']
        filesetName = pl['FileStoreNotification']['fileset']['name']
        #packageName = pl['FileStoreNotification']['fileset']['packageName']
        numFiles    = pl['FileStoreNotification']['fileset']['numFiles']
        file_stream_list = pl['FileStoreNotification']['fileset']['files']['values']

        print('--------- CurrentAt {}, Start Download {} cfs file from bucketName {}, packageId {}, filesetName {}'.format(currentAt, numFiles, bucketName, packageId, filesetName))

        for index, file_stream in enumerate(file_stream_list):
            file_name = file_stream["filename"]
            file_size_bytes = file_stream["fileSizeInBytes"]
            url = file_stream["href"]
            print("Downloading {}/{},  filename {}, file size (bytes) from url {}, into {}".format(index+1, len(file_stream_list), file_name, file_size_bytes, url, destination_folder))
            downloadFileFromURL(url, destination_folder)
    except Exception as e:
        traceback.print_exc()
        print("Could not download file")


def downloadFileFromURL(url, destination_folder):
    requestData = {
        "doNotRedirect": True
    }
    # get the latest access token
    accessToken = rdpToken.getToken()
    resp = requests.get(url, headers={"Authorization": "Bearer " + accessToken}, params=requestData, stream=True)
    if resp.ok:
        jResp = json.loads(resp.text)
        url = jResp["url"]
        a = urlparse(url)
        print("s3_path: {}".format(a.path))  # Output: /kyle/09-09-201315-47-571378756077.jpg
        print("s3_filename: {}".format(os.path.basename(a.path)))  # Output: 09-09-201315-47-571378756077.jpg
        fileResp = requests.get(url, stream=True)
        if fileResp.ok:
            final_path = os.path.join(os.path.abspath(destination_folder), os.path.basename(a.path))
            print("saving to ", final_path)
            with open(final_path, 'wb') as f:
                for chunk in fileResp.iter_content(chunk_size=1024 * 8):
                    if chunk:
                        f.write(chunk)
                        f.flush()
                        os.fsync(f.fileno())
        else:  # HTTP status code 4XX/5XX
            print("Download {} failed: status code {}\n{}".format(url, fileResp.status_code, fileResp.text))
    else:
        print("Could not get cfs signed url: status code {}\n{}".format(resp.status_code, resp.text))


def getQueueAttributes(subscriptionId):
    endpoint, cryptographyKey, currentSubscriptionID = showActiveSubscriptions(subscriptionId)
    if currentSubscriptionID is None:
        raise Exception("subscriptionID {0} is not found".format(subscriptionId))

    print("Getting credentials to connect to AWS Queue {}".format(endpoint))
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
    print("Getting attributes Queue endpoint: {}, ApproximateNumberOfMessages: {}".format(endpoint, approxNumMsgInQueue))
    return endpoint, approxNumMsgInQueue


# ==============================================
def startFileNotiAlerts(subscriptionId=None, destinationFolder=None, input=None):
    # ==============================================
    global currentSubscriptionID
    try:
        print("Subscribing to file stream ...")
        if subscriptionId is None:
            endpoint, cryptographyKey, currentSubscriptionID = subscribeToFileNoti(input)
            return
        else:
            endpoint, cryptographyKey, currentSubscriptionID = showActiveSubscriptions(subscriptionId)
            if currentSubscriptionID is None:
                raise Exception("subscriptionID {0} is not found".format(subscriptionId))

        print("  Queue endpoint: {}, Subscription ID: {}".format(endpoint, currentSubscriptionID))

        while 1:
            try:
                print("Getting credentials to connect to AWS Queue...")
                accessID, secretKey, sessionToken = getCloudCredentials(endpoint)
                print("Queue access ID: %s" % (accessID))
                print("Getting file, press BREAK to exit...")
                sqsQueue.startPolling(accessID, secretKey, sessionToken, endpoint, cryptographyKey, currentSubscriptionID, downloadFile, destinationFolder)

            except ClientError as e:
                print("Cloud credentials expired!")
    except KeyboardInterrupt:
        print("User requested break, cleaning up...")
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
        print("Deleting the open file-store subscription")
        dResp = requests.delete(RESOURCE_ENDPOINT, headers={"Authorization": "Bearer " + accessToken},
                                params={"subscriptionID": subscription_id})
    else:
        print("Deleting ALL open file-store subscriptions")
        dResp = requests.delete(RESOURCE_ENDPOINT, headers={"Authorization": "Bearer " + accessToken})

    if dResp.status_code > 299:
        print(dResp)
        print(rdpToken.UUID)
        print("Warning: unable to remove subscription. Code %s, Message: %s" % (dResp.status_code, dResp.text))
    else:
        print("File-Store unsubscribed!")


# ==============================================
def showActiveSubscriptions(subscription_id=None):
    # ==============================================

    # get the latest access token
    accessToken = rdpToken.getToken()

    category_URL = "/message-services"
    endpoint_URL = "/file-store/subscriptions" if subscription_id is None else "/file-store/subscriptions?subscriptionID={}".format(subscription_id)
    RESOURCE_ENDPOINT = base_URL + category_URL + RDP_version + endpoint_URL

    print("Getting all open file-store subscriptions {0}".format(RESOURCE_ENDPOINT))
    dResp = requests.get(RESOURCE_ENDPOINT, headers={"Authorization": "Bearer " + accessToken})

    if dResp.status_code != 200:
        print("uuid = " + str(rdpToken.UUID))
        raise ValueError("Unable to get subscriptions. Code %s, Message: %s" % (dResp.status_code, dResp.text))
    else:
        jResp = json.loads(dResp.text)
        print(json.dumps(jResp, indent=2))

    if 'subscriptions' in jResp:
        subscription_list = jResp['subscriptions']
        if len(subscription_list) > 0:
            endpoint = subscription_list[0]['transportInfo']['endpoint']
            cryptography_key = subscription_list[0]['transportInfo']['cryptographyKey']
            current_subscription_id = subscription_list[0]['subscriptionID']
            return endpoint, cryptography_key, current_subscription_id

    return None, None, None


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

    # Read arguments from command line
    args = parser.parse_args()

    args_dict = vars(parser.parse_args())
    print(args_dict)

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
            raise Exception("subscriptionId and input are required fields, please check via 'python fileStoreMessages.py -h'")
        updateSubscription(args.subscriptionId, args.input)
    elif args.queue:
        if args.subscriptionId is None:
            raise Exception("subscriptionId is required field, please check via 'python fileStoreMessages.py -h'")
        else:
            getQueueAttributes(args.subscriptionId)
    else:
        raise Exception("Found invalid command, please check via 'python fileStoreMessages.py -h'")

