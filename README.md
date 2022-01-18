# FileNoti.Message.Distribution.Tools
## Overview

![](FileNotiWorkflow.png)

## Setup

1. If you didn't have python3.7 yet please install it via https://www.python.org/ftp/python/3.7.11/Python-3.7.11.tgz
2. Run command to install python libraries 
   - python3 -m pip install -r libs.txt
3. If you encounter error ModuleNotFoundError: No module named 'Crypto' please follow step below
   - python3 -m pip uninstall crypto
   - python3 -m pip install pycrypto
   - python3 -m pip install -r libs.txt
4. **Go to folder name msg_dist_tools** and open file credentials.ini and specify all information (If you don't know information please contact https://developers.refinitiv.com)
5. Run Program please check Tool Description section
6. Messages will be stored under metadata/<subscriptionId> folder
7. FileNoti file will be downloaded into your destination folder

## Tools Description

1. **Help Command**
      ```
      python fileNotiMessages.py -h
      ```

2. **Create a new subscription and specify input json file**
      ```
      python fileNotiMessages.py -c -i requestBody/<json file>
      ```
      Example
      ```
      python fileNotiMessages.py -c -i requestBody/singleBucketFilter.json
      ```
      

3. **Poll message queue from existing subscription and specify destination folder**
      ```
      python fileNotiMessages.py -p -s <subscriptionId> -d <destination folder>
      ```
      Example
      ```
      python fileNotiMessages.py -p -s xxxx-xxxx-xxxx-xxxx -d C:\msg_dist_python_tools\cfs_download
      ```

4. **Get all subscriptions**
      ```
      python fileNotiMessages.py -g
      ```

5. **Get specific subscription**
      ```
      python fileNotiMessages.py -g -s <subscriptionId>
      ```
      Example
      ```
      python fileNotiMessages.py -g -s xxxx-xxxx-xxxx-xxxx
      ```

6. **Update user subscription filter**
      ```
      python fileNotiMessages.py -m -s <subscriptionId> -i requestBody/<json file>
      ```
      Example
      ```
      python fileNotiMessages.py -m -s xxxx-xxxx-xxxx-xxxx -i requestBody/multipleBucketFilter.json
      ```


7. **Delete all subscriptions**
      ```
      python fileNotiMessages.py -u
      ```

8. **Delete specific subscription**
      ```
      python fileNotiMessages.py -u -s <subscriptionId>
      ```
      Example
      ```
      python fileNotiMessages.py -u -s xxxx-xxxx-xxxx-xxxx
      ```

9. **Get NumberOfAvailableMessages in sqs queue**
      ```
      python fileNotiMessages.py --q -s <subscriptionId>
      ```
      Example
      ```
      python fileNotiMessages.py --q -s xxxx-xxxx-xxxx-xxxx
      ```


