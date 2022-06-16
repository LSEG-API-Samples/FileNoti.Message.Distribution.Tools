
from loggingFileNoti import get_app_logger, get_error_logger

import csv
import datetime
import os.path

app_logger = get_app_logger("app_info")
error_logger = get_error_logger("app_error")

ERROR_FIELDS = ['messageId', 'totalErrors', 'lastUpdate', 'bucketName',
                'filesetName', 'fileId', 'errorMessage', 'statusCode', 'url', 'response']
INPUT_ERROR_MAPPING_FILE = "error_mapping.csv"


def read_error_mapping():
    app_logger.info("********************************************************************************")
    app_logger.info("***************************** Read Error Mapping File **************************")
    app_logger.info("********************************************************************************")
    error_mapping_dict = {}

    file_exists = os.path.exists(INPUT_ERROR_MAPPING_FILE)
    if not file_exists:
        return error_mapping_dict

    with open(INPUT_ERROR_MAPPING_FILE) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for line_count, row in enumerate(csv_reader):
            if line_count > 0:
                if len(row) != len(ERROR_FIELDS):
                    continue
                message_id = row[0]
                total_errors = row[1]
                last_update = row[2]
                bucket_name = row[3]
                fileset_name = row[4]
                file_id = row[5]
                error_message = row[6]
                status_code = row[7]
                url = row[8]
                response = row[9]

                error_mapping_dict[construct_error_key(message_id, file_id)] = {
                    'messageId': message_id,
                    'totalErrors': total_errors,
                    'lastUpdate': last_update,
                    'bucketName': bucket_name,
                    'filesetName': fileset_name,
                    'fileId': file_id,
                    'errorMessage': error_message,
                    'statusCode': status_code,
                    'url': url,
                    'response': response
                }

        app_logger.info("input file path {}".format(INPUT_ERROR_MAPPING_FILE))
        app_logger.info("read {} records".format(len(error_mapping_dict)))

    return error_mapping_dict


def construct_error_key(message_id, file_id):
    return "{}|_|{}".format(message_id, file_id)


def write_csv_files(output_file, output_data_dict, is_require_delete_error_mapping=False):
    try:
        if is_require_delete_error_mapping:
            app_logger.error("********************************************************************************")
            app_logger.error("****************************** Delete error mapping ****************************")
            app_logger.error("********************************************************************************")
            os.remove(output_file)
            return

        if len(output_data_dict) == 0:
            return

        output_data_list = []
        for key, value in output_data_dict.items():
            app_logger.debug("key {}, value {}".format(key, value))
            output_data_list.append(value)

        with open(output_file, 'w', newline='') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=ERROR_FIELDS)
            writer.writeheader()
            writer.writerows(output_data_list)

    except Exception as err:
        app_logger.error("********************************************************************************")
        app_logger.error("*********************** Unable to save error mapping file **********************")
        app_logger.error("********************************************************************************")
        error_logger.error("********************************************************************************")
        error_logger.error("*********************** Unable to save error mapping file **********************")
        error_logger.error("********************************************************************************")
        app_logger.errro("cause {}".format(err), exc_info=True)
        error_logger.errro("cause {}".format(err), exc_info=True)
        exit(1)


def get_current_datetime():
    now = datetime.datetime.now()
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")
    app_logger.info("Current date and time: {}".format(now_str))
    return now_str
