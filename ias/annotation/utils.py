import sys
import os
import logging
import json

from clarifai_grpc.grpc.api.status import status_code_pb2


def setup_logging():
    logging.basicConfig(format='%(asctime)s %(message)s \t')
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    return logger


def show_progress_bar(i, total):
    done = int(100 * i / total)
    sys.stdout.write("\r[%s%s] %d%s complete " % ('=' * done, ' ' * (100-done), done, '%'))    
    sys.stdout.flush()
    if i == total:
        sys.stdout.write("\n")    
        sys.stdout.flush()


def process_response(response):
    if response.status.code != status_code_pb2.SUCCESS:
        logging.error("There was an error with your request!")
        logging.error("\tDescription: {}".format(response.status.description))
        logging.error("\tDetails: {}".format(response.status.details))
        raise Exception("Request failed, status code: " + str(response.status.code))


def get_response_if_failed(response):
    if response.status.code != status_code_pb2.SUCCESS:
        return {'code': response.status.code, 
                'description': response.status.description, 
                'details': response.status.details}
    else:
        return False
  

def save_data(to_save, out_path, data, tag, name):
  ''' Dump provided data to a json file '''

  if to_save:
    # Create output dir if needed
    path = os.path.join(out_path, name)
    if not os.path.exists(path):
      os.mkdir(path)

    # Set file path
    file_path = os.path.join(path, "{}_{}.json".
                             format(tag, name))

    # Write to file
    with open(file_path, 'w') as f:
      json.dump(data, f)