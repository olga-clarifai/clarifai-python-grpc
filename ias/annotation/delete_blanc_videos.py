import os
import json
import argparse
import requests
import utils
import random
from tqdm import tqdm
import csv

import ground_truth as gt

# Import in the Clarifai gRPC based objects needed
from clarifai_grpc.channel.clarifai_channel import ClarifaiChannel
from clarifai_grpc.grpc.api import resources_pb2, service_pb2, service_pb2_grpc
from clarifai_grpc.grpc.api.status import status_code_pb2
from google.protobuf.json_format import MessageToDict
from google.protobuf.struct_pb2 import Struct

# Setup logging
logger = utils.setup_logging()

# Construct the communications channel and the object stub to call requests on.
channel = ClarifaiChannel.get_json_channel()
stub = service_pb2_grpc.V2Stub(channel)


def get_inputs(metadata):

  inputs = {}
  
  # Get inputs
  for page in range(1, 11):
    list_inputs_response = stub.ListInputs(
                          service_pb2.ListInputsRequest(page=page, per_page=1000),
                          metadata=metadata
    )
    utils.process_response(list_inputs_response)

    # Extract video ids for inputs without errors
    for input_object in list_inputs_response.inputs:
      hosted = input_object.data.video.hosted
      inputs[input_object.id] = f"{hosted.prefix}/orig/{hosted.suffix}"

  logger.info("Fetched videos: {}".format(len(inputs)))
  return inputs


def remove_blanc_videos(metadata, inputs):

  # Test each video leangth
  blanc_video_ids = []
  for input_id, url in tqdm(inputs.items(), total=len(inputs)):
    try:
      r = requests.get(url, allow_redirects=True, timeout=5)
      if int(r.headers.get('content-length')) == 40467:
        blanc_video_ids.append(input_id)
    except:
      continue

  # Remove failed inputs from the app
  for input_id in blanc_video_ids:
    delete_input_response = stub.DeleteInput(
        service_pb2.DeleteInputRequest(input_id=input_id),
        metadata=metadata
    )
    utils.process_response(delete_input_response)
  logger.info("Blanc videos removed: {}".format(len(blanc_video_ids)))


def main(args, metadata):

  inputs = get_inputs(metadata)
  remove_blanc_videos(metadata, inputs)


if __name__ == '__main__':  
  parser = argparse.ArgumentParser(description="Upload videos.") 
  parser.add_argument('--api_key',
                      default='',
                      help="API key to the required application.")         

  args = parser.parse_args()

  metadata = (('authorization', 'Key {}'.format(args.api_key)),)

  main(args, metadata)