import os
import json
import argparse
import requests
import utils
from tqdm import tqdm

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

def get_uploaded_inputs(metadata):
  ''' Get ids of videos that were already uploaded to the app '''

  video_ids = {}
  
  # Get inputs
  for page in range(1, 11):
    list_inputs_response = stub.ListInputs(
                          service_pb2.ListInputsRequest(page=page, per_page=1000),
                          metadata=metadata
    )

    # Extract video ids for inputs without errors
    for input_object in list_inputs_response.inputs:
        json_obj = MessageToDict(input_object)
        video_id = json_obj['data']['metadata']['video_id']
        input_id = False # TODO
        video_ids[video_id] = input_id

  logger.info("Previously uploaded videos: {}".format(len(video_ids)))
  return video_ids


def get_input_ids_to_delete(args, metadata):
  '''Load a list of selected video ids to upload'''

  # Get the list of videos selected for upload
  with open(args.videos_meta, 'r') as f:
    video_ids = json.load(f)
  logger.info("Curated videos: {}".format(len(video_ids)))  

  # Keep on list only videos that were downloaded
  if args.videos_path:
    video_ids_ = {}
    for video_id in video_ids:
      if os.path.exists(os.path.join(args.videos_path, video_id + '.mp4')):
        video_ids_[video_id] = video_ids[video_id]
    video_ids = video_ids_
    logger.info("Downloaded videos: {}".format(len(video_ids))) 

  # Extract input ids that were uploaded but have to be removed
  uploaded_video_ids = get_uploaded_inputs(metadata)
  input_ids = []
  for video_id in uploaded_video_ids:
    if video_id not in video_ids:
      input_ids.append(uploaded_video_ids[video_id])
  logger.info("Inputs to delete: {}".format(len(input_ids)))     
    
  return input_ids


def delete_inputs(input_ids_to_remove, metadata):

  # Remove inputs
  delete_inputs_response = stub.DeleteInputs(
      service_pb2.DeleteInputsRequest(
          ids=[input_ids_to_remove]
      ),
      metadata=metadata
  )

  # Process failed response
  if utils.get_response_if_failed(delete_inputs_response):
    logger.info("Deletion failed.")
  else:
    logger.info("Inputs successfully deleted.")
  

def main(args, metadata):

  # Get ids of inputs to delete
  input_ids = get_input_ids_to_delete(args, metadata)

  # Delete seletced inputs
  delete_inputs(input_ids, metadata)



if __name__ == '__main__':  
  parser = argparse.ArgumentParser(description="Remove input videos from app.") 
  parser.add_argument('--api_key',
                      default='3541bcec8c674006b8aab586ad286b9c',
                      help="API key to the required application.") 
  parser.add_argument('--tag',
                    default='DE_Batch1',
                    help="Name of the process/application.")         
  parser.add_argument('--videos_path',
                      default='/Users/olgadergachyova/Downloads/DATA/videos/DE/batch1_curated',
                      help="Path to folder with dowloaded videos.")                
  parser.add_argument('--videos_meta', 
                      default='/Users/olgadergachyova/work/ias/clarifai-python-grpc/ias/annotation/output/curated_videos/DE/DE_batch1_curated_videos.json', 
                      help="Path to json file with videos metadata.") 

  args = parser.parse_args()

  metadata = (('authorization', 'Key {}'.format(args.api_key)),)

  main(args, metadata)