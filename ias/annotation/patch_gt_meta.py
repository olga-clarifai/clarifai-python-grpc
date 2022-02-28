import argparse
from tqdm import tqdm
import numpy as np
import utils

# Import in the Clarifai gRPC based objects needed
from clarifai_grpc.channel.clarifai_channel import ClarifaiChannel
from clarifai_grpc.grpc.api import resources_pb2, service_pb2, service_pb2_grpc
from clarifai_grpc.grpc.api.status import status_code_pb2
from google.protobuf.json_format import MessageToDict
from google.protobuf.struct_pb2 import Struct

import ground_truth as gt

# Setup logging
logger = utils.setup_logging()

# Construct the communications channel and the object stub to call requests on.
channel = ClarifaiChannel.get_json_channel()
stub = service_pb2_grpc.V2Stub(channel)


def get_video_ids(args):
  print("Retrieving inputs...")
  video_ids = {} # video_ids to input_ids

  last_id = ''
  while True:
    # Make request
    req = service_pb2.StreamInputsRequest(per_page=1000, last_id=last_id)
    response = stub.StreamInputs(req, metadata=args.metadata)
    utils.process_response(response)

    # Process inputs
    if len(response.inputs) == 0:
      break
    else:
      for input in response.inputs:
        video_id = MessageToDict(input)['data']['metadata']['video_id']
        video_ids[video_id] = input.id

    # Set id for next stream
    last_id = response.inputs[-1].id

  print(f"Total of {len(video_ids)} inputs retrieved")
  return video_ids



def add_ground_truth_to_metadata(args, ground_truth, video_ids):
  for video_id in tqdm(video_ids, total=len(video_ids)):
    if video_id in ground_truth:
      input_metadata = Struct()
      input_metadata.update({"ground_truth": ground_truth[video_id]})
      response = stub.PatchInputs(
        service_pb2.PatchInputsRequest(
          action="merge", 
          inputs=[
            resources_pb2.Input(
              id=video_ids[video_id],
              data=resources_pb2.Data(metadata=input_metadata)
            )
          ]
        ),
        metadata=args.metadata
      )
      utils.process_response(response)

    else:
      delete_input_response = stub.DeleteInput(
        service_pb2.DeleteInputRequest(input_id=video_ids[video_id]),
        metadata=args.metadata
      )
      utils.process_response(delete_input_response)
      logger.info(f"Removed input without ground truth: {video_ids[video_id]}")


def main(args):

  ground_truth = gt.load_all_from_csv(args.videos_meta, 'safe')
  video_ids = get_video_ids(args)
  add_ground_truth_to_metadata(args, ground_truth, video_ids)
  print("Done!")


if __name__ == '__main__':  
  parser = argparse.ArgumentParser(description="Split inputs into groups for labeling.") 
  parser.add_argument('--api_key',
                      default='',
                      help="API key of the application.")                   
  parser.add_argument('--videos_meta', 
                      default='', 
                      help="Path to csv file with videos metadata and ground truth.") 

  args = parser.parse_args()
  args.metadata = (('authorization', 'Key {}'.format(args.api_key)),)

  main(args)