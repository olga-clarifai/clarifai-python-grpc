import os
import json
import argparse
import logging

# Import in the Clarifai gRPC based objects needed
from clarifai_grpc.channel.clarifai_channel import ClarifaiChannel
from clarifai_grpc.grpc.api import resources_pb2, service_pb2, service_pb2_grpc
from clarifai_grpc.grpc.api.status import status_code_pb2
from google.protobuf.json_format import MessageToDict
from google.protobuf.struct_pb2 import Struct

# Setup logging
logging.basicConfig(format='%(asctime)s %(message)s \t')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Construct the communications channel and the object stub to call requests on.
channel = ClarifaiChannel.get_json_channel()
stub = service_pb2_grpc.V2Stub(channel)


def failed_upload(response):
    if response.status.code != status_code_pb2.SUCCESS:
        return {'code': response.status.code, 'description': response.status.description, 'details': response.status.details}
    else:
        return False


def load_selected_video_ids(selected_videos_path):
  '''Load a list of selected video ids to upload'''

  with open(selected_videos_path, 'r') as f:
    video_ids = json.load(f)

  logging.info("List of selected videos loaded: {} videos to upload".format(len(video_ids)))  
  return video_ids


def upload_videos(video_ids, videos_path):
    '''Upload videos from the list'''

    # List to keep tack of failed upload
    failed_uploads = {}

    for video_id in video_ids:
      # Set metadata
      input_meta = Struct()
      input_meta.update(video_ids[video_id])

      video_file = os.path.join(videos_path, video_id + '.mp4')
      with open(video_file, "rb") as f:
        # Load file
        file_bytes = f.read()

        # Send post request to upload video
        post_inputs_response = stub.PostInputs(
            service_pb2.PostInputsRequest(
                inputs=[
                    resources_pb2.Input(
                        data=resources_pb2.Data(
                            video=resources_pb2.Video(base64=file_bytes),
                            metadata=input_meta
                        )
                    )
                ]
            ),
            metadata=metadata
        )

        # Process response
        if failed_upload(post_inputs_response):
          failed_uploads[video_id] = failed_upload(post_inputs_response)
          logging.info("Upload of {} failed.".format(video_id))  
        else:
          logging.info("Video {} was successfully uploaded.".format(video_id))


def main(args, metadata):

  logger.info("----- Uploading videos -----")

  # Load ids of videos selected for the upload
  video_ids = load_selected_video_ids(args.selected_videos)

  # Upload videos
  upload_videos(video_ids, args.videos)


if __name__ == '__main__':  
  parser = argparse.ArgumentParser(description="Upload videos.")
  parser.add_argument('--api_key',
                      default='',
                      help="API key to the required application.")        
  parser.add_argument('--videos',
                      default='',
                      help="Path to folder with videos to load.")                
  parser.add_argument('--selected_videos', 
                      default='', 
                      help="Path to json file with video ids of videos selected for upload.") 

  args = parser.parse_args()

  metadata = (('authorization', 'Key {}'.format(args.api_key)),)
  args.safe_gt_label = 'safe'

  main(args, metadata)