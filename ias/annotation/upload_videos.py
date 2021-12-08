import os
import json
import argparse
import requests
import utils
import random
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


def get_video_ids_to_upload(args, metadata):
  '''Load a list of selected video ids to upload'''

  # Get the list of videos selected for upload
  with open(args.videos_meta, 'r') as f:
    video_ids = json.load(f)
  logger.info("Selected videos: {}".format(len(video_ids)))  

  # Keep on list only videos that were downloaded
  if args.videos_path:
    video_ids_ = {}
    for video_id in video_ids:
      if os.path.exists(os.path.join(args.videos_path, video_id + '.mp4')):
        video_ids_[video_id] = video_ids[video_id]
    video_ids = video_ids_
    logger.info("Downloaded videos: {}".format(len(video_ids))) 

  # Exclude videos that vere previously uploaded
  previsly_uploaded = get_previously_uploaded_video_ids(metadata)
  logger.info("Previously uploaded videos: {}".format(len(previsly_uploaded)))  
  video_ids_ = {}
  for video_id in video_ids:
    if not video_id in previsly_uploaded:
      video_ids_[video_id] = video_ids[video_id]
  video_ids = video_ids_

  # Restrict the number of uploads to respect app limit
  n_uploads = args.limit - len(previsly_uploaded)
  if n_uploads > len(video_ids):
    logger.info("Videos to upload right now: {}".format(len(video_ids)))
  else:
    video_ids_ = {}
    for video_id in video_ids:
      if len(video_ids_) < n_uploads:
        video_ids_[video_id] = video_ids[video_id]
    video_ids = video_ids_
    logger.info("Videos to upload right now: {}".format(len(video_ids)))

  # Shuffle order of videos if required
  if args.shuffle:
    keys = list(video_ids.keys())
    random.shuffle(keys)
    video_ids_ = {}
    for key in keys:
      video_ids_[key] = video_ids[key]
    video_ids = video_ids_
    logger.info("Videos were shuffled before the upload.")  
    
  return video_ids


def get_previously_uploaded_video_ids(metadata):
  ''' Get ids of videos that were already uploaded to the app '''

  video_ids = []
  failed_input_ids = {}
  
  # Get inputs
  for page in range(1, 11):
    list_inputs_response = stub.ListInputs(
                          service_pb2.ListInputsRequest(page=page, per_page=1000),
                          metadata=metadata
    )

    # Extract video ids for inputs without errors
    for input_object in list_inputs_response.inputs:
      if input_object.status.code != status_code_pb2.INPUT_DOWNLOAD_SUCCESS:
        failed_input_ids[input_object.id] = input_object
      else:
        json_obj = MessageToDict(input_object)
        video_ids.append(json_obj['data']['metadata']['video_id'])

  # Remove failed inputs from the app
  for input_id in failed_input_ids:
    delete_input_response = stub.DeleteInput(
        service_pb2.DeleteInputRequest(input_id=input_id),
        metadata=metadata
    )
    utils.process_response(delete_input_response)
  logger.info("Failed inputs removed from the app: {}".format(len(failed_input_ids)))

  logger.info("Previously successfully uploaded videos: {}".format(len(video_ids)))
  return video_ids


def upload_videos(video_ids, videos_path):
    '''Upload videos from the list'''

    # Keep tack of successful and failed upload
    failed_video_ids = {}
    success_count = 0

    for video_id in tqdm(video_ids, total=len(video_ids)):
      file_bytes = False
      video_file = os.path.join(videos_path, video_id + '.mp4')

      # Load downloaded video if available
      if os.path.exists(video_file):
        with open(video_file, "rb") as f:
          file_bytes = f.read()
      
      # Or make a url request 
      else:
        try:
          url = video_ids[video_id]['url'].replace('playsource=3&', '') # fix for dead links
          r = requests.get(url, allow_redirects=True, timeout=2.5)
          if int(r.headers.get('content-length')) and r.headers.get('content-type') == 'video/mp4':
            file_bytes = r.content
        except:
          failed_video_ids[video_id] = video_ids[video_id]

      # Send post request to upload video
      if file_bytes:
        input_meta = Struct()
        input_meta.update(video_ids[video_id])

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

        # Process failed response
        if utils.get_response_if_failed(post_inputs_response):
          failed_video_ids[video_id] = video_ids[video_id]
        else:
          success_count += 1

    logger.info("Uploaded videos: {}".format(success_count))
    return failed_video_ids


def main(args, metadata):

  logger.info("----- Uploading videos -----")

  # Load ids of videos selected for the upload
  video_ids = get_video_ids_to_upload(args, metadata)

  # Upload videos
  failed_uploads = upload_videos(video_ids, args.videos_path)

  # Save meta about videos with a failed upload
  if failed_uploads:
    utils.save_data(args.save_failed, args.out_path, failed_uploads, args.tag, 'failed_uploads')


if __name__ == '__main__':  
  parser = argparse.ArgumentParser(description="Upload videos.") 
  parser.add_argument('--api_key',
                      default='',
                      help="API key to the required application.") 
  parser.add_argument('--tag',
                    default='',
                    help="Name of the process/application.")         
  parser.add_argument('--videos_path',
                      default='',
                      help="Path to folder with dowloaded videos.")                
  parser.add_argument('--videos_meta', 
                      default='', 
                      help="Path to json file with videos metadata.") 
  parser.add_argument('--limit', 
                      default=10642,
                      type=int, 
                      help="Maximum number of inputs in the app.")  
  parser.add_argument('--shuffle',
                      default=False,
                      type=lambda x: (str(x).lower() == 'true'),
                      help="Shuffle video ids before uploading.")                   
  parser.add_argument('--out_path', 
                      default='', 
                      help="Path to general output directory for this script.")
  parser.add_argument('--save_failed',
                      default=True,
                      type=lambda x: (str(x).lower() == 'true'),
                      help="Save information about failed uploads.")

  args = parser.parse_args()

  metadata = (('authorization', 'Key {}'.format(args.api_key)),)

  main(args, metadata)