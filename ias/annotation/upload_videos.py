import os
import json
import argparse
import utils

# Import in the Clarifai gRPC based objects needed
from clarifai_grpc.channel.clarifai_channel import ClarifaiChannel
from clarifai_grpc.grpc.api import resources_pb2, service_pb2, service_pb2_grpc
from google.protobuf.json_format import MessageToDict
from google.protobuf.struct_pb2 import Struct
# from proto.clarifai.api.resources_pb2 import Video

# Setup logging
logger = utils.setup_logging()

# Construct the communications channel and the object stub to call requests on.
channel = ClarifaiChannel.get_json_channel()
stub = service_pb2_grpc.V2Stub(channel)


def get_video_ids_to_upload(args, metadata):
  '''Load a list of selected video ids to upload'''

  # Get the list of videos selected for upload
  with open(args.selected_videos, 'r') as f:
    video_ids = json.load(f)
  logger.info("Selected videos: {}".format(len(video_ids)))  

  # Keep on list only videos that were downloaded
  if args.downloaded_videos:
    video_ids_ = {}
    for video_id in video_ids:
      if os.path.exists(os.path.join(args.downloaded_videos, video_id + '.mp4')):
        video_ids_[video_id] = video_ids[video_id]
    video_ids = video_ids_
    logger.info("Downloaded videos: {}".format(len(video_ids))) 

  # Exclude videos that vere previously uploaded
  previsly_uploaded = get_previously_uploaded_video_ids(metadata)
  video_ids_ = {}
  for video_id in video_ids:
    if not video_id in previsly_uploaded:
      video_ids_[video_id] = video_ids[video_id]
  video_ids = video_ids_
  logger.info("Videos to upload right now: {}".format(len(video_ids)))     
    
  return video_ids


def get_previously_uploaded_video_ids(metadata):
  ''' Get ids of videos that were already uploaded to the app '''

  video_ids = []
  
  # Get inputs
  for page in range(1, 11):
    list_inputs_response = stub.ListInputs(
                          service_pb2.ListInputsRequest(page=page, per_page=1000),
                          metadata=metadata
    )
    utils.process_response(list_inputs_response)

    # Extract video ids
    for input_object in list_inputs_response.inputs:
      json_obj = MessageToDict(input_object)
      video_ids.append(json_obj['data']['metadata']['video_id'])

  logger.info("Previously uploaded videos: {}".format(len(video_ids)))
  return video_ids


def upload_videos(video_ids, downloaded_videos_path):
    '''Upload videos from the list'''

    # List to keep tack of failed upload
    failed_uploads = {}

    for i, video_id in enumerate(video_ids):
      # Set metadata
      input_meta = Struct()
      input_meta.update(video_ids[video_id])

      video_file = os.path.join(downloaded_videos_path, video_id + '.mp4')
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

        # Process failed response
        if utils.get_response_if_failed(post_inputs_response):
          failed_uploads[video_id] = video_ids[video_id]

      # Progress bar
      utils.show_progress_bar(i+1, len(video_ids))

    return failed_uploads


def print_fails(failed_uploads):

  if failed_uploads:
    print(' ---------- Faild upload --------- ')
    for video_id in failed_uploads:
      print(video_id)

def main(args, metadata):

  logger.info("----- Uploading videos -----")

  # Load ids of videos selected for the upload
  video_ids = get_video_ids_to_upload(args, metadata)

  # Upload videos
  failed_uploads = upload_videos(video_ids, args.downloaded_videos)

  # Save meta about videos with a failed upload
  utils.save_data(args.save_failed, args.out_path, failed_uploads, args.tag, 'failed_uploads')


if __name__ == '__main__':  
  parser = argparse.ArgumentParser(description="Upload videos.") 
  parser.add_argument('--api_key',
                      default='',
                      help="API key to the required application.") 
  parser.add_argument('--tag',
                    default='',
                    help="Name of the process/application.")         
  parser.add_argument('--downloaded_videos',
                      default='',
                      help="Path to folder with dowloaded videos.")                
  parser.add_argument('--selected_videos', 
                      default='', 
                      help="Path to json file with video ids of videos selected for upload.") 
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