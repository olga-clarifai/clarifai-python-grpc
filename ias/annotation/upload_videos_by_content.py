import argparse
import requests
import csv
from tqdm import tqdm

# Import in the Clarifai gRPC based objects needed
from clarifai_grpc.channel.clarifai_channel import ClarifaiChannel
from clarifai_grpc.grpc.api import resources_pb2, service_pb2, service_pb2_grpc
from clarifai_grpc.grpc.api.status import status_code_pb2
from google.protobuf.struct_pb2 import Struct


# Construct the communications channel and the object stub to call requests on.
channel = ClarifaiChannel.get_json_channel()
stub = service_pb2_grpc.V2Stub(channel)


def get_video_ids_to_upload(args):

  video_ids = {}
  with open(args.videos, 'r') as f:
    reader = csv.DictReader(f)
    for line in reader:
      line = {k.lower(): v for k, v in line.items()}
      meta = {'video_id': line['video_id'], 
              'description': line['video_description'], 
              'url': line['video_url']}
      video_ids[meta['video_id']] = meta

  print("Videos to upload: {}".format(len(video_ids)))
    
  return video_ids


def check_upload(args, video_id):

  # Fetch information about uploaded videos
  input_response = stub.GetInput(
    service_pb2.GetInputRequest(input_id=video_id),
    metadata=args.metadata
  )    

  # Check if the video is hosted by the platform.
  # If no, the upload in reality failed
  success = False
  if input_response.input.data.video.hosted.suffix:
    success = True

  return success


def delete_failed(args, video_id):

  delete_input_response = stub.DeleteInput(
    service_pb2.DeleteInputRequest(input_id=video_id),
    metadata=args.metadata
  )  
        

def upload_videos_by_content(args, video_ids):

  # Keep tack of successful and failed downloads/upload
  success_count = 0
  failed_videos = {} # video_id: code
  download_attempts = {} # video_id: number of attempts
  upload_attempts = {} # video_id: number of attempts

  for video_id in tqdm(video_ids, total=len(video_ids), desc='Downloading and uploading videos...'):
    
    # Download first
    content = None
    for i in range(args.max_attempts): # Re-try until downloaded or reached max number of attempts
      # Update number of downloading attempts
      download_attempts[video_id] = i+1

      # Attempt to download
      try:
        r = requests.get(video_ids[video_id]['url'], allow_redirects=True, timeout=2.5)
        if int(r.headers.get('content-length')) and r.headers.get('content-type') == 'video/mp4':
          content = r.content
          break
      except:
        continue # Make another attemps

    # Save id if download failed
    if content is None:
      failed_videos[video_id] = 'Download failed'

    # Continue with the upload if downloaded successfully
    else:
      for i in range(args.max_attempts): # Re-try until uploaded or reached max number of attempts

        # Update number of uploading attempts
        upload_attempts[video_id] = i+1

        # Set metadata and correct url
        input_meta = Struct()
        input_meta.update(video_ids[video_id])

        # Make request
        post_inputs_response = stub.PostInputs(
          service_pb2.PostInputsRequest(
            inputs=[
              resources_pb2.Input(
                id=video_id,
                data=resources_pb2.Data(
                  video=resources_pb2.Video(base64=content),
                  metadata=input_meta
                )
              )
            ]
          ),
          metadata=args.metadata
        )    

        # Process response and check if videos is hosted
        success = False
        if post_inputs_response.status.code == status_code_pb2.SUCCESS:
          if check_upload(args, video_id):
            success = True
            success_count += 1 
            break 

        # If not hosted, delete correspoding input
        if not success:  
          delete_failed(args, video_id)

        # Save id and status code if completely failed 
        if i == args.max_attempts - 1: # Last unsuccessful attempt
          failed_videos[video_id] = post_inputs_response.status.code

  print("Succesfully uploaded videos: {}".format(success_count))
  return failed_videos, download_attempts, upload_attempts


def main(args):

  # Load ids of videos selected for the upload
  video_ids = get_video_ids_to_upload(args)

  # Upload videos
  failed_videos, download_attempts, upload_attempts = upload_videos_by_content(args, video_ids)

  # Print out number of attemps for each video
  print("\n------- Downloading attempts -------")
  for video_id, n_attempts in download_attempts.items():
    print(f"Video id {video_id} | Number of attempts: {n_attempts}")
  print("\n------- Uploading attempts -------")
  for video_id, n_attempts in upload_attempts.items():
    print(f"Video id {video_id} | Number of attempts: {n_attempts}")

  # Print out failed videos
  if failed_videos:
    print("\n------- Failed videos -------")
    for video_id, status in failed_videos.items():
      print(f"Video id {video_id} | Status: {status}")


if __name__ == '__main__':  
  parser = argparse.ArgumentParser() 
  parser.add_argument('--api_key',
                      default='',
                      help="API key to the required application.")                    
  parser.add_argument('--videos', 
                      default='', 
                      help="Path to csv file with videos metadata.")      
  parser.add_argument('--max_attempts', 
                      default=50,
                      type=int,
                      help="Maximum number of attempts to upload a video.")             

  args = parser.parse_args()
  args.metadata = (('authorization', 'Key {}'.format(args.api_key)),)

  main(args)