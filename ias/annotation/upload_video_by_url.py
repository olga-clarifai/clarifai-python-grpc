import argparse
import time
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

  print("Total videos to upload: {}".format(len(video_ids)))

  return video_ids


def is_successeful_upload(args, video_id):

  for i in range (1000):

    # Fetch information about uploaded videos
    input_response = stub.GetInput(
      service_pb2.GetInputRequest(input_id=video_id),
      metadata=args.metadata
    )    

    # Check if download completed
    if input_response.input.status.code == status_code_pb2.INPUT_DOWNLOAD_SUCCESS:
      # Check if video was properly hosted and format was recognized
      if input_response.input.data.video.hosted.suffix != '':
        return True
      else:
        return False
    elif input_response.input.status.code == status_code_pb2.INPUT_DOWNLOAD_IN_PROGRESS:
      # Give some time to process and repeat request
      time.sleep(0.1)
      continue
    else:
      return False

  return False


def delete_failed(args, video_id):

  delete_input_response = stub.DeleteInput(
    service_pb2.DeleteInputRequest(input_id=video_id),
    metadata=args.metadata
  )  


def upload_videos_by_url(args, video_ids):

  # Keep tack of successful and failed upload
  failed_videos = {video_id: 'Start' for video_id in video_ids}
  upload_attempts = {} # video_id: number of attempts

  # Re-try to upload until max number of attempts reached or failed_videos is empty
  for i in range(args.max_attempts):
    if failed_videos:
      print(f'\nUploading attempt number: {i+1}. Videos to upload: {len(failed_videos)}')
      failed_videos_ = {}

      # Upload videos one by one
      for video_id in tqdm(failed_videos, total=len(failed_videos), desc='Uploading'):
        # Update number of attempts
        upload_attempts[video_id] = i+1

        # Set metadata
        input_meta = Struct()
        input_meta.update(video_ids[video_id])

        # Make a request
        post_inputs_response = stub.PostInputs(
          service_pb2.PostInputsRequest(
            inputs=[
              resources_pb2.Input(
                id=video_id,
                data=resources_pb2.Data(
                  video=resources_pb2.Video(url=video_ids[video_id]['url']),
                  metadata=input_meta
                )
              )
            ]
          ),
          metadata=args.metadata
        )
        failed_videos[video_id] = post_inputs_response.status.code

      # Check videos one by one
      for video_id in tqdm(failed_videos, total=len(failed_videos), desc='Checking'):
        # If upload unsuccesful, delete video and add to the queue for next attempt
        if not is_successeful_upload(args, video_id):
          failed_videos_[video_id] = failed_videos[video_id]
          delete_failed(args, video_id)

      # Update failed list
      failed_videos = failed_videos_

  print(f"\nSuccesfully uploaded videos: {len(video_ids)-len(failed_videos)}")
  return failed_videos, upload_attempts


def main(args):

  # Load ids of videos selected for the upload
  video_ids = get_video_ids_to_upload(args)

  # Upload videos
  failed_videos, upload_attempts  = upload_videos_by_url(args, video_ids)

  # Print out number of attemps for each video
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