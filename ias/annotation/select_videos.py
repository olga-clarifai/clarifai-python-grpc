import argparse
import logging
import os
import json

# Import in the Clarifai gRPC based objects needed
from clarifai_grpc.channel.clarifai_channel import ClarifaiChannel
from clarifai_grpc.grpc.api import resources_pb2, service_pb2, service_pb2_grpc
from clarifai_grpc.grpc.api.status import status_code_pb2
from google.protobuf.json_format import MessageToDict
from proto.clarifai.api.resources_pb2 import Video
import load_ground_truth

# Setup logging
logging.basicConfig(format='%(asctime)s %(message)s \t')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Construct the communications channel and the object stub to call requests on.
channel = ClarifaiChannel.get_json_channel()
stub = service_pb2_grpc.V2Stub(channel)


def process_response(response):
    if response.status.code != status_code_pb2.SUCCESS:
        logger.error("There was an error with your request!")
        logger.error("\tDescription: {}".format(response.status.description))
        logger.error("\tDetails: {}".format(response.status.details))
        raise Exception("Request failed, status code: " + str(response.status.code))


def load_meta(videos_meta_path):
    ''' Load information about videos '''

    # TODO: make a general load (for now from ground truth)
    videos_meta = load_ground_truth.load_all_from_csv(videos_meta_path, 'safe')
    
    logger.info("Number of initially loaded videos: {}".format(len(videos_meta)))
    return videos_meta


def get_existing_video_ids(metadata):
    ''' Get list of all inputs that were already uploaded '''

    existing_video_ids = []
    
    # Get inputs and extract video ids
    for page in range(1,5):
        list_inputs_response = stub.ListInputs(
                            service_pb2.ListInputsRequest(page=page, per_page=1000),
                            metadata=metadata
        )
        process_response(list_inputs_response)

        for input_object in list_inputs_response.inputs:
            json_obj = MessageToDict(input_object)
            video_id = json_obj['data']['metadata']['id']
            #video_id = json_obj['data']['metadata']['source-file-line']
            existing_video_ids.append(video_id)

    logger.info("Number of fetched videos: {}".format(len(existing_video_ids)))
    return existing_video_ids


def select_videos(videos_meta):
    ''' Select specific videos according to criteria '''

    # TODO: write actual code
    video_ids = videos_meta

    logger.info("Selected {} videos".format(len(video_ids)))
    return video_ids


def remove_existing(video_ids, existing_video_ids):
    ''' Remove from the list ids of videos that are already uploaded '''

    final_video_ids = {}
    for video_id in video_ids:
        if not video_id in existing_video_ids:
            final_video_ids[video_id] = video_ids[video_id]
    
    logger.info("Total number of selected videos after removing existing ones: {}".format(len(final_video_ids)))
    return final_video_ids


def save_data(args, to_save, data, name):
    ''' Dump provided data to a json file '''

    if to_save:
        # Create output dir if needed
        path = os.path.join(args.out_path, name)
        if not os.path.exists(path):
            os.mkdir(path)

        # Set file path
        file_path = os.path.join(path, "{}_{}_{}.json".format(args.app_name, name))

        # Write to file
        with open(file_path, 'w') as f:
            json.dump(data, f)


def main(args, metadata):

    # Load videos metadata
    videos_meta = load_meta(args.videos_meta)
    
    # Select videos according to criteria
    selected_video_ids = select_videos(videos_meta)

    # Fetch ids of videos that were already uploaded
    existing_video_ids = get_existing_video_ids(metadata)

    # Make final selecting by exclusing already existing ones
    selected_video_ids = remove_existing(selected_video_ids, existing_video_ids)

    # Save selected ids to a file
    save_data(args, args.save_selected, selected_video_ids, 'selected_videos')


if __name__ == '__main__':  
    parser = argparse.ArgumentParser(description="Download videos.")
    parser.add_argument('--app_name',
                        default='',
                        help="Name of the pplication.") 
    parser.add_argument('--api_key',
                        default='',
                        help="API key to the required application.")  
    parser.add_argument('--videos_meta', 
                        default='', 
                        help="Path to csv file with ground truth.")       
    parser.add_argument('--out_path',
                        default='',
                        help="Path to output file for storing video ids.")     
    parser.add_argument('--save_selected',
                        default=True,
                        type=lambda x: (str(x).lower() == 'true'),
                        help="Save ids of selected videos to a json file.")

    args = parser.parse_args()

    metadata = (('authorization', 'Key {}'.format(args.api_key)),)

    main(args, metadata)