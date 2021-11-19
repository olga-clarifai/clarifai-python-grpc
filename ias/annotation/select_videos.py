import argparse
import os
import json
import csv
import utils

import ground_truth as gt

# Import in the Clarifai gRPC based objects needed
from clarifai_grpc.channel.clarifai_channel import ClarifaiChannel
from clarifai_grpc.grpc.api import resources_pb2, service_pb2, service_pb2_grpc
from clarifai_grpc.grpc.api.status import status_code_pb2
from google.protobuf.json_format import MessageToDict

# Setup logging
logger = utils.setup_logging()

# Construct the communications channel and the object stub to call requests on.
channel = ClarifaiChannel.get_json_channel()
stub = service_pb2_grpc.V2Stub(channel)


def load_meta(args):
    ''' Load information about videos '''

    # Load ground truth if available
    if args.has_gt:
        ground_truth = gt.load_all_from_csv(args.videos_meta, 'safe')

    # Load the rest of meta
    video_ids = {}
    with open(args.videos_meta, 'r') as f:
        reader = csv.DictReader(f)
        for line in reader:
            line = {k.lower(): v for k, v in line.items()}

            # Extract meta
            try: # ENG
                meta = {'video_id': line['video_id'], 'description': line['video_description'], 'url': line['video_url']}
            except: # FR/DE
                meta = {'video_id': line['video_id'], 'description': line['description'], 'url': line['url']}

            # Add ground truth if available
            if args.has_gt and meta['video_id'] in ground_truth:
                meta['ground_truth'] = ground_truth[meta['video_id']]
                video_ids[meta['video_id']] = meta
            elif not args.has_gt:
                video_ids[meta['video_id']] = meta

    logger.info("Initial number of videos: {}".format(len(video_ids)))
    return video_ids


def select_videos(video_ids):
    ''' Select specific videos according to criteria '''

    # TODO: write appropriate selection algorithm

    logger.info("Selected {} videos".format(len(video_ids)))
    return video_ids


def save_data(args, to_save, data, name):
    ''' Dump provided data to a json file '''

    if to_save:
        # Create output dir if needed
        path = os.path.join(args.out_path, name)
        if not os.path.exists(path):
            os.mkdir(path)

        # Set file path
        file_path = os.path.join(path, "{}_{}.json".format(args.app_name, name))

        # Write to file
        with open(file_path, 'w') as f:
            json.dump(data, f)


def main(args):

    # Load videos metadata
    video_ids = load_meta(args)
    
    # Select videos according to criteria
    selected_video_ids = select_videos(video_ids)

    # Save selected ids to a file
    utils.save_data(True, args.out_path, selected_video_ids, args.tag, 'selected_videos')


if __name__ == '__main__':  
    parser = argparse.ArgumentParser(description="Download videos.")
    parser.add_argument('--tag',
                        default='',
                        help="Name of the process/application.") 
    parser.add_argument('--videos_meta', 
                        default='', 
                        help="Path to csv file with ground truth.")     
    parser.add_argument('--has_gt',
                        default=False,
                        type=lambda x: (str(x).lower() == 'true'),
                        help="Indicates if ground truth is present in meta file.")                         
    parser.add_argument('--out_path',
                        default='',
                        help="Path to output file for storing video ids.")     

    args = parser.parse_args() 

    main(args)