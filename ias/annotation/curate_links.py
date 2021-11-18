import argparse
import logging
import os
import csv
import requests
import utils

# Import in the Clarifai gRPC based objects needed
from clarifai_grpc.channel.clarifai_channel import ClarifaiChannel
from clarifai_grpc.grpc.api import resources_pb2, service_pb2, service_pb2_grpc
from clarifai_grpc.grpc.api.status import status_code_pb2
from google.protobuf.json_format import MessageToDict
import load_ground_truth

# Setup logging
logger = utils.setup_logging()

# Construct the communications channel and the object stub to call requests on.
channel = ClarifaiChannel.get_json_channel()
stub = service_pb2_grpc.V2Stub(channel)

GT_LABELS = load_ground_truth.GT_LABELS + ['safe']
GT_LABELS_IDX = {label: idx for label, idx in zip(load_ground_truth.GT_LABELS , 
                                                  range(len(load_ground_truth.GT_LABELS )))}


def load_meta(args):
    ''' Load information about videos '''

    # Load ground truth if available
    if args.has_gt:
        ground_truth = load_ground_truth.load_all_from_csv(args.videos_meta, 'safe')

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
        
    # # ------ DEBUG CODE
    # video_ids_ = {}
    # for id in list(video_ids.keys())[245:270]:
    #   video_ids_[id] = video_ids[id]
    # video_ids = video_ids_
    # # ------ DEBUG CODE

    logger.info("Total number of videos: {}".format(len(video_ids)))
    return video_ids


def check_links_with_ground_truth(video_ids):
    ''' Sorts videos into dead or live link lists by label category '''

    # Prepare variables
    live_links = {label: [] for label in GT_LABELS}
    dead_links = {label: [] for label in GT_LABELS}
    live_video_ids, dead_video_ids = {}, {}
    dead_link_count = 0

    for i, video_id in enumerate(video_ids):
        url = video_ids[video_id]['url'].replace('playsource=3&', '') # fix from TT
        working_url = False

        # Make a request
        try:
            r = requests.get(url, allow_redirects=True, timeout=5)
            if int(r.headers.get('content-length')) and r.headers.get('content-type') == 'video/mp4':
                working_url = True
                live_video_ids[video_id] = video_ids[video_id]
            else:
                dead_link_count += 1
                dead_video_ids[video_id] = video_ids[video_id]
        except:
            dead_link_count += 1
            dead_video_ids[video_id] = video_ids[video_id]

        # Assign to appropriate list according to request's results
        if working_url:
            for label in video_ids[video_id]['ground_truth']:
                live_links[label].append({'video_id': video_id, 'url': url})
        else:
            for label in video_ids[video_id]['ground_truth']:
                dead_links[label].append({'video_id': video_id, 'url': url})

        # Progress bar
        utils.show_progress_bar(i+1, len(video_ids))

    logger.info("Number of dead links: {}".format(dead_link_count)) 
    return live_links, dead_links, live_video_ids, dead_video_ids, dead_link_count


def plot_distribution(live_links, dead_links):
    ''' Plot distribution of live and dead links by label category '''

    print("\n")
    for label in GT_LABELS:
        print("live: {} | dead: {} \t - {}".
              format(len(live_links[label]), len(dead_links[label]), label))
    print('\n')


def save_distribution(args, video_ids, live_links, dead_links, dead_link_count):
    ''' Save distribution of live and dead links by label category to csv file'''

    # Create output dir if needed
    path = os.path.join(args.out_path, 'link_distribution')
    if not os.path.exists(path):
        os.mkdir(path)

    # Set file path
    file_path = os.path.join(path, "{}_link_distribution.csv".format(args.dataset_name))

    # Write to file
    with open(file_path, 'w', encoding='UTF8', newline='') as f:

        writer = csv.writer(f)
        writer.writerow(['category', 'live', 'dead'])

        # Save general info
        total_row = ['total_for_all_videos', len(video_ids)-dead_link_count, dead_link_count]
        writer.writerow(total_row)

        # Save info for every label category
        for label in GT_LABELS:
            row = [label, len(live_links[label]), len(dead_links[label])]
            writer.writerow(row)

    logger.info("Saved distribution to {}".format(file_path))


def save_live_ground_truth(args, live_video_ids):
    ''' Dump ground truth with live urls only '''

    if args.save_gt:
        # Create output dir if needed
        path = os.path.join(args.out_path, 'live_ground_truth')
        if not os.path.exists(path):
            os.mkdir(path)        

        # Set file path
        file_path = os.path.join(path, "{}_{}.csv".format(args.dataset_name, 'ground_truth_live'))

        # Write to file
        with open(file_path, 'w', encoding='UTF8', newline='') as f:

            writer = csv.writer(f)
            writer.writerow(['video_id', 'description', 'url'] + load_ground_truth.GT_LABELS)

            # Get back ground truth 
            for video_id, meta in live_video_ids.items():
                row_meta = [video_id, meta['description'], meta['url']]
                row_labels = [0] * len(load_ground_truth.GT_LABELS)
                for label in load_ground_truth.GT_LABELS:
                    if label in meta['ground_truth']:
                        row_labels[GT_LABELS_IDX[label]] = 1
                writer.writerow(row_meta + row_labels)

        logger.info("Saved live ground truth to {}".format(file_path))


def save_categorized_links(args, links, name):
    ''' Save information about links in csv file '''

    if args.save_links:
        # Create output dir if needed
        path = os.path.join(args.out_path, 'links')
        if not os.path.exists(path):
            os.mkdir(path)

        # Set file path
        file_path = os.path.join(path, "{}_{}.csv".format(args.dataset_name, name))

        # Write to file
        with open(file_path, 'w', encoding='UTF8', newline='') as f:

            writer = csv.writer(f)
            writer.writerow(['label', 'video_id', 'url'])

            # Save info for every label category and video
            for label in GT_LABELS:
                for video in links[label]:
                    row = [label, video['video_id'], video['url']]
                    writer.writerow(row)

        logger.info("Saved {} to {}".format(name, file_path))


def main(args):

    # Load dataset metadata
    video_ids = load_meta(args)

    # Examine links and get separate lists of live and dead links
    # TODO: make a funtion for data without ground truth
    live_links, dead_links, live_video_ids, _ , dead_link_count = check_links_with_ground_truth(video_ids)

    # Display and save distribution per label category
    plot_distribution(live_links, dead_links)
    save_distribution(args, video_ids, live_links, dead_links, dead_link_count)

    # Save categorized lists of live and dead links,
    # as well as curated ground truth
    save_categorized_links(args, live_links, 'live_links')
    save_categorized_links(args, dead_links, 'dead_links')
    save_live_ground_truth(args, live_video_ids)


if __name__ == '__main__':  
    parser = argparse.ArgumentParser(description="Download videos.")
    parser.add_argument('--dataset_name',
                        default='',
                        help="Name of the dataset.")  
    parser.add_argument('--videos_meta', 
                        default='', 
                        help="Path to csv file with links and ground truth (optional).")     
    parser.add_argument('--has_gt',
                        default=True,
                        type=lambda x: (str(x).lower() == 'true'),
                        help="Indicates if ground truth is present in meta file.")                         
    parser.add_argument('--out_path',
                        default='',
                        help="Path to output file for storing links.")     
    parser.add_argument('--save_links',
                        default=True,
                        type=lambda x: (str(x).lower() == 'true'),
                        help="Save live and dead links to a csv file.")
    parser.add_argument('--save_gt',
                        default=True,
                        type=lambda x: (str(x).lower() == 'true'),
                        help="Save curated ground truth with live urls only.")

    args = parser.parse_args()
    main(args)