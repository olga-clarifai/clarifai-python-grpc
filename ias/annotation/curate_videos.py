import json
import argparse
import os
import utils
import hashlib
import numpy as np
import itertools
from tqdm import tqdm

# Setup logging
logger = utils.setup_logging()

def main(args):

    # Load file containing info about videos to download
    with open(args.selected_videos, 'r') as f:
        video_ids = json.load(f)
    logger.info("Number of selected videos: {}".format(len(video_ids)))  

    # Open videos and compute hash
    hashes_ids = {}
    downloaded_count = 0
    for video_id in tqdm(video_ids, total=len(video_ids)):
        video_file = os.path.join(args.in_path, video_id + '.mp4')
        if os.path.exists(video_file):
            downloaded_count += 1

            # Read video
            with open(video_file, "rb") as f:
                file_bytes = f.read()

                # compute hash and add it to dictionary
                hash = hashlib.md5(file_bytes).hexdigest()
                if hash in hashes_ids:
                    hashes_ids[hash].append(video_id)
                else:
                    hashes_ids[hash] = [video_id] 

    logger.info("Number of downloaded videos to curate: {}".format(downloaded_count))

    # Remove blanc videos
    id_lists = list(hashes_ids.values())
    blanc_idx = np.argmax([len(l) for l in id_lists])
    bad_ids = id_lists.pop(blanc_idx)
    logger.info("Number of blanc videos: {}".format(len(bad_ids)))

    # Remove duplicates (preserve only ids, no meta)
    duplicate_ids = list(itertools.chain(*[l[1:] for l in id_lists]))
    unique_ids = [l[0] for l in id_lists]
    bad_ids += duplicate_ids
    logger.info("Number of duplicate videos: {}".format(len(duplicate_ids)))

    # Save meta for curated videos
    curated_video_ids = {}
    for id in unique_ids:
        curated_video_ids[id] = video_ids[id]
    utils.save_data(args.save_curated_list, args.out_path, curated_video_ids, args.tag, 'curated_videos')

    # Delete bad videos from disk
    removed_count = 0
    if args.remove_bad:
        for id in bad_ids:
            os.remove(os.path.join(args.in_path, id + '.mp4'))
            removed_count += 1
        logger.info("Number of videos removed from disk: {}".format(removed_count))
    
    logger.info("Number of curated videos: {}".format(downloaded_count-removed_count))

    end = True


if __name__ == '__main__':  
    parser = argparse.ArgumentParser(description="Download videos.")    
    parser.add_argument('--tag',
                        default='',
                        help="Name of the process/application.")   
    parser.add_argument('--selected_videos', 
                        default='', 
                        help="Path to json file with metadata about the selected videos to download.") 
    parser.add_argument('--in_path',
                        default='',
                        help="Path to load videos from.")  
    parser.add_argument('--out_path',
                        default='',
                        help="Path to output curated lists.")  
    parser.add_argument('--save_curated_list',
                        default=True,
                        type=lambda x: (str(x).lower() == 'true'),
                        help="Save list of curated videos.")
    parser.add_argument('--remove_bad',
                        default=True,
                        type=lambda x: (str(x).lower() == 'true'),
                        help="Delete blanc and duplicate videos from disk.")                    

    args = parser.parse_args()
    main(args)