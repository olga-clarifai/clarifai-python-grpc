import json
import requests
import argparse
import os
import utils
from tqdm import tqdm

# Setup logging
logger = utils.setup_logging()

def main(args):

    logger.info("----- Download videos -----")

    # Load file containing info about videos to download
    with open(args.selected_videos, 'r') as f:
        video_ids = json.load(f)
    logger.info("List of selected videos loaded: {} videos to download".format(len(video_ids)))  

    # Download videos one by one
    downloaded_count = 0
    for video_id in tqdm(video_ids, total=len(video_ids)):
        #logger.info("Downloading video {} from {}".format(video_id, video_ids[video_id]['url']))  
        video_file = os.path.join(args.out_path, video_id + '.mp4')

        if not os.path.exists(video_file):
            # Make request and write if no error
            try:
                # url = video_ids[video_id]['url']
                url = video_ids[video_id]['url'].replace('playsource=3&', '') # fix for dead links
                r = requests.get(url, allow_redirects=True, timeout=2.5)
                if int(r.headers.get('content-length')) and r.headers.get('content-type') == 'video/mp4':
                    open(video_file, 'wb').write(r.content)
                    downloaded_count += 1
                else:
                    #logger.info("\tNo content. Video skipped.")  
                    pass
            except:
                #logger.info("\tBad request. Video skipped.")  
                pass

    logger.info("Done. Downloaded {} videos.".format(downloaded_count)) 


if __name__ == '__main__':  
    parser = argparse.ArgumentParser(description="Download videos.")      
    parser.add_argument('--selected_videos', 
                        default='', 
                        help="Path to json file with metadata about the selected videos to download.") 
    parser.add_argument('--out_path',
                        default='',
                        help="Path to output folder for storing videos.")  

    args = parser.parse_args()
    main(args)