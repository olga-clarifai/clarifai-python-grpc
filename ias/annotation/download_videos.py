import json
import requests
import argparse
import logging
import os

# Setup logging
logging.basicConfig(format='%(asctime)s %(message)s \t')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def main(args):

    logger.info("----- Download videos -----")

    # Load file containing info about videos to download
    with open(args.selected_videos, 'r') as f:
        video_ids = json.load(f)
    logging.info("List of selected videos loaded: {} videos to upload".format(len(video_ids)))  

    # Download videos one by one
    downloaded_count = 0
    for video_id in video_ids:
        logging.info("Downloading video {} from {}".format(video_id, video_ids[video_id]['url']))  
        video_file = os.path.join(args.out_path, video_id + '.mp4')

        # Make request and write if no error
        try:
            # url = video_ids[video_id]['url']
            url = video_ids[video_id]['url'].replace('playsource=3&', '') # fix for dead links
            r = requests.get(url, allow_redirects=True, timeout=2.5)
            length = int(r.headers.get('content-length'))
            if length != 0:
                open(video_file, 'wb').write(r.content)
                downloaded_count += 1
            else:
                logging.info("\tNo content. Video skipped.")  
        except:
            logging.info("\tBad request. Video skipped.")  

    logging.info("Done. Downloaded {} videos.".format(downloaded_count)) 



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