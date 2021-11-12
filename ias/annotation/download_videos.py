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
  for video_id in video_ids:
    video_file = os.path.join(args.out_path, video_id + '.mp4')
    r = requests.get(video_ids[video_id]['url'], allow_redirects=True)
    open(video_file, 'wb').write(r.content)
    logging.info("Downloading video {} from {}".format(video_id, video_ids[video_id]['url']))  


if __name__ == '__main__':  
  parser = argparse.ArgumentParser(description="Download videos.")
  parser.add_argument('--api_key',
                      default='',
                      help="API key to the required application.")        
  parser.add_argument('--out_path',
                      default='',
                      help="Path to output folder for storing videos.")     
  parser.add_argument('--selected_videos', 
                      default='', 
                      help="Path to json file with metadata about the selected videos to download.") 

  args = parser.parse_args()
  main(args)