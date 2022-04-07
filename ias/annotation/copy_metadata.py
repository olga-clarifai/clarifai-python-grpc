
import argparse
import pandas as pd
from tqdm import tqdm
import logging
import random

# Import in the Clarifai gRPC based objects needed
from clarifai_grpc.channel.clarifai_channel import ClarifaiChannel
from clarifai_grpc.grpc.api import resources_pb2, service_pb2, service_pb2_grpc
from clarifai_grpc.grpc.api.status import status_code_pb2
from google.protobuf.struct_pb2 import Struct

# Setup logging
logging.basicConfig(format='%(asctime)s %(message)s \t')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Construct the communications channel and the object stub to call requests on.
channel = ClarifaiChannel.get_json_channel()
stub = service_pb2_grpc.V2Stub(channel)


INPUT_API = ''
OUTPUT_API = ''

INPUT_METADATA = (('authorization', f'Key {INPUT_API}'),)
OUTPUT_METADATA = (('authorization', f'Key {OUTPUT_API}'),)

def process_response(response):
    if response.status.code != status_code_pb2.SUCCESS:
        logging.error("There was an error with your request!")
        logging.error("\tDescription: {}".format(response.status.description))
        logging.error("\tDetails: {}".format(response.status.details))
        raise Exception("Request failed, status code: " + str(response.status.code))


def get_inputs_meta():
    meta = {}

    # Get inputs
    for page in range(1,21):
        list_inputs_response = stub.ListInputs(
                            service_pb2.ListInputsRequest(page=page, per_page=1000),
                            metadata=INPUT_METADATA
        )

        # Process inputs in response
        for input in list_inputs_response.inputs:
            meta[input.id] = input.data.metadata
    return meta


def patch_metadata(meta):
    ''' Add information about final labels to inputs' metadata '''

    logger.info("Patching metadata...")
    for input_id, meta_ in tqdm(meta.items(), total=len(meta)):

        # Set final labels as metadata
        input_metadata = Struct()
        input_metadata.update(meta_)
        data = resources_pb2.Data(metadata=input_metadata)

        response = stub.PatchInputs(
            service_pb2.PatchInputsRequest(
                action="overwrite", 
                inputs=[resources_pb2.Input(id=input_id, data=data)]
            ),
            metadata=OUTPUT_METADATA
        )
        process_response(response)
        print(response)

    logger.info("Successfully patched metadata.")


def main():
    meta = get_inputs_meta()
    patch_metadata(meta)

if __name__ == '__main__': 
  main()