import os
import os.path as op
import argparse
import pprint
import json
import math


def parse_args():
    """Parse input arguments."""
    parser = argparse.ArgumentParser(description='Submit job to Philly')
    parser.add_argument('--extra',
                        help='ExtraParams',
                        required=True,
                        type=str)
    parser.add_argument('--ngpus',
                        help='MinGPUS',
                        required=False,
                        type=int,
                        default=4)
    parser.add_argument('--debug',
                        help='keep node for debug purpose',
                        action='store_true')
    parser.add_argument('--job_name',
                        help='job name',
                        required=False,
                        type=str,
                        default='none')
    parser.add_argument('--cluster', 
                        help='cluster id', 
                        required=False,
                        type=str,
                        default='sc2')
    parser.add_argument('--user', 
                        help='user name',
                        required=True,
                        type=str,
                        default=None)
    parser.add_argument('--input_path', 
                        help='input path',
                        type=str,
                        required=False,
                        default=None)
    parser.add_argument('--config_file', 
                        help='config file on philly',
                        type=str,
                        required=False, 
                        default=None)
    args = parser.parse_args()
    return args


def submit_job(args):
    if args.input_path is None:
        args.input_path = op.join('/hdfs/input/', args.user)
    params = {
        "UserName": args.user,
        "CustomDockerName": None,
        "JobName": args.job_name,
        "VcId": "input",
        "Inputs": [
            {"Path": args.input_path,
             "Name": "dataDir"}
        ],
        "IsCrossRack": False,
        "MinGPUs": args.ngpus,
        "Outputs": [],
        "ClusterId": args.cluster,
        "ExtraParams": args.extra,
        "PrevModelPath": None,
        "DynamicContainerSize": False,
        "Registry": "phillyregistry.azurecr.io",
        "IsMemCheck": False,
        "BuildId": 0,
        "ConfigFile": args.config_file,
        "Timeout": None,
        "CustomMPIArgs": None,
        "OneProcessPerContainer": True,
        "RackId": "anyConnected",
        "Repository": "philly/jobs/test/vig-qd-env",
        "ToolType": None,
        "IsDebug": args.debug,
        "Tag": "maskrcnn",
        "SubmitCode": "p",
    }

    # need to write this explicitly.
    # otherwise jobs will be spreed across multiple nodes each with < 4 GPUs.
    # this is the case even though DynamicContainerSize is set to False. 
    params["NumOfContainers"] = int(math.ceil(params["MinGPUs"] / 4))

    pprint.pprint(params)
    with open('params.json', 'w') as f:
        json.dump(params, f)

    command = "curl -k --ntlm -n " + \
        " -X POST -H \"Content-Type: application/json\" " + \
        " --data @params.json https://philly/api/v2/submit" + \
        " --user '{}'".format(params['UserName'])

    print(command)
    os.system(command)


if __name__ == "__main__":
    """
      This script supports submitting multi-node multi-process jobs to philly.
      All default parameters are set in params.
      Frequently changed parameters can be given in command line to parse. 
      ConfigFile will figure out the number of nodes and run distributed training.
      Extra Parameters should include everything for training a single process.

      Sample Usage for training maskrcnn-benchmark: (other required user/cluster info
      are needed but omited in the following examples)
      1) Run single-node multi-process training: 
         >> python tools/philly/philly_submit.py --extra "./tools/train_net.py --config-file 
              configs/philly/train.yaml"
      
      2) Run multi-node multi-process training: 
         There are 4 GPUs on each node on Philly. 
         So this command will run 8 processes on 2 nodes. 
         >> python tools/philly/philly_submit.py --extra "./tools/train_net.py --config-file 
              configs/philly/train.yaml" --ngpus 8
    """


    args = parse_args()

    submit_job(args)

