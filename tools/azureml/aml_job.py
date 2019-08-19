import os
import os.path as op
import sys
import re

from workspace_utils import get_workspace
from azureml.core import Experiment
from misc import bcolors, get_json_cfg


def main(func, output):
    ws = get_workspace()
    njobs = 0
    if op.isdir(output):
        njobs = 1
        operate_on_one_job(ws, func, output)
    elif output.lower() == "all":
        for f in os.listdir("output/"):
            fname = op.join("output/", f)
            if op.isdir(fname) and op.isfile(op.join(fname, 
                                "experiment_config.json")):
                njobs += 1
                operate_on_one_job(ws, func, fname) 
    else:
        for f in os.listdir("output/"):
            fname = op.join("output/", f)
            if op.isdir(fname) and re.search(output, fname):
                njobs += 1
                operate_on_one_job(ws, func, fname)

    if njobs == 0:
        print("Could not match the job directory: {}".format(output))
    else:
        print("Processed {} jobs.".format(njobs))


def operate_on_one_job(ws, func, output_dir):
    # supported operations:
    func_set = ("abort", "status", "resubmit", "logs", "results")

    cfg_file = op.join(output_dir, "experiment_config.json")
    cfg = get_json_cfg(cfg_file)
    run_id = cfg['runId']
    print(bcolors.OKBLUE + "run_number: {}, run_id: {}, output_dir: {}".format(
          cfg["run_number"], run_id, output_dir) + bcolors.ENDC)

    exp_name = run_id.split('_')[0]
    exp = Experiment(ws, exp_name)

    run = None
    for r in exp.get_runs():
        if r.id == run_id:
            run = r
            break

    if func == "abort":
        run.cancel()
        print("Job " + bcolors.WARNING + "canceled" + bcolors.ENDC)
    elif func == "status":
        print("Job status: " + bcolors.OKGREEN + run.status + bcolors.ENDC)
    elif func == "resubmit":
        cfg1 = get_json_cfg(op.join(output_dir, 'submit_config.json'))
        cmd = "python tools/azureml/aml_submit.py "
        opts = ""
        for key in cfg1:
            if key == "opts":
                # take special care of opts that should come last
                opts = str(cfg1[key])
            else:
                cmd = cmd + "--" + key + " " + str(cfg1[key]) + " "
        cmd += opts
        print(cmd)
        os.system(cmd)
    elif func == "logs":
        run.download_files(output_directory=output_dir)
        print("Downloaded log files to: {}".format(output_dir))
        os.system("tail {}".format(op.join(output_dir, "azureml-logs/70_driver_log.txt")))
    elif func == "results":
        data_stores = cfg['runDefinition']['dataReferences']
        for key in data_stores.keys():
            if op.join(data_stores[key]['pathOnDataStore'], "") == \
               op.join(output_dir, ""):   # make sure there is '/' in the end
                ds_name = data_stores[key]['dataStoreName']
                break
        ds = ws.datastores[ds_name]
        cmd = "azcopy --source https://{}.blob.core.windows.net/{}/{} " \
              "--destination {} --source-key {} --recursive".format(
                ds.account_name, ds.container_name, output_dir, 
                output_dir, ds.account_key)
        print(cmd)
        os.system(cmd)
    else:
        print(bcolors.WARNING + "Unsupported function {}, only support {}"
                .format(func, func_set) + bcolors.ENDC)


if __name__ == "__main__":
    """
    This file support to query job status, cancel/resubmit a job, 
    or get the job logs, model results. 
    It takes two system arguments: one operation name 
    and one output_dir with job information. 
    output_dir can be one job directory, or a pattern that can 
    match to a list of jobs in output/.

    Example usages:
    1) set an alias in ~/.bashrc
    >> alias aml="python tools/azureml/aml_job.py "

    2) use the supported features:
    >> aml status output/20190731_test/ ===> check job status
    >> aml abort output/20190731_test/ ===> abort the job
    >> aml logs output/20190731_test/ ===> get job log
    >> aml results output/20190731_test/ ===> get job output like models
    >> aml resubmit output/20190731_test/ ===> resubmit a job

    3) the second parameter could be a pattern to match job names:
    >> aml status output/20190731_ ===> check status for all jobs start with this
    >> aml abort "output/20190731*test*" ===> abort jobs with this parttern 

    4) you can also use "all" to do an operation to all jobs in output/:
    >> aml status all ===> get the status for all jobs
    >> aml abort all ===> cancel all jobs
    """

    func = sys.argv[1]
    output_dir = sys.argv[2]

    main(func, output_dir)

