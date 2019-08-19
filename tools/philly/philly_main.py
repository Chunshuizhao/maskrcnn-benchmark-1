#!/opt/conda/bin/python3
import os
import sys
import json
import socket
import torch


def setup_maskrcnn(PIP3, PYTHON3, project_path):
    #setup environment on philly to run maskrcnn-benchmark
    requirements_file = os.path.join(project_path, 'requirements.txt')

    # install requirements
    # if there is problem with sudo, then consider to use --user flag
    # for example sudo pip install xx -> pip install xx --user
    setup = []
    setup.append("sudo {} install -r {}".format(PIP3, requirements_file))
    setup.append("sudo apt-get update")
    setup.append("sudo apt-get install libyaml-dev")
    setup.append("sudo {} install pyyaml --upgrade --force".format(PIP3))

    for cmd in setup:
        print(cmd)
        os.system(cmd)

    # copy project folder to /tmp/ and build to avoid potential conflict in each job submit
    # exclude several paths for large models or model build
    cmd = "rsync -av --progress {} /tmp/. \
            --exclude=output/ --exclude=build/ --exclude=maskrcnn_benchmark.egg-info/".format(project_path) 
    print(cmd)
    os.system(cmd)

    # change dir and build
    os.chdir("/tmp/maskrcnn-benchmark/")
    # add FORCE_CUDA env variable to make sure it compile with CUDA
    os.environ["FORCE_CUDA"] = "1"
    cmd = "sudo {} setup.py build develop".format(PYTHON3)
    os.system(cmd)


def get_host_addr():
    # another way to get host ip, keep it here for record
    host_name = socket.gethostname()
    return socket.gethostbyname(host_name)


def get_master_container_ip_port():
    job_config_file = os.getenv('PHILLY_RUNTIME_CONFIG')
    with open(job_config_file, 'r') as f:
        job_config = json.load(f)
    container = [
        c for c in job_config['containers'].values() if c['index']==0][0]
    return container['ip'], container['portRangeStart']


def main():
    PIP3 = "/opt/conda/bin/pip"
    PYTHON3 = "/opt/conda/bin/python3"
    project_path = "/hdfs/input/xiyin1/git/maskrcnn-benchmark"

    # setup maskrcnn environment
    setup_maskrcnn(PIP3, PYTHON3, project_path)

    # figure out GPU environment
    ompi_env = {e:os.environ[e] for e in os.environ if e.startswith("OMPI_COMM_WORLD")}
    print(ompi_env)

    ngpus_per_node = torch.cuda.device_count()
    nnodes = int(os.getenv("OMPI_COMM_WORLD_SIZE"))
    assert nnodes > 0, "# node should be larger than 0"

    # philly by default sends 3 parameters including varioua paths (argv 1-3) 
    # and argv 0 is for the file itself
    # the cmd to run on each process start from argv 4 
    cmd_per_process = ' '.join(sys.argv[4:])
    if nnodes == 1:
        # single-node multi-process distributed training
        cmd_per_node = "{} -m torch.distributed.launch --nproc_per_node={} \
              {}".format(PYTHON3, ngpus_per_node, cmd_per_process)
    else:
        # multi-node multi-process distributed training
        node_rank = int(os.getenv("OMPI_COMM_WORLD_RANK"))
        master_ip, master_port = get_master_container_ip_port()
        cmd_per_node = "{} -m torch.distributed.launch --nproc_per_node={} \
              --nnodes {} --node_rank {} --master_addr {} --master_port {} {}".format(
              PYTHON3, ngpus_per_node, nnodes, node_rank, master_ip, master_port, cmd_per_process)
                
    print(cmd_per_node)
    os.system(cmd_per_node)


if __name__ == "__main__":
    main()
    







    
    
