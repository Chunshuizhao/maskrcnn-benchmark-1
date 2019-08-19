If you don't want to use PhillyTools, this folder contains custom scripts included in that can help you launching jobs on Philly and transfer files between local machine and philly. 
However, as Philly platform is keep changing, it is hard to maintain these scipts to keep up-to-date. So they may not work properly. 
PhillyTools are recommended. 

All philly related tools are in tools/philly/. You need to do the following before submitting a job:
1) Install azcopy, philly-fs, and setup environmental variables properly as required in `philly_main.py`, `philly_submit.py`, `philly_transfer.py`.
2) clone a copy of maskrcnn-benchmark to /hdfs/input/your-user-name/
3) upload data to philly, you can refer to `philly_transfer.py` for details.
4) make sure you have the same copy and file structure of codes/data on local machine and philly.

#### Single-Node training:
```bash
python tools/philly/philly_submit.py --extra "./tools/train_net.py --config-file path/to/config/file.yaml" --user my_username --config_file my_config_on_philly
```

#### Multi-Node training:
```bash
python tools/philly/philly_submit.py --extra "./tools/train_net.py --config-file path/to/config/file.yaml" --ngpus 8 --user my_username --config-file my_config_on_philly
```
By default, it will request one node with 4 gpus. If you specify 8 gpus, it will distribute on 2 nodes. To simplify the job submission, I make the extra parameter to be the full command you want to run on a single process (GPU). Then `philly_main.py` will take care of opening multiple processes on each node.







