#!/bin/bash



python tools/philly/philly_submit.py \
	--user xiyin1 \
        --config_file //hdfs/input/xiyin1/git/maskrcnn-benchmark/tools/philly/philly_main.py \
	--ngpus 16 \
	--extra "./tools/train_net.py --config-file train_oid_x101_16gpus.yaml"

