import os
import os.path as op
import glob
import argparse
import pdb


def configure_files_local(files):
    if len(files) == 1:
        # can be either one file, one path, or files with pattern
        if op.isfile(files[0]):
            return files, [False]
        elif op.isdir(files[0]):
            return [op.join(files[0], '')], [True]
        else:
            file_list = glob.glob(files[0])
            assert len(file_list) > 0, "No file matched with input"
            is_dirs = [op.isdir(f) for f in file_list]
            return file_list, is_dirs
    else:
        # can be a list of path and file combinations given by user
        is_dir = []
        file_list = []
        for file in files:
            is_dir.append(op.isdir(file))
            if op.isdir(file):
                file_list.append(op.join(file, '')) # to make sure there is / in the end
            else:
                file_list.append(file)
        return file_list, is_dir


def configure_files_on_philly(files):
    assert len(files) == 1, "Can only support transfer one file/dir from philly to local machine"
    filename = files[0]
    is_dir = True if filename[-1] == '/' else False
    return filename, is_dir


def file_transfer(args, settings):
    PHILLY_USER_ROOT = settings['PHILLY_USER_ROOT']
    PHILLY_PATH = settings['PHILLY_PATH']
    LOCAL_PATH = settings['LOCAL_PATH']
    AZURE_PATH = settings['AZURE_PATH']
    PHILLY_FS = settings['PHILLY_FS']
    AZCOPY = settings['AZCOPY']
    KEY = settings['AZURE_KEY']

    if args.from_philly:
        file, is_dir = configure_files_on_philly(args.files)
        if args.philly_path:
            src = op.join(PHILLY_USER_ROOT, args.philly_path, op.basename(file))
        else:
            src = op.join(PHILLY_PATH, file)
        if is_dir:
            des = op.join(LOCAL_PATH, op.dirname(file[:-1]))
            cmd = " ".join([PHILLY_FS, '-cp', '-r', src, des])
        else:
            des = op.join(LOCAL_PATH, op.dirname(file), '')
            cmd = " ".join([PHILLY_FS, '-cp', src, des])
        if not op.isdir(des):
            # when only transfer a subfolder of a folder that does not exist locally
            os.makedirs(des)
        print(cmd)
        os.system(cmd)
    else:
        files, is_dirs = configure_files_local(args.files)
        for file, is_dir in zip(files, is_dirs):
            src = op.join(LOCAL_PATH, file)
            # step one: from local to azure using azcopy
            if is_dir:
                des = op.join(AZURE_PATH, file)[:-1] # do not need /, otherwise will have problem.
                cmd = " ".join([AZCOPY, '--source', src, '--destination', des,
                               '--dest-key', KEY, '--recursive', '--quiet', '--parallel-level', '16'])
            else:
                des = op.join(AZURE_PATH, file)
                cmd = " ".join([AZCOPY, '--source', src, '--destination', des,
                               '--dest-key', KEY, '--quiet'])  # --quiet will replace all files on Azure by default
            print(cmd)
            os.system(cmd)
            # step two: from azure to philly using philly-fs
            if is_dir:
                src = op.join(AZURE_PATH, file)[:-1]
                if args.philly_path:
                    des = op.join(PHILLY_USER_ROOT, args.philly_path, op.basename(file[:-1]))
                else:
                    des = op.join(PHILLY_PATH, file)
                cmd = " ".join([PHILLY_FS, '-cp', '-r', src, des])
            else:
                src = op.join(AZURE_PATH, file)
                if args.philly_path:
                    des = op.join(PHILLY_USER_ROOT, args.philly_path, op.basename(file))
                else:
                    des = op.join(PHILLY_PATH, file)
                cmd = " ".join([PHILLY_FS, '-cp', src, des])
            print(cmd)
            os.system(cmd)


def parse_args():
    """Parse input arguments."""
    parser = argparse.ArgumentParser(description='Philly File Transfer')
    parser.add_argument('--files', required=True,  type=str, nargs='+', default=None,
                        help='files to be transferred, can be a list of files, or a path')
    parser.add_argument('--from_philly', required=False, default=False, action='store_true',
                        help='transfer from philly to local, default=False')
    parser.add_argument('--philly_path', required=False, type=str, default=None,
                        help='could also set the path on philly manually')
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    """ 
    This script supports file transfer between local machine and philly. 
    1) from local to philly
       a. use azcopy to transfer from local to Azure storage
       b. use philly-fs to transfer from Azure storage to philly 
    2) from philly to local
       a. use philly-fs to transfer from philly to local
    
    It supports to figure out the file type automatically. 
    Files can be a single file, a directory, or a file with certain pattern. 
    Files can also be a list of mixed files, directories given in command line. 
    All file systems are in the same structure and paths are relative to each root path. 
    Note that transferring to a softlink folder on philly will not work,
    in this case, the philly_path variable could be used to overwrite the default one.
    
    Example usages:
    ===== From local to philly (support various types of files)
    suppose this is what it looks like on local root directory:
    file1.txt, file2.txt, dir1/, dir2/, random_stuff/
    
    1) python tools/philly/philly_transfer.py --files file1.txt 
    2) python tools/philly/philly_transfer.py --files file*.txt
    3) python tools/philly_transfer.py --files dir1/
    4) python tools/philly_transfer.py --files dir*/
    5) python tools/philly_transfer.py --files file1.txt dir1/ dir2/

    transfer to a different path
    1) python tools/philly/philly_transfer.py --files file1.txt --philly_path /path/to/folder/
    2) python tools/philly/philly_transfer.py --files dir1/ --philly_path /path/to/folder/
    
    ====== From philly to local (only support one file or directory at a time)
    1) python tools/philly/philly_transfer.py --files file1.txt --from_philly
    2) python tools/philly/philly_transfer.py --files dir1/ --from_philly
    """

    args = parse_args()
    # change the setting for each user and fixed for all future transfers.
    # make sure to install azcopy and philly-fs first follow the official website.
    # make AZURE_PATH and AZURE_KEY as an environment variable. 
    settings = {
        'LOCAL_PATH': '/gpu02_raid/xiyin1/git/maskrcnn-benchmark/',
        'AZURE_PATH': os.genenv('AZURE_PATH'),
        'PHILLY_USER_ROOT': 'gfs://sc2/input/xiyin1/',
        'PHILLY_PATH': 'gfs://sc2/input/xiyin1/git/maskrcnn-benchmark/',
        'AZURE_KEY': os.getenv('AZURE_KEY'),
        'AZCOPY': 'azcopy',
        'PHILLY_FS': 'philly-fs',
    }

    file_transfer(args, settings)
