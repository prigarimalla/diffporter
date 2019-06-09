from hashlib import md5
from os import walk, path, mkdir
from shutil import move
from random import randint
from multiprocessing import Pool, cpu_count
from datetime import datetime
from time import time

def hash_file(filename):
    h=md5()
    with open(filename, 'rb') as f:
        buf = f.read()
        while len(buf) > 0:
            h.update(buf)
            buf = f.read()
    return h.hexdigest()


def hash_file_with_loc(filename):
    return hash_file(filename), filename


# https://stackoverflow.com/a/1094933
def sizeof_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


def collect_files(start_path = '.'):
    files_and_sizes = []
    for dirpath, dirnames, filenames in walk(start_path):
        cur_fas = []
        for f in filenames:
            # skip entirely if 'TO_DELETE' folder
            if f == '.dedupe':
                cur_fas = []
                break
            fp = path.join(dirpath, f)
            # skip if it is symbolic link
            if not path.islink(fp):
                cur_fas.append((fp, path.getsize(fp)))
        files_and_sizes.extend(cur_fas)

    files = map(lambda x: x[0], files_and_sizes)
    total_size = sum(map(lambda x: x[1], files_and_sizes))
    return files, total_size


def dedupe_folder_simple(folder):
    files, size = collect_files(folder)
    print('Deduplicating files...')
    print('(hashing {} on {} threads)'.format(sizeof_fmt(size), cpu_count()))
    p = Pool(cpu_count())
    t1 = time()
    hashes = p.map(hash_file_with_loc, files)
    td = time()-t1
    print('Done in {:.2f} seconds (~{}/s)'.format(td, sizeof_fmt(size/td)))

    unique_files = set()
    dupe_file_locs = set()

    for h, l in hashes:
        if h not in unique_files:
            unique_files.add(h)
        else:
            dupe_file_locs.add(l)

    if len(dupe_file_locs) > 0:
        print('Moving files...')
        del_dir =folder+'/TO_DELETE_'+str(randint(10000,99999))
        mkdir(del_dir)
        for df in dupe_file_locs:
            move(df, del_dir)
        with open(del_dir+'/.dedupe', 'w') as f:
            f.write('moved files for deletion at {}'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    else:
        print('No Dupes!')

if __name__ == '__main__':
    from sys import argv
    dedupe_folder_simple(argv[1])

