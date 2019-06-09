import json
from hashlib import md5
from os import walk, path, mkdir
from shutil import move
from random import randint
from multiprocessing import Pool, cpu_count
from datetime import datetime
from time import time
from hash_cache import HashCache

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

def hash_file_with_loc_and_cache_hash(filename):
    file_hash = hash_file(filename)
    hash_cache.cache(file_hash, filename)
    return file_hash, filename

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
        if '.diffporter' in dirpath:
            continue
        for f in filenames:
            # skip entirely if 'TO_DELETE' folder
            if f == '.dedupe':
                cur_fas = []
                break
            file_path = path.abspath(path.join(dirpath, f))
            # skip if it is symbolic link
            if not path.islink(file_path):
                cur_fas.append((file_path, path.getsize(file_path)))
        files_and_sizes.extend(cur_fas)

    files = map(lambda x: x[0], files_and_sizes)
    total_size = sum(map(lambda x: x[1], files_and_sizes))
    return files, total_size


def dedupe_folder_simple(folder):
    files, size = collect_files(folder)
    threads = max(cpu_count(), 8)
    print('Deduplicating files...')
    print('(hashing {} on {} threads)'.format(sizeof_fmt(size), threads))
    p = Pool(threads)
    t1 = time()
    hashes = p.map(hash_file_with_loc, files)
    p.map_async(hash_file_with_loc, files)
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

def init_cache(loc):
    global hash_cache
    hash_cache = HashCache(loc)

def process_file(filename):
    cache_res = hash_cache.get(filename)
    return cache_res if cache_res else hash_file_with_loc_and_cache_hash(filename)

def dedupe_folder_with_hash_cache(folder, cache_location=None):
    files, size = collect_files(folder)
    threads = min(cpu_count(), 8)
    print('Deduplicating files...')
    print('(hashing {} on {} threads)'.format(sizeof_fmt(size), threads))
    p = Pool(threads, initializer=init_cache, initargs=(path.join(folder, '.diffporter'), ))
    t1 = time()
    hashes = p.map(process_file, files)
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


def build_folder_cache(folder):
    files, size = collect_files(folder)
    threads = min(cpu_count(), 8)
    print('Building hash cache on {}...'.format(folder))
    print('(hashing {} on {} threads)'.format(sizeof_fmt(size), threads))
    p = Pool(threads, initializer=init_cache, initargs=(path.join(folder, '.diffporter'), ))
    t1 = time()
    p.map(process_file, files)
    td = time()-t1
    print('Done in {:.2f} seconds (~{}/s)'.format(td, sizeof_fmt(size/td)))

if __name__ == '__main__':
    from sys import argv
    # dedupe_folder_with_hash_cache(argv[1])
    build_folder_cache(argv[1])
