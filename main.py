from multiprocessing.dummy import Pool
from multiprocessing import cpu_count
from os import walk, fstat
from classes import *
from tqdm import tqdm
import argparse
import hashlib
import json


if cpu_count() > 1:
    thread_count = cpu_count() - 1
else:
    thread_count = 1

pool = Pool(thread_count)
folders_dict = {}
files_dict = {}


def scanning(directory):
    for root, dirs, files in walk(directory):
        for dir_name in dirs:
            folders_dict[root + '\\' + dir_name] = Folder()
            if root != directory:
                folders_dict[root + '\\' + dir_name].dir_name = root
        for file_name in files:
            files_dict[root + '\\' + file_name] = File()


def generation_hashes(directory, format):
    if format == 'normal':
        print('Создание хэша файлов')
        results = tqdm(pool.imap_unordered(get_file_info, files_dict.keys()), total=len(files_dict))
    else:
        results = pool.map(get_file_info, files_dict.keys())

    for file, info in results:
        files_dict[file] = info
        if info.dir_name != directory:
            update_dir_info(info)
    del(results)

    if format == 'normal':
        print('Создание хэша папок')

    for key, info in reversed(folders_dict.items()):
        if info.dir_name != '':
            update_dir_hash(info.dir_name, info)


def get_file_info(file):
    names = file.rsplit('\\', 1)
    with open(file, 'rb') as f:
        stat = fstat(f.fileno())
        info = File(
            dir_name=names[0],
            file_name=names[1],
            sha512=hashlib.sha512(f.read()).digest(),
            size=stat.st_size,
            atime=int(stat.st_atime),
            mtime=int(stat.st_mtime),
            ctime=int(stat.st_ctime),
        )
    return file, info


def update_dir_info(info):
    folders_dict[info.dir_name].size += info.size
    folders_dict[info.dir_name].hash.append(info.sha512)


def update_dir_hash(dir_name, info):
    info.hash.sort()
    folders_dict[dir_name].hash.append(hashlib.sha512(b''.join(info.hash)).digest())
    folders_dict[dir_name].size += info.size


def update_dir_hash_to_sha512():
    for key, info in folders_dict.items():
        if folders_dict[key].sha512 != b'':
            continue
        info.hash.sort()
        folders_dict[key].sha512 = hashlib.sha512(b''.join(info.hash)).digest()


def finder(format):
    temp = {}
    sha512size_list = []
    for dir_name, info in folders_dict.items():
        key = info.sha512.hex()+'-'+str(info.size)
        if not key in temp:
            temp[key] = []
        temp[key].append(dir_name)
    for key, temp_data in temp.items():
        if len(temp_data) > 1:
            folders = tuple(temp_data)
            i = 0
            for data in sha512size_list:
                if len(folders) != len(data[1]):
                    continue
                for name in data[1]:
                    for folder in folders:
                        if folder.find(name) != -1:
                            i += 1
            if len(folders) != i:
                sha512size_list.append((int(key.split('-')[1]), tuple(temp_data)))
    sha512size_list = sorted(sha512size_list, reverse=True)
    data = json.dumps(sha512size_list)
    with open('data.json', 'w') as f:
        f.write(data)

    if format == 'json':
        print(data)
    else:
        i = 0
        for info in sha512size_list:
            i += 1
            print('Дубликаты #' + str(i) + ':')
            size = info[0]
            if size < 1024:
                size = str(size) + ' байт'
            elif size < 1024**2:
                size = str(round(size/1024, 2)) + ' КБ'
            elif size < 1024**3:
                size = str(round(size/1024**2, 2)) + ' МБ'
            elif size < 1024**4:
                size = str(round(size/1024**3, 2)) + ' ГБ'
            print('Размер: ' + size)
            for folder in info[1]:
                print(folder)
            print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--dir',
        type=str,
        default='.',
    )
    parser.add_argument(
        '--format',
        type=str,
        default='normal',
    )
    FLAGS, unparsed = parser.parse_known_args()

    if FLAGS.format == 'normal':
        print('Сканирование: ' + FLAGS.dir)
    scanning(FLAGS.dir)
    generation_hashes(FLAGS.dir, FLAGS.format)
    update_dir_hash_to_sha512()

    if FLAGS.format == 'normal':
        print('Поиск дубликатов')
    finder(FLAGS.format)
