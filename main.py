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
folders_list = []
files_list = []
names_list = []


def name_to_index(name):
    try:
        index = names_list.index(name)
    except ValueError:
        index = len(names_list)
        names_list.append(name)
    return index

def index_to_name(index):
    return names_list[index]

def dir_index_to_name(dir_index):
    names = []
    for index in dir_index:
        names.append(index_to_name(index))
    return '\\'.join(names)

def scanning(directory):
    index_directory = name_to_index(directory)
    len_directory = len(directory) + 1
    for dir_name, dirs, files in walk(directory):
        dir_index = [index_directory]
        if dir_name != directory:
            for name in dir_name[len_directory:].split('\\'):
                dir_index.append(name_to_index(name))
        for folder_name in dirs:
            folder = Folder(dir_index=dir_index, folder_index=name_to_index(folder_name))
            folders_list.append(folder)
        for file_name in files:
            files_list.append(File(dir_index=dir_index, file_index=name_to_index(file_name)))

def generation_hashes(directory, format):
    if format == 'normal':
        print('Создание хэша файлов')
        results = tqdm(pool.imap_unordered(get_file_info, files_list), total=len(files_list))
    else:
        results = pool.imap_unordered(get_file_info, files_list)

    for file in results:
        update_dir_info(file)
    del(results)

    if format == 'normal':
        print('Создание хэша папок')

    for folder in reversed(folders_list):
        update_dir_hash(folder)


def get_file_info(file):
    with open(dir_index_to_name(file.dir_index + [file.file_index]), 'rb') as f:
        file.sha512=hashlib.sha512(f.read()).digest()
        file.size=fstat(f.fileno()).st_size
    return file


def update_dir_info(info):
    for i in range(len(folders_list)):
        if folders_list[i].dir_index + [folders_list[i].folder_index] == info.dir_index:
            folders_list[i].size += info.size
            folders_list[i].hash.append(info.sha512)
            break


def update_dir_hash(info):
    info.hash.sort()
    for i in range(len(folders_list)-1, -1, -1):
        if folders_list[i].dir_index + [folders_list[i].folder_index] == info.dir_index:
            folders_list[i].hash.append(hashlib.sha512(b''.join(info.hash)).digest())
            folders_list[i].size += info.size
            break


def update_dir_hash_to_sha512():
    done = True
    while done:
        for info in folders_list:
            info.hash.sort()
            for i in range(len(folders_list)-1, -1, -1):
                if folders_list[i].dir_index + [folders_list[i].folder_index] == info.dir_index:
                    if folders_list[i].sha512 != b'':
                        continue
                    folders_list[i].sha512 = hashlib.sha512(b''.join(info.hash)).digest()
                    done = False
                    break

def finder(format):
    temp = {}
    sha512size_list = []
    for info in folders_list:
        key = info.sha512.hex()+'-'+str(info.size)
        if not key in temp:
            temp[key] = []
        temp[key].append(info.dir_index + [info.folder_index])
    for key, temp_data in temp.items():
        if len(temp_data) > 1:
            folders = tuple(temp_data)
            i = 0
            for data in sha512size_list:
                if len(folders) != len(data['folders']):
                    continue
                for name in data['folders']:
                    for folder in folders:
                        if folder[:len(name)] == name:
                            i += 1
            if len(folders) != i:
                sha512size_list.append({
                    'size': int(key.split('-')[1]),
                    'folders': temp_data,
                })
    sha512size_list = sorted(sha512size_list, key=lambda i: i['size'], reverse=True)
    for i in range(len(sha512size_list)):
        for j in range(len(sha512size_list[i]['folders'])):
            sha512size_list[i]['folders'][j] = dir_index_to_name(sha512size_list[i]['folders'][j])
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
            size = info['size']
            if size < 1024:
                size = str(size) + ' байт'
            elif size < 1024**2:
                size = str(round(size/1024, 2)) + ' КБ'
            elif size < 1024**3:
                size = str(round(size/1024**2, 2)) + ' МБ'
            elif size < 1024**4:
                size = str(round(size/1024**3, 2)) + ' ГБ'
            print('Размер: ' + size)
            for folder in info['folders']:
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
