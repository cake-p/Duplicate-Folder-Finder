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
files_list = []
names_dict = {}


def name_to_index(name):
    if name in names_dict:
        index = names_dict[name]
    else:
        index = len(names_dict)
        names_dict[name] = index
    return index

def index_to_name(idx):
    for name, index in names_dict.items():
        if index == idx:
            return name

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
            folders_dict[tuple(dir_index + [name_to_index(folder_name)])] = Folder()
        for file_name in files:
            files_list.append(File(dir_index=dir_index, file_index=name_to_index(file_name)))

def generation_hashes(directory, format):
    if format == 'normal':
        print('Создание хэша файлов')
        results = tqdm(pool.imap_unordered(get_file_info, files_list), total=len(files_list))
    else:
        results = pool.imap_unordered(get_file_info, files_list)

    for file in results:
        if tuple(file.dir_index) in folders_dict:
            update_folder_info(file)
    del(results)

    if format == 'normal':
        print('Создание хэша папок')

    for dir_index, folder in reversed(folders_dict.items()):
        update_folder_hash(dir_index, folder)


def get_file_info(file):
    with open(dir_index_to_name(file.dir_index + [file.file_index]), 'rb') as f:
        file.sha512=hashlib.sha512(f.read()).digest()
        file.size=fstat(f.fileno()).st_size
    return file


def update_folder_info(file):
    folders_dict[tuple(file.dir_index)].size += file.size
    folders_dict[tuple(file.dir_index)].hash.append(file.sha512)


def update_folder_hash(dir_index, folder):
    if not dir_index[:-1] in folders_dict:
        return
    folder.hash.sort()
    folders_dict[dir_index[:-1]].hash.append(hashlib.sha512(b''.join(folder.hash)).digest())
    folders_dict[dir_index[:-1]].size += folder.size + 1


def update_dir_hash_to_sha512():
    done = True
    while done:
        for dir_index, folder in folders_dict.items():
            folder.hash.sort()
            if not dir_index[:-1] in folders_dict or folders_dict[dir_index[:-1]].sha512 != b'':
                continue
            folders_dict[dir_index[:-1]].sha512 = hashlib.sha512(b''.join(folder.hash)).digest()
            done = False
            break

def finder(format):
    temp = {}
    sha512size_list = []
    for dir_index, folder in folders_dict.items():
        key = folder.sha512.hex()+'-'+str(folder.size)
        if not key in temp:
            temp[key] = []
        temp[key].append(dir_index)
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
