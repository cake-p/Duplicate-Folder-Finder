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
directory = ''


# Сканирование папок для дальнейшего хэширования файлов и папок
def scanning():
    len_directory = len(directory)
    for dir, dirs, files in walk(directory):
        for dir_name in dirs:
            folders_dict[(dir + '\\' + dir_name)[len_directory:]] = Folder()
        for file_name in files:
            files_dict[(dir + '\\' + file_name)[len_directory:]] = File()


# Создание хэшей
def generation_hashes(format):
    if format == 'normal':
        print('Создание хэша файлов')
        results = tqdm(pool.imap_unordered(get_file_info, files_dict.keys()), total=len(files_dict))
    else:
        results = pool.imap_unordered(get_file_info, files_dict.keys())

    for key in results:
        dir_name = key.rsplit('\\',1)[0]
        if dir_name != '':
            update_folder_info(key, dir_name)
    del(results)

    if format == 'normal':
        print('Создание хэша папок')

    for key, info in reversed(folders_dict.items()):
        dir_name = key.rsplit('\\',1)[0]
        if dir_name != '':
            update_folder_hash(dir_name, info)


# Получение информации о файле (размер и хэш)
def get_file_info(key):
    try:
        with open(directory+key, 'rb') as f:
            files_dict[key].sha512 = hashlib.sha512(f.read()).digest()
            files_dict[key].size = fstat(f.fileno()).st_size
    except PermissionError:
        files_dict[key].sha512 = hashlib.sha512(key.rsplit('\\',1)[1].encode()).digest()
        files_dict[key].size = len(key.rsplit('\\',1)[1])

    return key


# Обновление данных папки
def update_folder_info(key, dir_name):
    folders_dict[dir_name].size += files_dict[key].size
    folders_dict[dir_name].hash.append(files_dict[key].sha512)


def update_folder_hash(dir_name, info):
    info.hash.sort()
    folders_dict[dir_name].hash.append(hashlib.sha512(b''.join(info.hash)).digest())
    folders_dict[dir_name].size += info.size


def update_folder_hash_to_sha512():
    for key, info in folders_dict.items():
        if folders_dict[key].sha512 != b'':
            continue
        info.hash.sort()
        folders_dict[key].sha512 = hashlib.sha512(b''.join(info.hash)).digest()


# Сравнение папок по хэшу и размеру
def finder(format):
    temp = {}
    sha512size_list = []
    for dir_name, info in folders_dict.items():
        key = info.sha512.hex()+'-'+str(info.size)
        if not key in temp:
            temp[key] = []
        temp[key].append(directory+dir_name)
    for key, temp_data in temp.items():
        if len(temp_data) > 1:
            folders = tuple(temp_data)
            i = 0
            for data in sha512size_list:
                if len(folders) != len(data['folders']):
                    continue
                for name in data['folders']:
                    for folder in folders:
                        if folder.find(name) != -1:
                            i += 1
            if len(folders) != i:
                sha512size_list.append({
                    'size': int(key.split('-')[1]),
                    'folders': temp_data,
                })
    sha512size_list = sorted(sha512size_list, key=lambda i: i['size'], reverse=True)
    data = json.dumps(sha512size_list)
    with open('data.json', 'w') as f:
        f.write(data)

    if format == 'json':
        print(data)
    elif format == 'normal':
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
    directory = FLAGS.dir

    if FLAGS.format == 'normal':
        print('Сканирование: ' + FLAGS.dir)
    scanning()
    generation_hashes(FLAGS.format)
    update_folder_hash_to_sha512()

    if FLAGS.format == 'normal':
        print('Поиск дубликатов')
    finder(FLAGS.format)
