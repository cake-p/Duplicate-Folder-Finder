from multiprocessing.dummy import Pool
from multiprocessing import cpu_count
from os import walk, fstat
from classes import *
from tqdm import tqdm
import argparse
import hashlib
import json
from concurrent.futures import ThreadPoolExecutor


if cpu_count() > 1:
    thread_count = cpu_count() - 1
else:
    thread_count = 1

pool = Pool(thread_count)
FOLDERS_DICT = {}
FILES_DICT = {}
PATH = ''


def _fill(path, folders, files, len_path, connector='\\'):
    for folder_name in folders:
        FOLDERS_DICT[(path + connector + folder_name)[len_path:]] = Folder()
    for file_name in files:
        FILES_DICT[(path + connector + file_name)[len_path:]] = File()


# Сканирование папок для дальнейшего хэширования файлов и папок
def scanning():
    len_path = len(PATH) + 1
    if PATH[-1] == ':':
        data_map = walk(PATH + '\\')
        _fill(*next(data_map), len_path, '')
    else:
        data_map = walk(PATH)
    for path, folders, files in data_map:
        _fill(path, folders, files, len_path)


# Создание хэшей
def generation_hashes(format):
    if format == 'normal':
        print('Создание хэша файлов')
        results = tqdm(pool.imap_unordered(get_file_info, FILES_DICT.keys()), total=len(FILES_DICT), ascii=True)
    else:
        results = pool.imap_unordered(get_file_info, FILES_DICT.keys())

    for key in results:
        key_rspilt = key.rsplit('\\',1)
        if len(key_rspilt) != 1 and key_rspilt[0] != '':
            update_folder_info(key, key_rspilt[0])
    del(results)

    if format == 'normal':
        print('Создание хэша папок')

    for key, info in reversed(FOLDERS_DICT.items()):
        path = key.rsplit('\\',1)[0]
        if path != '':
            update_folder_hash(path, info)


# Получение информации о файле (размер и хэш)
def get_file_info(key):
    try:
        with open(PATH+'\\'+key, 'rb') as f:
            size = fstat(f.fileno()).st_size
            chunk_size = 536870912
            if size > chunk_size:
                temp = b''
                hash = False
                if len(key) > 50:
                    desc = '..' + key[-48:]
                else:
                    desc = key
                tbar = tqdm(desc=desc, total=int(size/1024/1024), unit='MB', leave=False, ascii=True)
                with ThreadPoolExecutor(max_workers=2) as executor:
                    while True:
                        data = executor.submit(f.read, chunk_size).result()
                        if hash:
                            temp += hash.result().digest()
                            tbar.update(int(chunk_size/1024/1024))
                        if not data:
                            break
                        hash = executor.submit(hashlib.sha512, data)
                FILES_DICT[key].sha512 = hashlib.sha512(temp).digest()
                tbar.close()
            else:
                FILES_DICT[key].sha512 = hashlib.sha512(f.read()).digest()
            FILES_DICT[key].size = size
    except OSError as e:
        # При невозможности считать файл используется не содержимое файла, а его имя
        if e.errno == 13 or e.errno == 22 or e.errno == 2:
            FILES_DICT[key].sha512 = hashlib.sha512(key.rsplit('\\',1)[-1].encode()).digest()
            FILES_DICT[key].size = len(key.rsplit('\\',1)[-1])
        else:
            raise e
    return key


# Обновление данных папки
def update_folder_info(key, path):
    FOLDERS_DICT[path].size += FILES_DICT[key].size
    FOLDERS_DICT[path].hash.append(FILES_DICT[key].sha512)


def update_folder_hash(path, info):
    info.hash.sort()
    FOLDERS_DICT[path].hash.append(hashlib.sha512(b''.join(info.hash)).digest())
    FOLDERS_DICT[path].size += info.size


def update_folder_hash_to_sha512():
    for key, info in FOLDERS_DICT.items():
        if FOLDERS_DICT[key].sha512 != b'':
            continue
        info.hash.sort()
        FOLDERS_DICT[key].sha512 = hashlib.sha512(b''.join(info.hash)).digest()


# Сравнение папок по хэшу и размеру
def finder(format, out):
    temp = {}
    sha512size_list = []
    for path, info in FOLDERS_DICT.items():
        key = info.sha512.hex()+'-'+str(info.size)
        if not key in temp:
            temp[key] = []
        temp[key].append(PATH[4:]+'\\'+path)
    # Исправить производительность цикла
    for key, temp_data in tqdm(temp.items(), total=len(temp), ascii=True):
        if len(temp_data) > 1:
            size = int(key.split('-')[1])
            if size == 0:
                continue
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
                    'size': size,
                    'folders': temp_data,
                })
    sha512size_list = sorted(sha512size_list, key=lambda i: i['size'], reverse=True)
    data = json.dumps(sha512size_list)
    with open(out, 'w') as f:
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
    parser.add_argument(
        '--out',
        type=str,
        default='data.json',
    )
    FLAGS, unparsed = parser.parse_known_args()
    if FLAGS.dir[-1] in ('\\', '/'):
        PATH = '\\\\?\\' + FLAGS.dir[:-1]
    else:
        PATH = '\\\\?\\' + FLAGS.dir

    if FLAGS.format == 'normal':
        print('Сканирование: ' + FLAGS.dir)
    scanning()
    generation_hashes(FLAGS.format)
    update_folder_hash_to_sha512()

    if FLAGS.format == 'normal':
        print('Поиск дубликатов')
    finder(FLAGS.format, FLAGS.out)
