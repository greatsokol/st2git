import sys
import concurrent.futures
import configparser
import fnmatch
import os
import signal
import shutil
import subprocess
import threading
import time
import uuid
from datetime import datetime

try:
    from git import Repo, Actor
except:
    print('Error: GitPython library required (install with "pip install gitpython")')
    quit(-1)

# -------------------------------------------------------------------------------------------------
python_version = sys.version.split(' ', 1)[0]
if python_version < '3.6':
    print('Error: Version of python interpreter should start from 3.6 ({})'.format(python_version))
    quit(-1)

# -------------------------------------------------------------------------------------------------
COMMAND_LIST = 'list'
COMMAND_HIST = 'hist'
KEY_FILENAME = 'filename'
KEY_REVISION = 'revision'
KEY_PATH = 'path'
KEY_AUTHOR = 'author'
KEY_COMMENT = 'comment'
KEY_DATE = 'date'

PATH_ROOT = os.path.abspath('')
PATH_GIT_REPO = os.path.join(PATH_ROOT, '_REPO')
PATH_TEMP = os.path.join(PATH_ROOT, '_TEMP')
LOCK = threading.RLock()
EPOCH = datetime.utcfromtimestamp(0)
EXECUTOR = concurrent.futures.ThreadPoolExecutor(thread_name_prefix='thread')
ERROR_MESSAGES = ['Some of the required resources are currently in use by other users',
                  'An existing connection was forcibly closed by the remote host',
                  'Unable to update file status information in the database on the local workstation',
                  "index.lock' could not be obtained",
                  'Read timed out', 'Connection reset',
                  'Failed to login to Active Directory server.']


#
def kill_app(message):
    # Инным способом остановить все потоки не получается
    log('KILLIG APP {}'.format(message))
    os.kill(os.getpid(), signal.SIGTERM)


# -------------------------------------------------------------------------------------------------
def log(message_text, indent=False):
    LOCK.acquire(True)
    try:
        message_text = '[{}][{}_{}] {}'.format(current_time_str(), threading.get_ident(), threading.current_thread().name, str(message_text))
        if indent:
            message_text = '\n' + message_text
        log_file_name = os.path.join(PATH_ROOT, filename('log'))
        with open(log_file_name, mode='a') as f:
            print(message_text)
            f.writelines('\n' + message_text)
    finally:
        LOCK.release()


# -------------------------------------------------------------------------------------------------
def quote(string2prepare):
    return '"' + string2prepare + '"'


# -------------------------------------------------------------------------------------------------
def decode(str2decode):
    try:
        return str2decode.decode('windows-1251')
    except:
        return str2decode


# -------------------------------------------------------------------------------------------------
def __onerror_handler__(func, path, exc_info):
    """
    Error handler for ``shutil.rmtree``.

    If the error is due to an access error (read only file)
    it attempts to add write permission and then retries.

    If the error is for another reason it re-raises the error.

    Usage : ``shutil.rmtree(path, onerror=onerror)``
    """
    import stat
    # if not os.access(path, os.W_OK):
    # Is the error an access error ?
    os.chmod(path, stat.S_IWRITE)
    os.chmod(path, stat.S_IWUSR)
    func(path)
    # else:
    #   raise BaseException(exc_info)


# -------------------------------------------------------------------------------------------------
def current_time_str():
    return datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')


# -------------------------------------------------------------------------------------------------
def st_time_to_utc(str_st_time):
    str_st_time = str_st_time.rsplit(' ', 1)[0]  # отрезал MSK/MSD на конце
    dt_local = datetime.strptime(str_st_time, '%d.%m.%y %H:%M:%S')
    dt_utc = datetime.utcfromtimestamp(dt_local.timestamp())
    return dt_utc.isoformat()


# -------------------------------------------------------------------------------------------------
def need_retry(err_str):
    return sum([err_str.count(msg) for msg in ERROR_MESSAGES])


# -------------------------------------------------------------------------------------------------
def make_dir(path):
    try:
        if not os.path.exists(path):
            os.makedirs(path)
    except BaseException as e:
        log('ERROR: can''t create directory "{}" ({})'.format(path, e))


# -------------------------------------------------------------------------------------------------
def is_file_item(text_item):
    return text_item[-1] != '\\'  # Последний символ в строке с каталогом будет "слэш".


# -------------------------------------------------------------------------------------------------
def remove_dir(path):
    shutil.rmtree(path, onerror=__onerror_handler__)


# -------------------------------------------------------------------------------------------------
def clean(path, masks=None, write_log=True):
    if os.path.exists(path):
        try:
            if masks:
                if write_log:
                    log('CLEANING {} for {} files'.format(path, masks))
                for mask in masks:
                    # чистим все файлы по маске mask
                    [os.remove(os.path.join(d, filename)) for d, _, files in os.walk(path) for filename in
                     fnmatch.filter(files, mask)]
            else:
                if write_log:
                    log('CLEANING {}'.format(path))
                # Сначала чистим все файлы,
                [os.remove(os.path.join(d, filename)) for d, _, files in os.walk(path) for filename in files]
                # потом чистим все
                remove_dir(path)
        except FileNotFoundError:
            pass  # если папка отсутствует, то продолжаем молча
        except BaseException as e:
            log('ERROR when cleaning path="{}" ({})'.format(path, e))
            return False
    return True


# -------------------------------------------------------------------------------------------------
def retry(err_str, func, *args):
    log('------------ NEED RETRY ({}). Waiting for 30 sec.'.format(err_str))
    time.sleep(30)
    log('-------------RETRYING NOW.')
    func(*args)


# -------------------------------------------------------------------------------------------------
class GlobalSettings:
    def __init__(self):
        self.stcmd = ''
        self.starteam_server = ''
        self.starteam_port = ''
        self.starteam_login = ''
        self.starteam_project = ''
        self.starteam_view = ''
        self.starteam_password = ''
        self.view_label = ''
        self.git_url = ''

        self.__success = False
        self.read_config()

    def was_success(self):
        return self.__success

    def read_config(self):
        ini_filename = filename('ini')
        section_special = 'SPECIAL'
        section_common = 'COMMON'
        try:
            if not os.path.exists(ini_filename):
                raise FileNotFoundError('NOT FOUND ' + ini_filename)
            parser = configparser.RawConfigParser()
            res = parser.read(ini_filename, encoding="UTF-8")
            if res.count == 0:  # если файл настроек не найден
                raise FileNotFoundError('NOT FOUND {}'.format(ini_filename))
            self.stcmd = parser.get(section_common, 'stcmd').strip()
            self.starteam_server = parser.get(section_common, 'StarteamServer').strip()
            self.starteam_port = parser.get(section_common, 'StarteamPort').strip()
            self.starteam_project = parser.get(section_special, 'StarteamProject').strip()
            self.starteam_view = parser.get(section_special, 'StarteamView').strip()
            self.starteam_login = parser.get(section_special, 'StarteamLogin').strip()
            self.view_label = parser.get(section_special, 'ViewLabel').strip()
            self.git_url = parser.get(section_special, 'Git').strip()

            # проверка Labels -----------------------------------
            if not self.view_label:  # Если не дали совсем никаких меток для загрузки
                raise ValueError('NO ViewLabel defined in {}'.format(ini_filename))

            if not self.git_url:  # Если не дали совсем никаких меток для загрузки
                raise ValueError('NO Git defined in {}'.format(ini_filename))

            # проверка stsmd -----------------------------------
            if self.stcmd:  # если пусть к stcmd не задан
                self.stcmd = os.path.normpath(self.stcmd)
                self.stcmd = self.stcmd + os.sep + 'stcmd.exe'
                if not os.path.exists(self.stcmd):
                    raise FileNotFoundError('NOT FOUND ' + self.stcmd)
            else:
                raise FileNotFoundError('NOT DEFINED path to stcmd')

        except BaseException as e:
            log('ERROR when reading settings from file "{}":\n\t\t{}'.format(ini_filename, e))

        else:
            self.__success = True
            log('SETTINGS LOADED:\n\t'
                'StarteamProject = {}\n\t'
                'StarteamView = {}\n\t'
                'Path to stcmd.exe = {}\n\t'
                'Label = {}\n\t'
                'Git={}'.
                format(self.starteam_project, self.starteam_view, self.stcmd,
                       self.view_label, self.git_url))


# -------------------------------------------------------------------------------------------------
def get_password(message_text):
    import getpass
    # running under PyCharm or not
    if 'PYCHARM_HOSTED' in os.environ:
        return getpass.fallback_getpass(message_text)
    else:
        return getpass.getpass(message_text)


# -------------------------------------------------------------------------------------------------
def ask_starteam_password(settings):
    if settings.starteam_password == '':
        settings.starteam_password = get_password('Maestro, please, ENTER StarTeam PASSWORD for "{}":'.
                                                  format(settings.starteam_login))
    result = settings.starteam_password.strip() != ''
    if not result:
        log('ERROR: Empty password!')
    return result


# -------------------------------------------------------------------------------------------------
def git_init(git_url):
    # bare_repo = Repo.init(os.path.join(DIR_GIT_BARE_REPO, 'bare-repo'), bare=True)
    # del bare_repo
    # cloned_repo = Repo.clone_from(DIR_GIT_BARE_REPO, DIR_GIT_REPO)
    # log('Success repo init for path={}'.format(DIR_GIT_BARE_REPO))
    # return cloned_repo
    git_repo = Repo.init(PATH_GIT_REPO)
    origin = git_repo.create_remote('origin', git_url)
    exists = origin.exists()
    # log('{}'.format(exists))
    if exists:
        #origin.fetch()
        #git_repo.create_head('master', origin.refs.master)
        #git_repo.heads.master.set_tracking_branch(['origin'])
        return git_repo
    else:
        return None


# -------------------------------------------------------------------------------------------------
def git_add_file(git_repo, file_path, file_name, author, date, comment, revision):
    full_path = os.path.join(PATH_GIT_REPO, file_path, file_name)
    LOCK.acquire(True)
    try:
        success = False
        try:
            git_repo.index.add([full_path])
        except Exception as exc:
            err_str = str(exc)
            if need_retry(str(exc)):
                retry(err_str, git_add_file, git_repo, file_path,
                      file_name, author, date, comment, revision)
                return
            else:
                kill_app('Add exception: {}'.format(exc))

        try:
            git_author = Actor(author, '')
            commit_time = st_time_to_utc(date)
            git_repo.index.commit('{}'.format(comment) if comment else '',
                                  head=True,
                                  author=git_author,
                                  commit_date=commit_time)
            #git_repo.active_branch.commit = git_repo.commit('master')
            success = True
        except Exception as exc:
            kill_app('Commit exception: {}'.format(exc))

        try:
            git_repo.active_branch.commit = git_repo.commit('master')
        except Exception as exc:
            kill_app('Active_branch set up exception: {}'.format(exc))

        if success:
            log('Committed {} rev {}'.format(full_path, revision))
        else:
            log('NOT committed {} rev {}'.format(full_path, revision))
    finally:
        LOCK.release()


# -------------------------------------------------------------------------------------------------
def st_list_anything(settings, command, extra, what, st_path):
    launch_string = quote(settings.stcmd)
    launch_string += ' {} {} -nologo -x  -p "{}:{}@{}:{}/{}/{}/{}" -cfgl {}'.format(
        command,
        extra,
        settings.starteam_login,
        settings.starteam_password,
        settings.starteam_server,
        settings.starteam_port,
        settings.starteam_project,
        settings.starteam_view,
        st_path,
        quote(settings.view_label))
    message_text = 'Loading {} from Starteam path="{}". Please wait...'.format(what, st_path)
    log(message_text)

    process = subprocess.Popen(launch_string, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = process.communicate()
    process.stdout.close()
    if err:
        err_str = decode(err)
        message_text = 'Can not load {} path="{}".'.format(what, st_path)
        if need_retry(err_str):
            log(message_text)
            retry(err_str, st_list_anything, settings, command, extra, what, st_path)
        else:
            kill_app(message_text + '\n' + err_str)
    else:
        str_res = decode(out)
        # log('DEBUG OUTPUT: {}'.format(str_res))
        if str_res:
            st_list = str_res.splitlines()
            del st_list[0]  # В первой строке будет путь к виду стартима
            if command != COMMAND_HIST:
                st_list.sort()
            return st_list
        else:
            message_text = 'Can not load {} path="{}". NO RESULT'.format(
                what, st_path)
            kill_app(message_text)


# -------------------------------------------------------------------------------------------------
def st_download_one_file(settings, st_path, st_file, root_dir_path, revision):
    full_temp_path = os.path.join(PATH_TEMP, str(uuid.uuid4()), st_path)
    launch_string = '"{}" co -nologo -stop -q -x -o -is -p "{}:{}@{}:{}/{}/{}/{}" -fp "{}" -vn {} "{}"'.format(
        settings.stcmd,
        settings.starteam_login,
        settings.starteam_password,
        settings.starteam_server,
        settings.starteam_port,
        settings.starteam_project,
        settings.starteam_view,
        st_path,
        full_temp_path,  # Выгружаем во временный каталог с уникальным названием
        revision,
        st_file)
    message_text = 'Loading FILE from Starteam path="{}{}" rev {} temp_path="{}". Please wait...'.format(
        st_path, st_file, revision, full_temp_path)
    log(message_text)

    process = subprocess.Popen(launch_string, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = process.communicate()
    process.stdout.close()
    if err:
        err_str = decode(err)
        message_text = 'Can not download FILE path="{}{}" rev {}.'.format(st_path, st_file, revision)
        if need_retry(err_str):
            log(message_text)
            retry(err_str, st_download_one_file, settings, st_path, st_file, root_dir_path, revision)
        else:
            kill_app(message_text + '\n' + err_str)
    else:
        # Иногда файлы в предыдущих ревизиях имеют другое имя, поэтому
        # файл выгружается во веменный каталог и переносится из временного
        # каталога в репозторий с перименованием на актуальное название
        if os.path.exists(full_temp_path):  # Иногда файл есть в истории, но из стартима уже удален. Тогда
            files = []                      # ничего не скачается, каталог не будет создан. Это нормально.
            try:
                # Выбираем только файлы во временно каталоге (иногда в дереве каталогов несколько
                # одноименных файлов и выкачиваются они все со вложенными каталогами, они будут
                # загружены позже, когда доберемся до их уровня в дереве, сойчас пропускаем)
                files = [f for f in os.listdir(full_temp_path) if os.path.isfile(os.path.join(full_temp_path, f))]
            except BaseException as e:
                kill_app('is file ' + str(e))
            if len(files) != 1:
                # Если файл все-таки оказался в каталоге не один, то это ошибка. Такого не встерчалось.
                kill_app('Too many files in temp folder "{}": {}'.format(full_temp_path, files))
            else:
                try:
                    # Создаем каталог в папке репозитория
                    os.makedirs(os.path.join(root_dir_path, st_path), exist_ok=True)
                except BaseException as e:
                    kill_app('mkdir ' + str(e))
                try:
                    # Переносим файл в каталог репозитория с новым именем
                    shutil.move(os.path.join(full_temp_path, files[0]),
                                os.path.join(root_dir_path, st_path, st_file))
                    # clean(full_temp_path, write_log=False) решил не удалять временный каталог
                    return True
                except BaseException as e:
                    kill_app('copy ' + str(e))
        else:
            # Иногда файл есть в истории, но из стартима уже удален. Тогда
            # ничего не скачается, каталог не будет создан. Это нормально.
            log('File {} rev {} not loaded. Possibly it was deleted from StarTeam. Not error.'.format(
                st_file, revision))


# -------------------------------------------------------------------------------------------------
def st_download_files_and_commit_to_git(settings, git_repo, list_history, st_path):
    try:
        for item in list_history:
            if st_download_one_file(settings, st_path, item[KEY_FILENAME],
                                    PATH_GIT_REPO, item[KEY_REVISION]):
                git_add_file(git_repo,
                             item[KEY_PATH],
                             item[KEY_FILENAME],
                             item[KEY_AUTHOR],
                             item[KEY_DATE],
                             item[KEY_COMMENT] if KEY_COMMENT in item else None,
                             item[KEY_REVISION])
    except BaseException as e:
        kill_app(str(e))


# -------------------------------------------------------------------------------------------------
def st_list_history(settings, st_path):
    st_list = st_list_anything(settings, COMMAND_HIST, '', 'HISTORY', st_path)
    if not st_list:
        return {}
    history_list = []
    history_item = {KEY_PATH: st_path}
    comment_begin = False
    for line in st_list:
        line = line.strip()
        # log(line)
        if line.startswith('History for:'):  # начало блока информации о файле
            name = line.rsplit('History for: ', 1)[1]  # последнее слово с конца - название файла
            history_item[KEY_FILENAME] = name

        elif line.startswith('Revision:'):
            revision = line.split(' ', 2)[1]  # второая цифра от начала - номер ревизии
            history_item[KEY_REVISION] = int(revision)

        elif line.startswith('Author:'):
            author_date = line.split(' Date: ', 2)
            author = author_date[0]
            date = author_date[1]
            history_item[KEY_AUTHOR] = author.replace('Author: ', '', 1)
            history_item[KEY_DATE] = date
            comment_begin = True

        elif line == '----------------------------':
            # конец блока информации о ревизии -
            # ревизия уже есть в элементе, значит
            # начало блока ревизии уже было
            if KEY_REVISION in history_item:
                history_list.append(history_item)
                history_item = {KEY_PATH: st_path, KEY_FILENAME: history_item[KEY_FILENAME]}
                comment_begin = False

        elif line == '=============================================================================':
            # конец блока информации о файле
            history_list.append(history_item)
            history_item = {KEY_PATH: st_path, KEY_FILENAME: history_item[KEY_FILENAME]}
            comment_begin = False

        elif comment_begin:
            if KEY_COMMENT in history_item:
                history_item[KEY_COMMENT] += line
            else:
                history_item[KEY_COMMENT] = line
    # сортировка по путь - файл - ревизия
    list_return = sorted(history_list, key=lambda tup: (tup[KEY_PATH], tup[KEY_FILENAME], tup[KEY_REVISION]))
    # группировка по названию файла
    dict_return = {}
    for item in list_return:
        file_name = item[KEY_FILENAME]
        history_items_of_file = dict_return[file_name] if file_name in dict_return else []
        history_items_of_file.append(item)
        dict_return.update({file_name: history_items_of_file})

    log('List of history items for {}: {}'.format(st_path, dict_return))
    return dict_return


# -------------------------------------------------------------------------------------------------
def st_list_dirs(settings, st_path, excluded_folders=None):
    st_list = st_list_anything(settings, COMMAND_LIST, '-cf', 'FOLDERS', st_path)
    if not st_list:
        return []
    list_dirs = []
    list_return = []
    for item in st_list:
        if not is_file_item(item):
            list_dirs.append(item.strip().replace('\\', ''))
    list_dirs.sort()

    if excluded_folders:
        excluded_folders_lower = [excluded_folder.lower() for excluded_folder in excluded_folders]
        for item in list_dirs:
            if item.lower() not in excluded_folders_lower and \
                    'not in view' not in item.lower() and \
                    'missing ' not in item.lower():
                list_return.append(item)
    else:
        list_return = list_dirs

    log('List of subfolders for {}: {}'.format(st_path, list_return))
    return list_return


# -------------------------------------------------------------------------------------------------
def st_process_dir(settings, git_repo, futures, st_path):
    dict_history = st_list_history(settings, st_path)
    for file_name in dict_history.keys():
        list_history = dict_history[file_name]
        futures.append(EXECUTOR.submit(
            st_download_files_and_commit_to_git,
            settings, git_repo, list_history, st_path))


# -------------------------------------------------------------------------------------------------
def starteam_run(settings, git_repo, futures, st_path, excluded_folders=None):
    st_folders = st_list_dirs(settings, st_path, excluded_folders)
    try:
        for st_folder in st_folders:
            next_st_path = st_path + st_folder + '/'
            st_process_dir(settings, git_repo, futures, next_st_path)

        for st_folder in st_folders:
            next_st_path = st_path + st_folder + '/'
            starteam_run(settings, git_repo, futures, next_st_path)  # рекурсия
    except BaseException as e:
        kill_app('Exception {}'.format(e))


# -------------------------------------------------------------------------------------------------
def run():
    log('=' * 120)
    log('STARTED')

    global_settings = GlobalSettings()
    cleaned = clean(PATH_GIT_REPO) and clean(PATH_TEMP)
    not_inited = not global_settings.was_success() or not cleaned or not ask_starteam_password(global_settings)
    git_repo = git_init(global_settings.git_url)
    if not_inited or not git_repo:
        return

    futures = []
    starteam_run(global_settings, git_repo, futures, '',
                 ['BLL', 'BLL_Client', 'Doc', '_Personal', 'DBOReports', 'BUILD', 'Scripts',
                  '_TZ', '_ProjectData', '_ProjectData2', 'Config', 'DLL'])
                  
    # '###MBC41', 'SETUP', 'BASE', 'RT_Tpl', 'WWW', 'RTF',  'XSD', 'History',
    # 'WWW_react', '### Native ReactUI', 'EXTERNAL', 'MIG_UTIL', #'BLS'

    done, not_done = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_EXCEPTION)
    for future in concurrent.futures.as_completed(done):
        try:
            future.result()
        except Exception as exc:
            log('Thread generated an exception: {}'.format(exc))

    log('FINISHED')


run()
