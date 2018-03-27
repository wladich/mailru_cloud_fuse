# -*- coding: utf-8 -*-
import requests
import requests_toolbelt
from requests_toolbelt.multipart.encoder import total_len
import logging
from functools import wraps
import time

class NotFoundError(Exception):
    pass


class FileExistsError(Exception):
    pass


class ServerError(Exception):
    pass


max_retries = 1000
retry_wait = 1


def retry_on_server_error(f):
    @wraps(retry_on_server_error)
    def wrapped(*args, **kwargs):
        retries = 0
        while True:
            try:
                return f(*args, **kwargs)
            except (ServerError, requests.ConnectionError) as e:
                retries += 1
                time.sleep(retry_wait)
                if retries == max_retries:
                    raise
                logging.warning(
                    'Retrying #%s in %s%r(kwargs=%s) after error: %s' % (retries, f.__name__, args, kwargs, e))
    return wrapped


class Cloud(object):
    max_download_zip_size = 1000000000
    download_prefix = '.'

    def __init__(self, login, password):
        self.session = requests.Session()
        self.authenticate(login, password)
        self.csrf_token = self.get_csrf_token()

    @retry_on_server_error
    def authenticate(self, login, password):
        response = self.session.post('https://auth.mail.ru/cgi-bin/auth?lang=ru_RU&from=authpopup', data={
            'page': 'https://cloud.mail.ru/?from=promo',
            'FailPage': '',
            'Domain': 'mail.ru',
            'Login': login,
            'Password': password,
            'new_auth_form': '1',
            'saveauth': '1'})
        if response.status_code != 200:
            raise ServerError('Status code %s' % response.status_code)
        if response.url != 'https://cloud.mail.ru/?from=promo&from=authpopup':
            raise ServerError('response url  is "%s"' % response.url)

    @retry_on_server_error
    def get_csrf_token(self):
        response = self.session.post('https://cloud.mail.ru/api/v2/tokens/csrf', data={'api': '2'})
        jresp = response.json()
        if response.status_code != 200:
            raise ServerError('Status code %s' % response.status_code)
        token = jresp['body']['token']
        if not token:
            raise ServerError('Empty token')
        return token

    @retry_on_server_error
    def api_tokens_download(self):
        response = self.session.post('https://cloud.mail.ru/api/v2/tokens/download',
                                     data={'api': '2', 'token': self.csrf_token})
        if response.status_code != 200:
            raise ServerError('Status code %s' % response.status_code)
        return response.json()['body']['token']

    _servers = None

    @property
    def servers(self):
        if self._servers is None:
            dispatcher_response = self.api_dispatcher()
            servers = {}
            for k, v in dispatcher_response.items():
                servers[k] = v[0]['url']
            self._servers = servers
        return self._servers

    @retry_on_server_error
    def api_folder(self, path, page):
        response = self.session.get('https://cloud.mail.ru/api/v2/folder?limit=500&offset=%s' % (500 * page),
                                    params={'token': self.csrf_token, 'home': path})
        if response.status_code not in (200, 404):
            raise ServerError('Status code %s' % response.status_code)
        jresp = response.json()
        if response.status_code == 200:
            assert jresp['body']['kind'] == 'folder'
            return jresp['body']
        else:
            assert jresp['body']['home']['error'] == 'not_exists'
            raise NotFoundError

    @retry_on_server_error
    def api_file(self, path):
        response = self.session.get('https://cloud.mail.ru/api/v2/file',
                                    params={'token': self.csrf_token, 'home': path})
        if response.status_code not in (200, 404):
            raise ServerError('Status code %s' % response.status_code)
        jresp = response.json()
        if response.status_code == 200:
            return jresp['body']
        else:
            assert jresp['body']['home']['error'] == 'not_exists'
            raise NotFoundError

    @retry_on_server_error
    def api_dispatcher(self):
        response = self.session.get('https://cloud.mail.ru/api/v2/dispatcher', params={'token': self.csrf_token})
        if response.status_code != 200:
            raise ServerError('Status code %s' % response.status_code)
        return response.json()['body']

    @retry_on_server_error
    def api_zip(self, paths):
        if not hasattr(paths, '__iter__'):
            paths = [paths]
        home_list = []
        for path in paths:
            path = path.encode('utf-8')
            home_list.append('"%s"' % path)
        home_list = '[%s]' % ','.join(home_list)
        response = self.session.post('https://cloud.mail.ru/api/v2/zip', data={
            'home_list': home_list,
            'name': self.download_prefix,
            'cp866': 'false',
            'api': '2',
            'token': self.csrf_token
        })
        if response.status_code not in (200, 404):
            raise ServerError('Status code %s' % response.status_code)
        jresp = response.json()
        if response.status_code == 200:
            return jresp['body']
        else:
            raise NotFoundError

    def upload_blob(self, fd):
        url = self.servers['upload']
        start_pos = fd.tell()
        retries = 0
        while True:
            try:
                fd.seek(start_pos, 0)
                m = requests_toolbelt.MultipartEncoder(fields={'file': ('filename', fd)})
                response = self.session.post(url, params={'cloud_domain': '2'}, data=m,
                                             headers={'Content-Type': m.content_type})
                if response.status_code != 200:
                    raise ServerError('Server returned status code %s' % response.status_code)
                result = response.text.split(';')
                if len(result) > 2:
                    raise ServerError('Server reported error: %s' % result[2:])
                hash_, size = result
                size = int(size)
                if size != total_len(fd):
                    raise ServerError('Invalid blob size in server response')
                return {'hash': hash_, 'size': size}
            except (ServerError, requests.ConnectionError) as e:
                if retries == max_retries:
                    raise
                retries += 1
                logging.warning('Retrying #%s in upload_blob() after error: %s' % (retries, e))
                time.sleep(retry_wait)

    @retry_on_server_error
    def api_file_add(self, path, blob, conflict='strict'):
        """
            returns added file name, can be different if file with specified name exists
        """
        response = self.session.post('https://cloud.mail.ru/api/v2/file/add', data={
            'home': path,
            'hash': blob['hash'],
            'size': blob['size'],
            'conflict': conflict,
            'api': '2',
            'token': self.csrf_token
        })
        if response.status_code not in (200, 404):
            raise ServerError('Status code %s' % response.status_code)
        jresp = response.json()
        if response.status_code == 200:
            return jresp['body']
        else:
            assert jresp['body']['home']['error'] == 'exists', jresp['body']['home']['error']
            raise FileExistsError

    @retry_on_server_error
    def api_space(self):
        response = self.session.get('https://cloud.mail.ru/api/v2/user/space', params={'token': self.csrf_token})
        if response.status_code != 200:
            raise ServerError('Status code %s' % response.status_code)
        return response.json()['body']

    @retry_on_server_error
    def api_file_move(self, path, target_dir, conflict='strict'):
        response = self.session.post('https://cloud.mail.ru/api/v2/file/move', data={
            'home': path,
            'conflict': conflict,
            'folder': target_dir,
            'api': '2',
            'token': self.csrf_token
        })
        if response.status_code != 200:
            raise ServerError('Status code %s' % response.status_code)

    @retry_on_server_error
    def api_file_remove(self, path):
        """
            removes files and folders
            does not raise exception if file doesnot exist
        """
        response = self.session.post('https://cloud.mail.ru/api/v2/file/remove', data={
            'home': path,
            'api': '2',
            'token': self.csrf_token
        })
        if response.status_code != 200:
            raise ServerError('Status code %s' % response.status_code)

    @retry_on_server_error
    def api_folder_add(self, path, conflict='strict'):
        """
        conflict: strict or rename
        returns name of created folder
        """
        response = self.session.post('https://cloud.mail.ru/api/v2/folder/add', data={
            'home': path,
            'conflict': conflict,
            'api': '2',
            'token': self.csrf_token
        })
        if response.status_code not in (200, 404):
            raise ServerError('Status code %s' % response.status_code)
        jresp = response.json()
        if response.status_code == 200:
            return jresp['body']
        else:
            assert jresp['body']['home']['error'] == 'exists'
            raise FileExistsError

    @retry_on_server_error
    def api_file_rename(self, path, new_name, conflict='strict'):
        """
            Renanme file or folder

            new_name: without path

            conflict: rename or strict

            returns: new name, can differ from specified if already exists
        """
        response = self.session.post('https://cloud.mail.ru/api/v2/file/rename', data={
            'home': path,
            'name': new_name,
            'conflict': conflict,
            'api': '2',
            'token': self.csrf_token
        })
        if response.status_code not in (200, 404):
            raise ServerError('Status code %s' % response.status_code)
        jresp = response.json()
        if response.status_code == 200:
            return jresp['body']
        else:
            assert jresp['body']['home']['error'] in ('not_exists', 'exists', 'invalid')
            if jresp['body']['home']['error'] == 'not_exists':
                raise NotFoundError
            else:
                raise FileExistsError

    def file_exists(self, path):
        """
            returns: False, file, folder
        """
        try:
            return self.api_file(path)['type']
        except NotFoundError:
            return False

    def upload_file(self, path, fd):
        blob = self.upload_blob(fd)
        self.api_file_add(path, blob)

    @retry_on_server_error
    def get_file_reader(self, path):
        url = self.servers['get'][:-1] + path
        response = self.session.get(url, stream=True)
        if response.status_code not in (200, 404):
            raise ServerError('Status code %s' % response.status_code)
        if response.status_code == 404:
            raise NotFoundError
        return response.raw

    def dir_list(self, path):
        resp = self.api_folder(path, 0)
        items = resp['list']
        items_count = resp['count']['files'] + resp['count']['folders']
        pages = (items_count - 1) / 500 + 1
        for page in xrange(1, pages):
            resp = self.api_folder(path, page)
            items.extend(resp['list'])
        return items
