#!/usr/bin/env python
# coding: utf-8
import logging
import os
import fusepy
import cloudapi
import errno
import stat
import tempfile
import json
import argparse

api = None
api_cache_dir = None


class CachedFile(object):
    def __init__(self, path):
        self.path = path
        self.fd = None
        self.changed = False
        self.ref_count = 1

    def inc_ref(self):
        self.ref_count += 1

    def dec_ref(self):
        self.ref_count -= 1

    def has_refs(self):
        return self.ref_count > 0

    def _retrieve(self):
        if not self.fd:
            retries = 10
            while True:
                if self.path is None:
                    raise fusepy.FuseOSError(errno.EACCES)
                self.fd = tempfile.TemporaryFile(dir=api_cache_dir)

                try:
                    res = api.api_file(self.path)
                except cloudapi.NotFoundError:
                    raise fusepy.FuseOSError(errno.ENOENT)
                expected_size = res['size']
                try:
                    f = api.get_file_reader(self.path)
                except cloudapi.NotFoundError:
                    return
                size = 0
                while True:
                    s = f.read(1024)
                    size += len(s)
                    if not s:
                        break
                    self.fd.write(s)
                if size == expected_size:
                    break
                logging.warning('File size mismatch (%s): expected %s, received %s, retrying' % (self.path, expected_size, size))
                retries -= 1
                if not retries:
                    raise Exception('File size mismatch (%s): expected %s, received %s, retrying' % (self.path, expected_size, size))

    def write(self, buf, offset):
        self._retrieve()
        self.fd.seek(offset)
        self.fd.write(buf)
        self.changed = True
        return len(buf)

    def _upload(self):
        if self.path is None:
            return
        self.fd.seek(0, 0)
        if api.file_exists(self.path):
            api.api_file_remove(self.path)
        api.upload_file(self.path, self.fd)

    def flush(self):
        if self.changed:
            self._upload()
        self.changed = False

    def read(self, size, offset):
        self._retrieve()
        self.fd.seek(offset)
        return self.fd.read(size)

    def truncate(self, len):
        if len:
            self._retrieve()
        if self.fd:
            self.fd.truncate(len)
        else:
            self.fd = tempfile.TemporaryFile(dir=api_cache_dir)
        self.changed = True
        self.flush()

    def close(self):
        if self.fd:
            self.fd.close()


class MRUC(fusepy.Operations):
    def __init__(self):
        self.fd = 0
        self.files = {}
        logging.info('Started Mail.ru Cloud FUSE FS')

    def __call__(self, op, *args):
        try:
            res = super(MRUC, self).__call__(op, *args)
        except fusepy.FuseOSError:
            logging.debug('%s%r', op, args)
            raise
        except:
            logging.exception('%s%r', op, args)
            raise
        logging.debug('%s%r', op, args)
        return res

    def readdir(self, path, fh):
        try:
            items = api.dir_list(path)
        except cloudapi.NotFoundError:
            raise fusepy.FuseOSError(errno.ENOENT)
        return ['.', '..'] + [item['name'].encode('utf-8') for item in items]

    def getattr(self, path, fh=None):
        try:
            res = api.api_file(path)
        except cloudapi.NotFoundError:
            raise fusepy.FuseOSError(errno.ENOENT)
        st = {}
        if res['kind'] == 'file':
            st = dict(
                st_mode=stat.S_IFREG | 0666,
                st_nlink=1,
                st_size=res['size'],
                st_mtime=res['mtime'],
                st_ctime=res['mtime'],
                st_atime=res['mtime'],
                st_uid=1000)
        elif res['kind'] == 'folder':
            st = dict(
                st_mode=stat.S_IFDIR | 0777,
                st_nlink=2,
                st_uid=1000)
        return st

    def statfs(self, path):
        res = api.api_space()
        st = dict(f_bsize=1024, f_frsize=1024, f_blocks=res['total'] * 1024,
                  f_bfree=(res['total'] - res['used']) * 1024,
                  f_bavail=(res['total'] - res['used']) * 1024)
        return st

    def next_fd(self):
        self.fd += 1
        return self.fd

    def open(self, path, flags):
        file_type = api.file_exists(path)
        if file_type == 'folder':
            raise fusepy.FuseOSError(-errno.EACCES)
        new_fd = self.next_fd()
        for file_obj in self.files.values():
            if file_obj.path == path:
                fo = self.files[new_fd] = file_obj
                file_obj.inc_ref()
                break
        else:
            fo = self.files[new_fd] = CachedFile(path)
        if not file_type:
            if flags & os.O_CREAT:
                fo.truncate(0)
            else:
                raise fusepy.FuseOSError(errno.ENOENT)
        else:
            if flags & os.O_TRUNC:
                fo.truncate(0)
        return new_fd

    def create(self, path, mode, fi=None):
        return self.open(path, os.O_CREAT | os.O_TRUNC | os.O_WRONLY)

    def read(self, path, size, offset, fh):
        return self.files[fh].read(size, offset)

    def write(self, path, data, offset, fh):
        return self.files[fh].write(data, offset)

    def flush(self, path, fh):
        self.files[fh].flush()

    def fsync(self, path, datasync, fh):
        self.files[fh].flush()

    def truncate(self, path, length, fh=None):
        if fh is None:
            fo = CachedFile(path)
            fo.truncate(length)
            fo.close()
        else:
            self.files[fh].truncate(length)

    def release(self, path, fh):
        fo = self.files[fh]
        fo.flush()
        fo.dec_ref()
        if not fo.has_refs():
            fo.close()
        del self.files[fh]

    def unlink(self, path):
        for fo in self.files.values():
            if fo.path == path:
                fo.path = None
        api.api_file_remove(path)

    def rename(self, old, new):
        if os.path.dirname(old) == os.path.dirname(new):
            if api.file_exists(new):
                self.unlink(new)
            api.api_file_rename(old, os.path.basename(new))
        else:
            raise fusepy.FuseOSError(errno.ENOTSUP)

    def mkdir(self, path, mode):
        api.api_folder_add(path)

    def rmdir(self, path):
        api.api_file_remove(path)

    def link(self, target, source):
        raise fusepy.FuseOSError(errno.ENOTSUP)

    def symlink(self, target, source):
        raise fusepy.FuseOSError(errno.ENOTSUP)

    def chmod(self, path, mode):
        pass

    def chown(self, path, uid, gid):
        pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mountpoint')
    parser.add_argument('-c', metavar='credentials.json', required=True)
    parser.add_argument('-t', metavar='TEMP_DIR', required=True)
    parser.add_argument('-l', metavar='logfile')

    conf = parser.parse_args()
    credentials = json.load(open(conf.c))

    tempfile.TemporaryFile(dir=conf.t)

    global api
    global api_cache_dir
    api = cloudapi.Cloud(credentials['login'], credentials['password'])
    api_cache_dir = conf.t

    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s', filename=conf.l)
    fuse = fusepy.FUSE(MRUC(), conf.mountpoint, foreground=False, nothreads=True, allow_other=True)


if __name__ == '__main__':
    main()
