#!/usr/bin/env python

from time import time
import logging
import os
import stat

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time

import fuse
from fuse import FUSE
from fuse import FuseOSError
from fuse import Operations
from fuse import LoggingMixIn
from fuse import fuse_get_context

import swiftclient.client as swift

AUTH_URL = os.getenv('OS_AUTH_URL')
USER = os.getenv('OS_USERNAME')
KEY = os.getenv('OS_PASSWORD')
TENANT_NAME = os.getenv('OS_TENANT_NAME')
AUTH_VERION = 2

# i.e. "/"
MOUNT_CONTAINER = 'swift-fuse'


class SwiftFuse(LoggingMixIn, Operations):
    """
    Implementation Notes:

    Files and Directories
    Given Swift doesn't distinguish between files and dirs, directories should
    be represented by a trailing slash.  This should be transparent to the
    user.  i.e.

    $ mkdir foo

    will PUT an object called "foo/" in Swift.
    """

    def __init__(self):
        self.swift_client = swift.Connection(authurl=AUTH_URL, user=USER,
                                             key=KEY,
                                             auth_version=AUTH_VERION,
                                             tenant_name=USER)

    def getattr(self, path, fh=None):
        """
        http://sourceforge.net/apps/mediawiki/fuse/index.php?
            title=Getattr%28%29
        """
        st = {}
        if path.endswith('/'):
            st['st_mode'] = S_IFDIR | 0755
            st['st_nlink'] = 2  # . and .. at a minimum
        else:
            st['st_mode'] = stat.S_IFREG | 0666
            st['st_nlink'] = 1
            # TODO: extract size from swift
            st['st_size'] = 1
        st['st_ctime'] = time()
        st['st_mtime'] = st['st_ctime']
        st['st_atime'] = st['st_ctime']
        return st

    def readdir(self, path, fh):
        headers, objects = self.swift_client.get_container(MOUNT_CONTAINER)
        contents = ['.', '..']
        for obj in objects:
            contents.append(obj['name'].split('/')[0].rstrip('/'))
        return contents

    def create(self, path, mode):
        try:
            self.swift_client.put_object(MOUNT_CONTAINER, path.lstrip('/'),
                                         None)
        except swift.ClientException as e:
            raise
        return 0

    def read(self, path, size, offset, fh):
        headers, body = self.swift_client.get_object(MOUNT_CONTAINER, path)
        with open(body) as f:
            f.seek(offset, 0)
            return f.read(size)
        return ''

    def mkdir(self, path, mode):
        # use trailing slash to indicate a dir
        path = path.lstrip('/').rstrip('/') + '/'
        headers, body = self.swift_client.put_object(
                MOUNT_CONTAINER, path, None)
        return 0


if __name__ == '__main__':
    if len(argv) != 2:
        print('usage: %s <mountpoint>' % argv[0])
        exit(1)

    logging.getLogger().setLevel(logging.DEBUG)
    fuse = FUSE(SwiftFuse(), argv[1], foreground=True)
