#!/usr/bin/env python

import logging
import os

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

import swiftclient.client as swift

AUTH_URL = os.getenv('OS_AUTH_URL')
USER = os.getenv('OS_USERNAME')
KEY = os.getenv('OS_PASSWORD')
TENANT_NAME = os.getenv('OS_TENANT_NAME')
AUTH_VERION = 2

# i.e. "/"
MOUNT_CONTAINER = 'swift-fuse'

class SwiftFuse(LoggingMixIn, Operations):
    def __init__(self):
        self.swift_client = swift.Connection(authurl=AUTH_URL, user=USER,
                                             key=KEY,
                                             auth_version=AUTH_VERION,
                                             tenant_name=USER)

    def create(self, path, mode):
        try:
            self.swift_client.put_object(MOUNT_CONTAINER, path, None)
        except swift.ClientException as e:
            raise
        return 0

    def read(self, path, size, offset, fh):
        headers, body = self.swift_client.get_object(MOUNT_CONTAINER, path)
        with open(body) as f:
            f.seek(offset, 0)
            return f.read(size)

    def readdir(self, path, fh):
        headers, objects = self.swift_client.get_container(MOUNT_CONTAINER)
        contents = ['.', '..']
        for obj in objects:
            contents.append(obj['name'].encode('utf-8'))
        return contents


if __name__ == '__main__':
    if len(argv) != 2:
        print('usage: %s <mountpoint>' % argv[0])
        exit(1)

    logging.getLogger().setLevel(logging.DEBUG)
    fuse = FUSE(SwiftFuse(), argv[1], foreground=True)
