# -*- coding: utf-8 -*-
__author__ = "e.bataev@corp.mail.ru"
import os
from getpass import getuser


class LocalFSClient(object):
    def __init__(self):
        pass

    def homedir(self):
        return "/user/%s/" % getuser()

    def exists(self, path):
        return os.path.isdir(path)
