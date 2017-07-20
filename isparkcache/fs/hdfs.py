# -*- coding: utf-8 -*-
__author__ = "e.bataev@corp.mail.ru"

from getpass import getuser
from subprocess import check_output

from snakebite.client import AutoConfigClient


class HDFSClient(object):
    def __init__(self):
        check_output("hadoop")
        self.fs = AutoConfigClient()

    def homedir(self):
        return "/user/%s/" % getuser()

    def exists(self, path):
        try:
            return self.fs.test(path)
        except Exception:
            return False
