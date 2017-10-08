'''
Creates file objects similar to Java.
Created on Oct 7, 2017

@author: aevans
'''

import os


class File():
    """
    File object containing pat parts.
    """

    def __init__(self, file_path=None, file_path_parts=None):
        if file_path is None and file_path_parts is None:
            raise ValueError("File Path and File Path Parts Cannot be None.")
        self.full_path = None
        self.dirs = []
        self.__setup(file_path, file_path_parts)

    def __setup(self, file_path, file_path_parts):
        """
        Setup the file

        :param file_path: The file path
        :type file_path: str()
        :param file_path_parts: The list of file parts
        :type file_path_parts: list()
        """
        if file_path is not None:
            self.full_path = file_path
            self.dirs = os.path.split(os.path.sep)[:-1]
        else:
            self.full_path = os.path.sep.join(file_path_parts)
            self.dirs = file_path_parts
