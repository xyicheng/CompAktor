'''
Created on Oct 7, 2017

@author: aevans
'''

class OutputStream():
    """
    File output stream
    """

    def __init__(self, stream=None):
        """
        Constructor

        :param stream: Optional stream if not using closeability
        :type stream: io.open() 
        """
        self.is_open = False
        self.__stream = stream
        self.__next =None
        if self.__stream is not None:
            self.__next = self.__stream.readline()
            self.is_open = True

    def __enter__(self, fpath):
        """
        Open the stream

        :param fpath: File path to open
        :type fpath: str()
        """
        self.__stream = open(fpath, 'w')

    def write(self, line):
        """
        Write to the stream
        """
        if self.is_open is False:
            raise ValueError("Stream Not Open")
        self.__stream.write(line)

    def __exit__(self):
        """
        Close the stream
        """
        if self.is_open:
            self.__stream.close()
            self.is_open = False

    def close(self):
        """
        Close the stream if not using closeability
        """
        if self.is_open:
            self.__stream.close()
            self.is_open = False


class InputStream():
    """
    Abstracts a stream to make it easier to handle.
    """

    def __init__(self, stream=None):
        """
        Constructor
        """
        self.is_open = False
        self.__stream = stream
        self.__next =None
        if self.__stream is not None:
            self.__next = self.__stream.readline()
            self.is_open = True

    def __enter__(self, file_path):
        """
        Open the stream
        """
        self.__stream = open(file_path, 'r')
        self.__next = self.__stream.readline()
        if len(self.__next) is 0:
            self.__next = None
        return self

    def has_next(self):
        """
        Return whether there is a next element

        :return: The next string or None
        :rtype: str()
        """
        return self.__next != None

    def next(self):
        """
        Get the next element or None
        :return: The next string or None
        :rtype: str()
        """
        r_next = self.__next
        if r_next is not None:
            self.__next = self.__stream.readline()
            if len(self.__next) is 0:
                self.__next = None
        return r_next

    def __exit__(self):
        """
        Close the stream
        """
        if self.is_open:
            self.__stream.close()
            self.is_open = False

    def close(self):
        """
        Close the stream if not using closeability.
        """
        if self.is_open:
            self.__stream.close()
            self.is_open = False
