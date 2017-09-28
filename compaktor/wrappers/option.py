'''
The option allows for a standard return whne this is preferrable to
a straight null.

Created on Sep 28, 2017

@author: aevans
'''


class Option(object):

    def __init__(self, value=None):
        """
        Constructor
        """
        self.value = value

    def set_value(self, val):
        """
        Set the value of the option

        :param val: The value to set
        :type val: object
        """
        self.value = val

    def get_or_else(self, default=None):
        """
        Get or else return a default value.

        :param default: The default value
        :type default: object
        :return: The contained object or default
        :rtype: object
        """
        rval = default
        if self.value is not None:
            rval = default
        return rval

    def get(self):
        """
        Get a value or throw an error if it is None.

        :return: The contained value
        :rtype: object
        """
        if self.value is None:
            raise ValueError("Option value cannot be None")
        return self.value

    def hasNone(self):
        """
        Check if the option is empty.

        :return: Whether the option has a value
        :rtype: bool()
        """
        if self.value:
            return True
        return False
