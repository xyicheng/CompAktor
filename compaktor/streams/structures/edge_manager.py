'''
An Edge Manager

Created on Sep 28, 2017

@author: aevans
'''


from compaktor.wrappers.option import Option


class Manager(object):
    """
    A manager that can be overridden for quickly finding graph nodes.
    """

    def __init__(self, index):
        """
        Constructor

        :param index: The unique key to index on
        :type index: str()
        """
        self.__edge_map = {}
        self.__index = index

    def set_index(self, index):
        """
        Set the index for the node.

        :param index: The index to search on by default
        :type index: str()
        """
        self.__index = index

    def add_edge(self, edge):
        """
        Add an edge to the graph.  The index must be in the graph.

        :param edge: The edge dictionary
        :type edge: streams.objects.edge.Edge()
        :return: Any overwritten edge in an option
        :rtype: Option()
        """
        if self.__index not in edge.keys():
            err_msg = "Index not in Dictionary on Add\n"
            err_msg += "Index={}".format(self.__index)
            raise ValueError(err_msg)

        index = edge[self.__index]
        ret_opt = Option()
        if self.__edge_map[index]:
            existing = self.__edge_map[index]
            ret_opt.set_value(existing)
        self.__edge_map[index] = edge
        return ret_opt

    def search_by_key(self, key, val, all=False):
        """
        Search by the key.

        :param key: The index to search on
        :type key: str()
        :param val: The value to search for
        :type val: object
        :param all: Whether to return all edges
        :type all: bool()
        :return: An option containing any found edges
        :rtype: Option()
        """
        edges = []
        ret_opt = Option()
        for k in self.__edge_map.keys():
            edge = self.__edge_map[k]
            if key in edge.keys():
                kval = edge[key]
                if kval is not None:
                    if isinstance(kval, str):
                        if kval == val:
                            edges.append(edge)
                    elif kval is val:
                        edges.append(edge)
                elif val is None:
                    edges.append(edge)
        if len(edges) > 0:
            ret_opt.set_value(edges)
        return ret_opt

    def search(self, index_val):
        """
        Search by index using for the value.

        :param val: The value to search on
        :type val: object
        :return: The first node found
        :rtype: Edge()
        """
        ret_opt = Option()
        if index_val in self.__edge_map.keys():
            edge = self.__edge_map[index_val]
            ret_opt.set_value(edge)
        return ret_opt
