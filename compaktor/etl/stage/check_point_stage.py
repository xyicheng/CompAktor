'''
Save the input to a file
Created on Oct 10, 2017

@author: aevans
'''

import asyncio
import datetime
import os
import random
import pickle
import json
from compaktor.streams.objects.node_pub_sub import NodePubSub

class CheckpointStage(NodePubSub):

    def __init__(self, name, fdir, ftype="txt", dict_to_json=False, providers,
                 individual_file=False,loop=asyncio.get_event_loop(),
                 address=None, mailbox_size=1000, inbox=None,
                 empty_demand_logic = "broadcast"):
        super().__init__(name, providers, loop, address, mailbox_size, inbox,
                         empty_demand_logic)
        self.__fdir = fdir
        if self.__fdir is None or os.path.exists(fdir) is False:
            raise ValueError("Checkpoint Directory Does Not Exist")
        self.__fparts = self.__fdir.split(os.path.sep)
        self.__ftype = ftype
        self.__dict_to_json = dict_to_json
        self.__fpath = None
        self.__individual_file = individual_file

    def on_pull(self, message):
        try:
            payload = message.payload
            fpath = self.__fpath
            if self.__individual_file or self.__ftype == "pickle":
                r_part = random.randint()
                dtime = datetime.datetime().strftime("%H%M%S%f")
                uparts = [x for x in self.__fparts]
                fname = "{}_{}.{}".format(dtime, r_part, self.__ftype)
                fpath = os.path.sep.join(uparts)

            if self.__ftype == "pickle":
                with open(fpath, 'w') as fp:
                    pickle._dump(payload, fpath)
            else:
                if isinstance(payload, dict) and self.dict_to_json:
                    payload = json.dumps(payload)
                fopen = None
                try:
                    if self.__individual_file:
                        fopen = open(self.__fpath, 'w')
                    else:
                        fopen = open(self.__fpath, 'a')
                    fopen.write(str(payload))
                except Exception as e:
                    if fopen:
                        fopen.close()
        except Exception as e:
            self.handle_fail()
        return payload
