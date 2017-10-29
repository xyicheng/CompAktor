'''
Created on Oct 19, 2017

@author: simplrdev
'''

import asyncio
from compaktor.streams.objects.node_pub_sub import NodePubSub
from compaktor.streams.objects.source import Source
from compaktor.streams.objects.sink import Sink


class StringSource(Source):

    def __init__(self, src_num=0, loop=asyncio.get_event_loop()):
        super().__init__("StringSource",loop=loop)
        self.__iteration = 0
        self.src_num = src_num

    async def on_pull(self, payload):
        self.__iteration += 1
        return "TestPull {} {}".format(
            self.src_num, self.__iteration)

class LargeStringSource(Source):

    def __init__(self, src_num=1, loop=asyncio.get_event_loop()):
        super().__init__("LargeStringSource", loop=loop)
        self.__iteration = 0
        self.src_num = src_num
        self.words = """                
        Lorem ipsum dolor sit amet, consectetur adipiscing elit. Ut eu dictum enim. Etiam et odio rhoncus leo cursus pharetra. Phasellus pretium lacinia placerat. Nullam ut egestas turpis, eget elementum lacus. Duis eleifend magna ut elit viverra, in facilisis urna finibus. Pellentesque vestibulum ex non convallis feugiat. Sed sit amet dignissim magna, id vulputate sapien. Aliquam erat volutpat. Ut blandit eu arcu et venenatis. Mauris quis neque sapien. Duis eget purus ut massa varius volutpat. Mauris maximus mi ac nibh elementum iaculis. Ut congue iaculis lacinia. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Quisque a aliquam erat.
        Nullam vel placerat massa. Proin sodales risus scelerisque dui tempor sagittis. Sed efficitur augue vel felis rutrum, quis tempus nunc consequat. Vestibulum aliquet consequat massa vel lacinia. Morbi lacinia a tellus ut aliquam. Mauris accumsan dapibus est sed euismod. Maecenas ac suscipit lectus, nec viverra dui.        
        Orci varius natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. Aliquam erat volutpat. Phasellus ac lobortis massa. Curabitur ultrices scelerisque magna at posuere. Nulla non felis placerat, mattis ligula nec, pulvinar eros. Nulla luctus diam in ante hendrerit maximus. Maecenas lobortis neque nunc. Interdum et malesuada fames ac ante ipsum primis in faucibus. Vivamus egestas felis et ullamcorper pellentesque. Vivamus et metus blandit, accumsan arcu at, vulputate sapien. Vivamus sed sapien ut dui bibendum blandit non sollicitudin augue. Mauris pharetra massa ac ex bibendum, quis congue ante dapibus. Donec quis est non ante blandit efficitur ut in erat. Praesent scelerisque ipsum quis odio fermentum maximus.        
        Aliquam imperdiet sit amet metus sit amet euismod. Fusce tincidunt ante sed dolor pulvinar tincidunt. Cras vel aliquam tortor. Vestibulum dignissim nisl nisl, non posuere turpis tincidunt eget. Aliquam a erat tempus, cursus purus ut, tristique lacus. Nullam nisl ante, accumsan ac justo suscipit, euismod ornare massa. Aliquam varius enim lorem, eget bibendum justo fermentum sit amet. Maecenas fringilla mollis lectus vitae malesuada. Fusce sollicitudin ultrices justo, vel venenatis sapien. Etiam vitae lectus consequat, ultrices dui ac, fringilla enim.        
        Fusce suscipit rutrum nisl ac sollicitudin. Curabitur mollis ultrices tincidunt. Ut aliquet elit et lorem lacinia, vitae egestas turpis accumsan. Vestibulum commodo velit vitae maximus posuere. Duis elementum sem non massa pharetra, sit amet porta est dapibus. Donec accumsan ac nibh at convallis. Vestibulum hendrerit justo nec purus sagittis rutrum.
        Sed laoreet vitae ligula id imperdiet. Duis sed purus sed augue scelerisque volutpat ac non leo. Integer feugiat velit in augue pulvinar, nec sodales est convallis. In aliquet commodo orci vel ullamcorper. Praesent feugiat volutpat faucibus. Proin eleifend consequat massa, eget pulvinar neque. Maecenas nec egestas nibh, eu facilisis quam. Quisque id mi ipsum. Curabitur efficitur viverra nulla, ac aliquam ante luctus vel. Fusce vehicula nulla et tempor elementum. Morbi auctor fringilla urna, ac volutpat sem lobortis et. Curabitur et euismod tellus, non molestie dolor. Curabitur ac ultricies augue. Donec molestie purus id leo sodales finibus ut sit amet turpis. Vivamus eleifend auctor. 
        """

    async def on_pull(self, payload):
        print("Large Pull")
        self.__iteration -= 1
        return "TestPull {} {} {}".format(
            self.src_num, self.__iteration, self.words)

class SplitNode(NodePubSub):

    def __init__(self, name="SplitNode", loop=asyncio.get_event_loop()):
        super().__init__(name, loop=loop)

    async def on_pull(self, message):
        print("Splitting {}".format(type(message)))
        return message.split(" ")

class CountNode(NodePubSub):

    def __init__(self, name="CountNode", loop=asyncio.get_event_loop()):
        super().__init__(name, loop=loop)

    async def on_pull(self, message):
        print("Counting {}".format(type(message)))
        return len(message)

class PrintSink(Sink):

    def __init__(self, name="PrintSink",loop=asyncio.get_event_loop()):
        super().__init__(name=name, loop=loop)

    async def on_push(self, payload):
        print(payload)
