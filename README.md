# CompAktor

CompAktor is an attempt to create a full fledged actor system akin to Akka in Python. The actor system is perfect for Python projects. The possibilities are endless here. Interoperability with C code, streams, and more can be added to the project. However, this project will start small. It is a thought experiment at the moment written entirely in Python.

As I am just starting my Python 3.x and asyncio journey, the base actors are from the Cleveland project (https://github.com/biesnecker/cleveland). They have been updated to Python 3.5+ and changed to reflect be more Akka-like.

# Achieving Concurrency

Asyncio allows a running task to yield time to other tasks. However, it is not truly parallelism. For this reason, CompAktor's plumbing is mainly for messaging and really small tasks. It is best to combine CompAktor processes and other threads (not really the latter) to achieve multi-core concurrency. However, the tool achieves work sharing on a single core which may be beneficial for other reasons. It is recommended to use the Flows library as threads can be used per event loop and boundaries established. Again, due to the GIL this may still not be ideal. 

Aioprocessing will be examined and implemented to help alleviate some issues as well.

# Use Cases

Microservices, large tasks that can be run well in a subprocess and other functions that block will perform well at the moment. This may violate some of the basic actor principals (one function per actor) but works better in this case.

Tasks that work well include:
 - micro-sevices architectures with large blocking calls such as calls to a Spark process
 - programs requiring large amounts of network calls
 - non-blocking streaming of data from devices

Perhaps the GIL will disappear in the future and we can implement 1 event loop per actor. Until then, these are the best use cases for this project. Remember GIL sucks. He'll promise concurrency but its a bit of an illusion.

# Initial Features

Initial features will include:

- Single Node Actors
- An actor manager
- Actor States
- Round Robin Router
- Random Router
- Balancing Router
- Flows
- Threadsafe async boundaries inside flows (may be slower than we want)
- Multi-processing and thread pools for actor tasks (that way we have non-blocking concurrent tasks)
- Many of the features available in the original actor components in Akka

Our goal at the moment is to surpass Akka 1.1-1.3 with modern features such as a set of router types. 
At the moment this will occur when failure handling, health checking, and dead letter handling are in
place. The goal after this is to create streams and then move to remoting and clustering. In the 
meantime, maybe we could pressure Lightbend to create a Python system. 

We are currently implementing:

- Health Checking (in test)
- Dead Letter Handling (starting dev)
- Streams (implementing in the use cases section)
- Failure Handler Strategies (one for one replacement and removal with the latter already happening) 

I would say the biggest to dos at the moment are replacing the janus queue in the balancing routher,failure handling, health checking, and dead letter handling in that order.
Streams will develop through the use cases. 

Our test cases and use cases are available to help deepen an und

Feature Wish List (next round of stuff from the backlog):

- Remote Actors
- Stream Sources and Sinks
- Graph Stages
- Clustered Actors
- More advanced routing techniques

The goal is basically a Python port of Akka which I think is a superior actor system and highyl effective for the 
many use cases of the system.

# License

This code is mainly going to be custom but the base actors are from the Cleveland project (https://github.com/biesnecker/cleveland).


Copyright 2017 - present Andrew Evans with heavy help from Biesnecker and Saaj

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
