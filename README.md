# CompAktor

CompAktor is an attempt to create a full fledged actor system akin to Akka in Python. The actor system is perfect for Python projects. The possibilities are endless here. Interoperability with C code, streams, and more can be added to the project. However, this project will start small. It is a thought experiment at the moment written entirely in Python.

As I am just starting my Python 3.x and asyncio journey, the base actors are from the Cleveland project (https://github.com/biesnecker/cleveland). They have been updated to Python 3.5+ and changed to reflect be more Akka-like.

Initial features will include:

- Single Node Actors
- Remote Actors
- An actor manager
- Actor States
- Round Robin Router
- many of the features available in the original actor components in Akka

Feature Wish List (next round of stuff for the backlog):

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