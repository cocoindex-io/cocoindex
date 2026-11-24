# Getting started

CocoIndex turns a folder of files into derived outputs and keeps them in sync.
You write a plain Python function that declares what should exist (a file, a
row), and the engine works out what changed since the last run and recomputes
only that. The first run processes everything; later runs are proportional to
the edit, not to the folder.

Install with `pip install cocoindex`, write an app whose main function mounts a
source and a target, and run `cocoindex update` from the project directory.
