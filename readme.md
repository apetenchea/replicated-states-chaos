Replicated States Stress Testing for ArangoDB
============================================

In order to run stress testing on the prototype replicated state,
start a cluster with at least 12 DB servers and 3 coordinators (recommended).  

Run `chaos.py` and follow the logs in the console.
The files `actual.json` and `expected.json` represent the actual snapshot gathered form the coordinator and the expected final
state which was reconstructed.  

There can be a lot of output produce, hence you might want to redirect everything to a file: `python chaos.py &> log.txt`