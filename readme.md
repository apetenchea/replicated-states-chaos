Replicated States Stress Testing for ArangoDB
============================================

**Replicated states HTTP API might have changed**

In order to run stress testing on the prototype replicated state,
start a cluster with at least 12 DB servers and 3 coordinators (recommended).  

Run `chaos.py` and follow the logs in the console.
The files `actual.json` and `expected.json` represent the actual snapshot gathered form the coordinator and the expected final
state which was reconstructed.  

There can be a lot of output produced, hence you might want to redirect everything to a file: `python chaos.py &> log.txt`  

How I generally do it:
```
./scripts/startLocalCluster -a 1 -c 3 -d 12
python chaos.py -op 5000 &> log.txt
```
Then follow the logs in another terminal:
```
tail -f log.txt
```
When you want to re-run the whole thing, don't forget to shutdown the previous cluster.  
For debugging purposes, the contents of the replicated log are dumped into `replicated-log.json`.
