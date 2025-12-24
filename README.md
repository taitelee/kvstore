# kvstore
A distributed, sharded key–value store in Go. I'm going to try an implement consistent hashing, replication, rebalancing, and a WAL. Each node is going to run as an independent storage server and cooperatively forms a scalable, fault tolerant cluster capable of automatic request routing and data redistribution.

This is part of the unemployed series.

EDIT: I'm actually employed now, but this project is still in progress.


## TODO:
* Docker setup
