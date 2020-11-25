# Advanced Algorithm
In this document 

## Algorithm description
In order to spread updates across every replica, we've decided to use a Reliable Broadcast adaptation to this problem. The protocol is as follows:

 * When a replica receives an update from a client, then it sends it to every other replica (in the same partition). When it gets its first ACK, it returns to the client (because now it is sure that every replica will eventually receive the message).
 * When a replica `i` receives a broadcast from other replica `j`, it stores its content and sends an ACK to every replica (in the same partition). It also stores that it received this message from `j` in a `retransmission_buffer` (`retbuf`). If at some point `i` knows that `j` has failed, then `i` retransmits every message it received from `j` stored in this `retbuf`. (In this case, the other replicas will store this retransmission as a message received from `i` and do the same if they know `i` failed.)
 * When a replica `i` receives an ACK relative to a message `m` from every replica that has not crashed, then `i` may remove the entries relative to `m` from its `retbuf`, since every replica received `m` and it won't need to be retransmitted.
 * Everytime a replica `i` tries to contact another replica `j` and it fails, it broadcasts (to every other replica or to every replica in the same partition?) that `j` has crashed.
    * In order to detect these failures quickly, every 5 seconds, each replica `k` pings the "next" replica alive, which is the replica `l` with the smallest `id` that is higher than `l.id`. If there is no such replica, it restarts from the lowest `id`. (Circular buffer scheme)


## Object versions
 * Every client and replica keeps a version associated with each object. This version is as follows: `<counter, clientID>`, where `counter` is the version number and is incremented by every write and `clientID` is the ID of the last client that wrote it.
 * When a client reads an object, the serving replica retrieves the stored object and its version.
If the client's version is more recent that the retrieved one, it may accept the read, discard it or ask another replica for that object. In this client implementation, the client uses its cached value.
 * When a client writes to a replica, it sends the object's version. The replica then compares it with its stored version. The new object version corresponds to `max(replica_version, client_version) + 1`. The client gets the new object version.
 * When a replica `i` receives a write from another replica `j` with the same `counter` as the stored version, it keeps the version with the higher `clientID`



## Report Notes
 * This algorithm is correctif time between crashes in one partitoin is higher than the time needed for that partition to stabilize. This is the time needed to detect the crash and to send pending messages to one of the remaining replicas.
Eg. If a client `1` sends a write to replica `A` and it crashes after sending it to replica `B`, the time needed to stabilize is the time that `B` takes to notice that `A` crashed and send it to some other replica. If `B` crashes after sending it to one replica, it is guaranteed that that replica will have enough time to notice that `B` crashed and send the pending message to another replica. And so on until every replica has crashed.
 * The system is stable if, for every partition, every message is replicated at least twice
