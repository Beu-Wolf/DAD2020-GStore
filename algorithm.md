# Algorithm description

In order to spread updates across every replica, we've decided to use a Reliable Broadcast adaptation to this problem. The protocol is as follows:

 * When a replica receives an update from a client, then it sends it to every other replica (in the same partition). When it gets its first ACK, it returns to the client (because now it is sure that every replica will eventually receive the message).
 * When a replica `i` receives a broadcast from other replica `j`, it stores its content and sends an ACK to every replica (in the same partition). It also stores that it received this message from `j` in a `retransmission_buffer` (`retbuf`). If at some point `i` knows that `j` has failed, then `i` retransmits every message it received from `j` stored in this `retbuf`. (In this case, the other replicas will store this retransmission as a message received from `i` and do the same if they know `i` failed.)
 * When a replica `i` receives an ACK relative to a message `m` from every replica that has not crashed, then `i` may remove the entries relative to `m` from its `retbuf`, since every replica received `m` and it won't need to be retransmitted.
 * Everytime a replica `i` tries to contact another replica `j` and it fails, it broadcasts (to every other replica or to every replica in the same partition?) that `j` has crashed.
    * In order to detect these failures quickly, every 5 seconds, each replica `k` pings the "next" replica alive, which is the replica `l` with the smallest `id` that is higher than `l.id`. If there is no such replica, it restarts from the lowest `id`. (Circular buffer scheme)
