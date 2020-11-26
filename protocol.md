# Algorithm Protocol (data structures, methods, etc)

## Data definitions
```
<obj_id>: <partition_id,key>
<obj_version>: <counter,client_id>
<prop_msg_id>: <author_replica_id,counter> // just used in RB messages
<prop_msg>: <prop_msg_id, partition_id, <obj_id>, <obj_version>, "value">
```

## Clients
```
client_id: id sent from puppet master || UUID
partitions: {
 partition_id: [
  server_id, ...
 ]
}
servers: {
 server_id: {
  URL
 }
}
cache: {
 <obj_id>: { "value", <obj_version>, last_access_time }
}
// arbitrary limit: 1024 items
// when removing, sort by timestamp adn remove oldest

write( ... );
```


## Replicas
```
replica_id
wated_replicas: {
 partition_id: watched_replica_id
}
database: {
 <obj_id>: { "value", <obj_version> }
}
belonging_partitions: [ // partition this replica belongs to
 partition_id, ...
]
partitions: { // network partitions (get to know its partition mates)
 partition_id: [ replica_id ]
}
retransmission_buffer: {          // WARNING: When removing a message from retransmission_buffer
 sender_replica_id: {             // (ack'd by everyone) we need to 
  partition_id: {                 // remove it from every replica_id
   <obj_id>: <prop_msg>
  }
 }                       
}

	gRPC
propagate_write(sender_replica_id, <prop_msg>) { returns OK; }
heartbeat() { returns OK; }
report_crash(dead_replica_id) { return OK; } // report dead body among us

	methods
when life => init() {
 build retransmission_buffer (empty list for every replica_id for every partition)
 ...
}
when client => write(<obj_id>, "value", <obj_version>) {
 get new object version
  // max(obj_version.counter, database[<obj_id>][obj_version].counter) + 1
  // 1 vs 2 => 3
  // 2 vs 2 => 3
  // 2 vs 1 => 3
 create new <obj_version>
 write_database()
 
 // remove retransmission_messages related to this object. We have a new one that was sent to everyone
 for replica in retransmission_buffers:
  for partition in replica:
   if partition[<obj_id>]:
    remove partition[<obj_id>]
 
 <prop_message>:= <prop_msg_id, partition_id, <obj_id>, <obj_version>, "value">
 broadcast_message(<prop_message>) (parallel)
 when first gRPC OK:
  return to client new <obj_version>
}

function broadcast_message(<prop_message>) { // maybe receives a lock variable
 for every replica in partitions[<prop_message>.partition_id]:
  propagate_write(my_id, <prop_message>);
  // after first ack, unlock caller
}

when replica => propagate_write(sender_replica_id, <prop_msg>) {
 if not write_database():
  return; // the object was older than ours
 
 // remove old messages referring the same object
 // we don't need to retransmit them anymore
 for replica in retransmission_buffers:
  if replica[<prop_msg>.partition_id][<prop_msg>.<obj_id>]:
   remove replica[<prop_msg>.partition_id][<prop_msg>.<obj_id>]
 
 // add to retransmission buffers
 retransmission_buffers[sender_replica_id][<prop_msg>.partition_id][<prop_msg>.<obj_id>] = <prop_msg>
 
 // TODO: horario de duvidas
 // if sender_replica_id is already crashed, we may be the only ones with this message
 // if that's the case, then the crashed replica didn't return to the client.
 //    So the client will retry to write in another replica
 // otherwise, another replica has already received that message and it will retransmit it
 //    when it knows that the first replica has crashed. That replica won't crash before sending
 //    that message (model assumption, given in the project statement)
 // therefore we don't need to retransmit this message if the sender_replica has crashed
}

when replica => crash(crashed_replica_id) {
 // remove replica from correct replicas
 for partition in partitions:
  if replica_id in partition:
   partition.remove(replica_id)
 
 // broadcast buffered replica messages
 for partition in retransmission_buffers[crashed_replica_id]:
  for msg in partition:
   broadcast_message(<prop_msg>)

 // remove buffered replica messages
 retransmission_buffers.remove(crashed_replica_id)
 
 // update watched_replicas
 for partition in watched_replicas:
  if watched_replicas[partition] == crashed_replica_id:
   watched_replicas[partition] = partitions[partition][(partitions[partition].indexOf(my_id) + 1)%len(partitions[partition])]
}
```
