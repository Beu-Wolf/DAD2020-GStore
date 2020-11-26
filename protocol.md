# Algorithm Protocol (data structures, methods, etc)

## Data definitions
```
<obj_id>: <partition_id,key>
<obj_version>: <counter,client_id>
<prop_msg_id>: <author_replica_id,counter> // just used in RB messages
<prop_message>: <prop_msg_id, partition_id, <obj_id>, <obj_version>, "value">
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
database: {
 <obj_id>: { "value", <obj_version> }
}
belonging_partitions: [ // partition this replica belongs to
 partition_id, ...
]
partitions: { // network partitions (get to know its partition mates)
 partition_id: [ replica_id ]
}
retransmission_buffer: {
 sender_replica_id: {             // messages for every sender
  partition_id: [ <prop_msg_id> ] // WARNING: must be a set: no duplicates!
 }                                // WARNING: When removing a message from retransmission_buffer 
                                  // (ack'd by everyone) we need to 
                                  // remove it from every replica_id
}
received_prop_msgs: {             // WARNING: may have prop_msg_id with no <prop_msg>
 <prop_msg_id>:{                  // if this is the case we won't check retransmission_buffer
  message: <propagation_message>  // WARNING: when adding a new <prop_msg> a <prop_msg_id> may already exist
  acked_replicas: [ replica_id ]  // WARNING: must be a set: no duplicates!
 }                                // WARNING: We need to add the msg sender and ourseves to the ACK list
}

	gRPC
// WARNING: CHECK IF WE WERE THE LAST REPLICA RECEIVING A <prop_msg>.
//   In that case we don't need to add to retransmission_buffer
propagate_write(sender_replica_id, <prop_msg>) { returns OK; }
heartbeat() { returns OK; }
report_crash(dead_replica_id) { return OK; } // report dead body among us
ack_prop_msg(<prop_msg_id>) { return OK; }

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
 write in database
 
 <prop_message>:= <prop_msg_id, partition_id, <obj_id>, <obj_version>, "value">
 broadcast_message(<prop_message>) (parallel)
 when first ack:
  return to client
}

when broadcast_message(<prop_message>) { // maybe receives a lock variable
 for every replica in partitions[<prop_message>.partition_id]:
  propagate_write(my_id, <prop_message>);
  // after first ack, unlock caller
}

when replica => propagate_write(sender_replica_id, <prop_msg>) {
 write in database() // only if received version is greater than the stored one
 
 retransmission_buffer[sender_replica_id][<prop_msg>.partition_id].add(<prop_msg>.id)
 if(<prop_msg>.id not in received_prop_msgs):
  // we never saw this message. Adding message and acks
  received_prop_msgs[<prop_msg>.id] = { message: <prop_msg>, acked_replicas: [ my_id, sender_replica_id ]
  broadcast_acks(<prop_msg>.partition_id, <prop_msg>.prop_msg_id)
 else:
  // we already knew about this message_id
  received_prop_msgs[<prop_msg>.id].acked_replicas.add(my_id, sender_replica_id)
  if received_prop_msgs[<prop_msg>.id].message == null:
   // we just received acks
   received_prop_msgs[<prop_msg>.id].message = <prop_msg>
   broadcast_acks(<prop_msg>.partition_id, <prop_msg>.prop_msg_id)
  else:
   // we have already received this message before
   // therefore we have already broadcast_acks()
   received_prop_msgs[<prop_msg>.id].acked_replicas.add(sender_replica_id 
 
 if received_prop_msgs[<prop_msg>.id].acked_replicas.length == partitions[<prop_msg>.partition_id].length:
  // this message was ack'd by every other replica in partition <prop_msg>.partition_id
  // we can remove this message from the retransmission buffer
  remove_from_retransmission_buffer(<prop_msg>.id)
 
 // TODO: horario de duvidas
 // if sender_replica_id is already crashed, we may be the only ones with this message
 // if that's the case, then the crashed replica didn't return to the client.
 //    So the client will retry to write in another replica
 // otherwise, another replica has already received that message and it will retransmit it
 //    when it knows that the first replica has crashed
 // therefore we don't need to retransmit this message if the sender_replica_id has crashed
}

when broadcast_acks(partition_id, prop_msg_id) {
 TODO
}

when crash() {
 // TODO: remove from ack'd replicas

when remove_from_retransmission_buffer(msg_id) {
 for every replica in retransmission_buffer:
  for every partition in replica:
   partition.remove(msg_id) // if doesn't exist, do nothing
 
 
}

```
