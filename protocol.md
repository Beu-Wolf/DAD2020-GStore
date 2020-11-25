# Algorithm Protocol (data structures, methods, etc)

## Data definitions
```
<obj_id>: <partition_id,key>
<obj_version>: <counter,client_id>
<prop_msg_id>: <sender_replica_id,counter> // just used in RB messages
<propagation_message>: <prop_msg_id, partition_id, <obj_id>, <obj_version>, "value">
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
 <obj_id>: { "value", <obj_version> }
}
```


## Replicas
```
replica_id
belonging_partitions: [ // partition this replica belongs to
 partition_id, ...
]
partitions: { // network partitions (get to know its partition mates)
 partition_id: [ replica_id ]
}
database: {
 <obj_id>: { "value", <obj_version> }
}
retransmission_buffer: {
 replica_id: { // messages for every sender
  partition_id: [ <prop_msg_id> ]
 }
}

received_prop_msgs: {   // WARNING: may have prop_msg_id with no <prop_msg>
 <prop_msg_id>:{        // if this is the case we won't check retransmission_buffer
  <propagation_message> // WARNING: when adding a new <prop_msg> a <prop_msg_id> may already exist
  acked_replicas: [ replica_id ] // WARNING: we must not have duplicates in this list
 }
}

	gRPC
// WARNING: CHECK IF WE WERE THE LAST REPLICA RECEIVING A <prop_msg>.
//   In that case we don't need to add to retransmission_buffer
propagate_write(sender_replica_id, <prop_msg>) { returns OK; }
heartbeat() { returns OK; }
report_crash(dead_replica_id) { return OK; } // report dead body among us
ack_prop_msg(<prop_msg_id>) { return OK; }
```
