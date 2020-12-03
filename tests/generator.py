'''
T1 - Tempo de write (escritas concorrentes e aleatorias)
2 particoes, 4 servidores por particao
8 clientes, a escrever freneticamente em todos os servidores (intercalados, para trocarem o servidor ao qual estao ligados)
-> verificar tempo de escrita
-> verificar que todas as particoes chegam a um estado coerente


partitions = ["A", "B"]
servers_per_partition = 4
n_clients = 8
server_ids = [f"s{part}{id}" for id in range(1, servers_per_partition + 1) for part in partitions]
clients = [f"c0{id}" for id in range(1, n_clients + 1)]
port = 3000

pm_script = f"ReplicationFactor {servers_per_partition}\n"
pm_script += '\n'.join([f"Partition {servers_per_partition} p{part_name} " + (" ".join(f"s{part_name}{sid}" for sid in range(1, servers_per_partition + 1))) for part_name in partitions])
for server in server_ids:
    pm_script += f'\nServer {server} http://localhost:{port} 0 0'
    port += 1

client_scripts = []
numObjs = 10
numWritesPartition = 100
objs = [f'obj_{id}' for id in range(1, numObjs + 1)]


for client in clients:
    c_script_name = f'script_1_{client}.txt'
    c_script = ""

    pm_script += f'\nClient {client} http://localhost:{port} {c_script_name}'
    port += 1
    for write in range(numWritesPartition):
        for obj_id in objs:
            for part in partitions:
                c_script += f'\nwrite {part} {obj_id} {client}_{write}'

    with open(c_script_name, 'w') as f:
        f.write(c_script)

pm_script_name = 'pm_1.txt'
with open(pm_script_name, 'w') as f:
    f.write(pm_script)
'''


'''
T2 - Varios servidores com grandes delays; um servidor crasha quando estiver a divulgar escritas; o que acontece?
1 particao, 3 servidores com delay entre 5 e 10s
um cliente a escrever muitas vezes na particao
o pm crasha o master da particao
o que acontece?


partitions = ["A"]
servers_per_partition = 3
server_ids = [f"s{part}{id}" for id in range(1, servers_per_partition + 1) for part in partitions]
n_clients = 1
clients = [f"c0{id}" for id in range(1, n_clients + 1)]
port = 3000

pm_script = "ReplicationFactor 4\n"
pm_script += '\n'.join([f"Partition 4 p{part_name} " + (" ".join(f"s{part_name}{sid}" for sid in range(1, servers_per_partition + 1))) for part_name in partitions])
for server in server_ids:
    pm_script += f'\nServer {server} http://localhost:{port} 5000 10000'
    port += 1

client_scripts = []
numObjs = 10
numWritesPartition = 100
objs = [f'obj_{id}' for id in range(1, numObjs + 1)]


for client in clients:
    c_script_name = f'script_2_{client}.txt'
    c_script = ""

    pm_script += f'\nClient {client} http://localhost:{port} {c_script_name}'
    port += 1
    for write in range(numWritesPartition):
        for obj_id in objs:
            for part in partitions:
                c_script += f'\nwrite {part} {obj_id} {client}_{write}'

    with open(c_script_name, 'w') as f:
        f.write(c_script)

pm_script_name = 'pm_2.txt'
with open(pm_script_name, 'w') as f:
    f.write(pm_script)
'''


'''
T3 - Tempo de propagacao de writes
2 particoes, 2 servidores por particao, 0 delay
um cliente por particao a fazer muitas escritas
2 clientes por particao a fazer reads

partitions = ["A", "B"]
servers_per_partition = 2
server_ids = [f"s{part}{id}" for id in range(1, servers_per_partition + 1) for part in partitions]
n_clients = 3
clients = [f"c0{id}" for id in range(1, n_clients + 1)]
port = 3000

pm_script = f"ReplicationFactor {servers_per_partition}\n"
pm_script += '\n'.join([f"Partition {servers_per_partition} p{part_name} " + (" ".join(f"s{part_name}{sid}" for sid in range(1, servers_per_partition + 1))) for part_name in partitions])
for server in server_ids:
    pm_script += f'\nServer {server} http://localhost:{port} 0 0'
    port += 1

numObjs = 2
numWritesPartition = 200
objs = [f'obj_{id}' for id in range(1, numObjs + 1)]


for client in clients:
    c_script_name = f'script_3_{client}.txt'
    pm_script += f'\nClient {client} http://localhost:{port} {c_script_name}'
    port += 1

pm_script_name = 'pm_3.txt'
print(pm_script)
# with open(pm_script_name, 'w') as f:
    #f.write(pm_script)


# client 1 - writer: writes in both partitions every object
cs_1 = ""
cs_1_name = f'script_3_{clients[0]}.txt'
for i in range(numWritesPartition):
    for obj in objs:
        for part in partitions:
            cs_1 += f"\nwrite {part} {obj} c01_{i}"
with open(cs_1_name, 'w') as f:
    f.write(cs_1)

# client 2, 3 - readers
part_idx = 0
for client in clients[1:]:
    cs_name = f'script_3_{client}.txt'
    cs = ""
    for i in range(numWritesPartition):
        for obj in objs:
            for part in partitions:
                cs += f"\nread {part} {obj}"
    with open(cs_name, 'w') as f:
        f.write(cs)


    # with open(c_script_name, 'w') as f:
    #     f.write(c_script)

'''




'''
T4: Provar que o cliente nunca le um valor mais antigo
2 particoes, 3 servidores por particao 0 delay, um deles frozen
1 cliente a fazer escritas
outro cliente a ler de muitos servidores
pm para
leitor passa a ler apenas da frozen replica
pm faz unfreeze da replica frozen
'''
partitions = ["A", "B"]
servers_per_partition = 3
n_clients = 2
port = 3000
server_ids = [f"s{part}{id}" for id in range(1, servers_per_partition + 1) for part in partitions]
clients = [f"c0{id}" for id in range(1, n_clients + 1)]

pm_script = f"ReplicationFactor {servers_per_partition}\n"
pm_script += '\n'.join([f"Partition {servers_per_partition} p{part_name} " + (" ".join(f"s{part_name}{sid}" for sid in range(1, servers_per_partition + 1))) for part_name in partitions])
for server in server_ids:
    pm_script += f'\nServer {server} http://localhost:{port} 0 0'
    port += 1

for client in clients:
    c_script_name = f'script_4_{client}.txt'
    pm_script += f'\nClient {client} http://localhost:{port} {c_script_name}'
    port += 1

pm_script += f'\nFreeze sA1'
pm_script += f'\nFreeze sB1'
