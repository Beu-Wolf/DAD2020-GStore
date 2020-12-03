import random
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
T5: Cenario real
3 particoes, 4 servidores por particao
8 escritores / leitores

2 crashes por replica
'''
partitions = ["A", "B", "C"]
servers_per_partition = 4
n_clients = 8
server_ids = [f"s{part}{id}" for id in range(1, servers_per_partition + 1) for part in partitions]
clients = [f"c0{id}" for id in range(1, n_clients + 1)]
port = 3000

'''
pm_script = f"ReplicationFactor {servers_per_partition}\n"
pm_script += '\n'.join([f"Partition {servers_per_partition} p{part_name} " + (" ".join(f"s{part_name}{sid}" for sid in range(1, servers_per_partition + 1))) for part_name in partitions])
for server in server_ids:
    pm_script += f'\nServer {server} http://localhost:{port} 0 0'
    port += 1

for client in clients:
    c_script_name = f'script_5_{client}'
    pm_script += f'\nClient {client} http://localhost:{port} {c_script_name}'
    port += 1

tbcrash = 5000
for i in range(2, 4):
    for part in partitions:
        pm_script += f"\nCrash s{part}{i}"
        pm_script += f"\nWait {tbcrash}"

pm_script_name = 'pm_5.txt'
# print(pm_script)
with open(pm_script_name, 'w') as f:
    f.write(pm_script)

numObjs = 20
nwpo = 500 # number of operations per partition per object
objs = [f'obj_{id}' for id in range(1, numObjs + 1)]
rw_percentage = 0.90

for c in clients:
    value = 0
    cs = ""
    c_script_name = f'script_5_{c}.txt'
    for i in range(nwpo):
        for j in range(numObjs):
            for p in partitions:
                obj = random.choice(objs)
                part = random.choice(partitions)
                is_read = random.random() < rw_percentage
                if is_read:
                    cs += f"\nread p{part} {obj}"
                else:
                    cs += f"\nwrite p{part} {obj} {c}_{value}"
                    value += 1
    with open(c_script_name, 'w') as f:
        f.write(cs)

# client_scripts = []
# numObjs = 10
# numWritesPartition = 100
# objs = [f'obj_{id}' for id in range(1, numObjs + 1)]


# for client in clients:
#     c_script_name = f'script_1_{client}'
#     c_script = ""

#     pm_script += f'\nClient {client} http://localhost:{port} {c_script_name}'
#     port += 1
#     for write in range(numWritesPartition):
#         for obj_id in objs:
#             for part in partitions:
#                 c_script += f'\nwrite p{part} {obj_id} {client}_{write}'

#     with open(f'{c_script_name}.txt', 'w') as f:
#         f.write(c_script)

'''


'''
T6 - Tempo de write (escritas concorrentes e aleatorias)
2 particoes, 4 servidores por particao
4 clientes, a escrever freneticamente em todos os servidores (intercalados, para trocarem o servidor ao qual estao ligados)
-> verificar tempo de escrita
-> verificar que todas as particoes chegam a um estado coerente
'''

partitions = ["A", "B"]
servers_per_partition = 4
n_clients = 4
server_ids = [f"s{part}{id}" for id in range(1, servers_per_partition + 1) for part in partitions]
clients = [f"c0{id}" for id in range(1, n_clients + 1)]
port = 3000

pm_script = f"ReplicationFactor {servers_per_partition}\n"
pm_script += '\n'.join([f"Partition {servers_per_partition} p{part_name} " + (" ".join(f"s{part_name}{sid}" for sid in range(1, servers_per_partition + 1))) for part_name in partitions])
for server in server_ids:
    pm_script += f'\nServer {server} http://localhost:{port} 0 0'
    port += 1

client_scripts = []
numObjs = 5
numWritesPartition = 70
objs = [f'obj_{id}' for id in range(1, numObjs + 1)]


for client in clients:
    c_script_name = f'script_6_{client}'
    c_script = ""

    pm_script += f'\nClient {client} http://localhost:{port} {c_script_name}'
    port += 1
    for write in range(numWritesPartition):
        for obj_id in objs:
            for part in partitions:
                c_script += f'\nwrite p{part} {obj_id} {client}_{obj_id}_{write}'
    c_script += "\nwait 5000"
    for s in server_ids:
        c_script += f"\nlistServer {s}"
    with open(f'{c_script_name}.txt', 'w') as f:
        f.write(c_script[1:])

pm_script_name = 'pm_6.txt'
with open(pm_script_name, 'w') as f:
    f.write(pm_script)
'''
'''