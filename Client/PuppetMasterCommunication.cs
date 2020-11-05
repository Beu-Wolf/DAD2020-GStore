using Grpc.Core;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Client
{
    class PuppetMasterCommunicationService : PuppetMasterClientGrpcService.PuppetMasterClientGrpcServiceBase
    {
        private readonly ConcurrentDictionary<string, List<string>> ServersIdByPartition;
        private readonly ConcurrentDictionary<string, string> ServerUrls;
        private readonly ConcurrentBag<string> CrashedServers;
        BoolWrapper ContinueExecution;

        public PuppetMasterCommunicationService(ConcurrentDictionary<string, List<string>> serversIdByPartition, ConcurrentDictionary<string, string> serverUrls, 
            ConcurrentBag<string> crashedServers, BoolWrapper continueExecution)
        {
            ServersIdByPartition = serversIdByPartition;
            ServerUrls = serverUrls;
            CrashedServers = crashedServers;
            ContinueExecution = continueExecution;
        }

        public override Task<NetworkInformationReply> NetworkInformation(NetworkInformationRequest request, ServerCallContext context)
        {
            return Task.FromResult(NetworkInfo(request));
        }

        public NetworkInformationReply NetworkInfo(NetworkInformationRequest request)
        {
            Console.WriteLine("Received NetworkInfo");
            foreach (var serverUrl in request.ServerUrls)
            {
                if(!ServerUrls.ContainsKey(serverUrl.Key))
                {
                    ServerUrls[serverUrl.Key] = serverUrl.Value;
                }
            }
            foreach (var partition in request.ServerIdsByPartition)
            {
                if(!ServersIdByPartition.ContainsKey(partition.Key))
                {
                    if(!ServersIdByPartition.TryAdd(partition.Key, partition.Value.ServerIds.ToList()))
                    {
                        throw new RpcException(new Status(StatusCode.Unknown, "Could not add element"));
                    }
                }
            }
            lock(ContinueExecution.WaitForInformationLock)
            {
                ContinueExecution.Value = true;
                Monitor.PulseAll(ContinueExecution.WaitForInformationLock);
            }
            return new NetworkInformationReply();
        }


        public override Task<ClientStatusReply> Status(ClientStatusRequest request, ServerCallContext context)
        {
            return Task.FromResult(ClientStatus());
        }

        public ClientStatusReply ClientStatus()
        {
            Console.WriteLine("Online Servers:");
            foreach (var server in ServersIdByPartition)
            {
                Console.Write("Server ");
                server.Value.ForEach(x => Console.Write(x + " "));
                Console.Write("from partition " + server.Key + "\r\n");
            }
            Console.WriteLine("Crashed Servers");
            foreach (var server in CrashedServers)
            {
                Console.WriteLine($"Server {server}");
            }
            return new ClientStatusReply();
        }

        public override Task<ClientPingReply> Ping(ClientPingRequest request, ServerCallContext context)
        {
            return Task.FromResult(new ClientPingReply());
        }

    }
}
