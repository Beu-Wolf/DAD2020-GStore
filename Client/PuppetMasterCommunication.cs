﻿using Grpc.Core;
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
            foreach (var serverUrl in request.ServerUrls)
            {
                if(!ServerUrls.TryAdd(serverUrl.Key, serverUrl.Value)) 
                {
                    throw new RpcException(new Status(StatusCode.Unknown, "Could not add element"));
                }
            }
            foreach (var partition in request.ServerIdsByPartition)
            {
                if(!ServersIdByPartition.TryAdd(partition.Key, partition.Value.ServerIds.ToList()))
                {
                    throw new RpcException(new Status(StatusCode.Unknown, "Could not add element"));
                }
            }
            lock(ContinueExecution.WaitForInformationLock)
            {
                ContinueExecution.Value = true;
                Monitor.PulseAll(ContinueExecution.WaitForInformationLock);
            }
            return new NetworkInformationReply();
        }


        public override Task<StatusReply> Status(StatusRequest request, ServerCallContext context)
        {
            return Task.FromResult(ClientStatus());
        }

        public StatusReply ClientStatus()
        {
            Console.WriteLine("Online Servers");
            foreach (var server in ServersIdByPartition)
            {
                Console.WriteLine($"Server {server.Value} from partition {server.Key}");
            }
            Console.WriteLine("Crashed Servers");
            foreach (var server in CrashedServers)
            {
                Console.WriteLine($"Server {server}");
            }
            return new StatusReply();
        }

    }
}