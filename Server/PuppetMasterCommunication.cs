﻿using Grpc.Core;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Server
{
    public class PuppetMasterServerService : PuppetMasterServerGrpcService.PuppetMasterServerGrpcServiceBase
    {
        public int MyPort { get; set; }
        public string MyHost { get; set; }


        private readonly ConcurrentDictionary<string, List<string>> ServersByPartition;
        private readonly ConcurrentDictionary<string, string> ServerUrls;
        private readonly ConcurrentBag<string> CrashedServers;
        private readonly List<string> MasteredPartitions;

        private readonly DelayMessagesInterceptor Interceptor;

        public PuppetMasterServerService(ConcurrentDictionary<string, List<string>> serversByPartitions, ConcurrentDictionary<string, string> serverUrls,
            List<string> masteredPartitions, ConcurrentBag<string> crashedServers, DelayMessagesInterceptor interceptor)
        {
            ServersByPartition = serversByPartitions;
            ServerUrls = serverUrls;
            MasteredPartitions = masteredPartitions;
            CrashedServers = crashedServers;
            Interceptor = interceptor;
        }

        public override Task<CrashReply> Crash(CrashRequest request, ServerCallContext context)
        {
            Environment.Exit(1);
            return Task.FromResult(new CrashReply
            {
                Success = false
            }); 
        }

        public override Task<FreezeReply> Freeze(FreezeRequest request, ServerCallContext context)
        {
            return Task.FromResult(FreezeServer());
        }

        public FreezeReply FreezeServer()
        {
            Interceptor.FreezeCommands = true;
            return new FreezeReply
            {
                Success = true
            };
        }

        public override Task<UnfreezeReply> Unfreeze(UnfreezeRequest request, ServerCallContext context)
        {
            return Task.FromResult(UnfreezeServer());
        }

        public UnfreezeReply UnfreezeServer()
        {
            Interceptor.FreezeCommands = false;
            return new UnfreezeReply
            {
                Success = true
            };
        }


        public override Task<ServerStatusReply> Status(ServerStatusRequest request, ServerCallContext context)
        {
            return Task.FromResult(PrintStatus());
        }

        public ServerStatusReply PrintStatus()
        {
            Console.WriteLine("Online Servers");
            foreach (var server in ServersByPartition)
            {
                Console.Write("Servers ");
                server.Value.ForEach(x => Console.Write(x + " "));
                Console.Write($"from partition {server.Key}\r\n");
            }
            Console.WriteLine("Crashed Servers");
            foreach (var server in CrashedServers)
            {
                Console.WriteLine($"Server {server}");
            }
            return new ServerStatusReply
            {
                Success = true
            };
        }

        public override Task<ServerPingReply> Ping(ServerPingRequest request, ServerCallContext context)
        {
            return Task.FromResult(new ServerPingReply());
        }

        public override Task<PartitionSchemaReply> PartitionSchema(PartitionSchemaRequest request, ServerCallContext context)
        {
            return Task.FromResult(PartitionSchemaMe(request));
        }

        public PartitionSchemaReply PartitionSchemaMe(PartitionSchemaRequest request)
        {
            foreach (var partitionDetails in request.PartitionServers)
            {
                if(!ServersByPartition.TryAdd(partitionDetails.Key, partitionDetails.Value.ServerIds.ToList()))
                {
                    throw new RpcException(new Status(StatusCode.Unknown, "Could not add element"));
                }
            }

            foreach (var serverUrl in request.ServerUrls)
            {
                if(!ServerUrls.TryAdd(serverUrl.Key, serverUrl.Value))
                {
                    throw new RpcException(new Status(StatusCode.Unknown, "Could not add element"));
                }
            }

            foreach (var masteredPartition in request.MasteredPartitions.PartitionIds)
            {
                MasteredPartitions.Add(masteredPartition);
            }

            return new PartitionSchemaReply();
        }
    }
}
