﻿using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Core.Utils;
using System.Linq;

namespace Server
{

    public class ServerSyncService : ServerSyncGrpcService.ServerSyncGrpcServiceBase
    {
        // Dict with all values
        private readonly Dictionary<ObjectKey, string> KeyValuePairs;

        private readonly Dictionary<long, List<string>> ServersByPartition;
        private readonly List<long> MasteredPartitions;

        public ServerSyncService(Dictionary<ObjectKey, string> keyValuePairs, Dictionary<long, List<string>> serversByPartitions, List<long> masteredPartitions)
        {
            KeyValuePairs = keyValuePairs;
            ServersByPartition = serversByPartitions;
            MasteredPartitions = masteredPartitions;
        }


        public override Task<LockObjectReply> LockObject(LockObjectRequest request, ServerCallContext context)
        {
            return Task.FromResult(Lock(request));
        }

        public LockObjectReply Lock(LockObjectRequest request)
        {
            Console.WriteLine("Received LockObjectRequest with params:");
            Console.Write($"Key: \r\n PartitionId: {request.Key.PartitionId} \r\n ObjectId: {request.Key.ObjectId}\r\n");
            return new LockObjectReply
            {
                Success = false
            };
        }

        public override Task<ReleaseObjectLockReply> ReleaseObjectLock(ReleaseObjectLockRequest request, ServerCallContext context)
        {
            return Task.FromResult(ReleaseObject(request));
        }

        public ReleaseObjectLockReply ReleaseObject(ReleaseObjectLockRequest request)
        {
            Console.WriteLine("Received ReleaseObjectLockRequest with params:");
            Console.Write($"Key: \r\n PartitionId: {request.Key.PartitionId} \r\n ObjectId: {request.Key.ObjectId}\r\n");
            Console.WriteLine("Value: " + request.Value);

            return new ReleaseObjectLockReply
            {
                Success = false
            };
        }


    }
}
