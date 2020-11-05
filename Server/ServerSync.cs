using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Core.Utils;
using System.Linq;
using System.Threading;
using System.Collections.Concurrent;

namespace Server
{

    public class ServerSyncService : ServerSyncGrpcService.ServerSyncGrpcServiceBase
    {
        // Dict with all values
        private readonly ConcurrentDictionary<ObjectKey, ObjectValueManager> KeyValuePairs;


        private readonly ConcurrentDictionary<string, List<string>> ServersByPartition;
        private readonly ConcurrentBag<string> CrashedServers;

        private readonly ReaderWriterLock LocalReadWriteLock;

        public ServerSyncService(ConcurrentDictionary<ObjectKey, ObjectValueManager> keyValuePairs, ConcurrentDictionary<string, List<string>> serversByPartition, ReaderWriterLock readerWriterLock, ConcurrentBag<string> crashedServers)
        {
            KeyValuePairs = keyValuePairs;
            LocalReadWriteLock = readerWriterLock;
            CrashedServers = crashedServers;
            ServersByPartition = serversByPartition;
        }


        public override Task<LockObjectReply> LockObject(LockObjectRequest request, ServerCallContext context)
        {
            return Task.FromResult(Lock(request));
        }

        public LockObjectReply Lock(LockObjectRequest request)
        {
            Console.WriteLine("Received LockObjectRequest with params:");
            Console.Write($"Key: \r\n PartitionId: {request.Key.PartitionId} \r\n ObjectId: {request.Key.ObjectId}\r\n");

            if (!KeyValuePairs.TryGetValue(new ObjectKey(request.Key), out ObjectValueManager objectValueManager))
            {
                LocalReadWriteLock.AcquireWriterLock(-1);
                objectValueManager = new ObjectValueManager();
                KeyValuePairs[new ObjectKey(request.Key)] = objectValueManager;
                objectValueManager.LockWrite();
                LocalReadWriteLock.ReleaseWriterLock();
            }
            else
            {
                objectValueManager.LockWrite();
            }


            return new LockObjectReply
            {
                Success = true
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

            var objectValueManager = KeyValuePairs[new ObjectKey(request.Key)];

            objectValueManager.UnlockWrite(request.Value);

            return new ReleaseObjectLockReply
            {
                Success = true
            };
        }

        public override Task<RemoveCrashedServersReply> RemoveCrashedServers(RemoveCrashedServersRequest request, ServerCallContext context)
        {
            return Task.FromResult(RemoveCrashed(request));
        }

        public RemoveCrashedServersReply RemoveCrashed(RemoveCrashedServersRequest request)
        {
            CrashedServers.Union(request.ServerIds);
            ServersByPartition[request.PartitionId].RemoveAll(x => request.ServerIds.Contains(x));
            return new RemoveCrashedServersReply
            {
                Success = true
            };
        }


    }
}
