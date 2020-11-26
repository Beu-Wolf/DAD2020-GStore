﻿using Grpc.Core;
using System.Threading.Tasks;

namespace Client
{
    class PuppetMasterCommunicationService : PuppetMasterClientGrpcService.PuppetMasterClientGrpcServiceBase
    {
        private readonly GStoreClient Client;

        public PuppetMasterCommunicationService(GStoreClient client)
        {
            Client = client;
        }

        public override Task<NetworkInformationReply> NetworkInformation(NetworkInformationRequest request, ServerCallContext context)
        {
            return Task.FromResult(Client.NetworkInformation(request));
        }

        public override Task<ClientStatusReply> Status(ClientStatusRequest request, ServerCallContext context)
        {
            return Task.FromResult(Client.Status());
        }

        public override Task<ClientPingReply> Ping(ClientPingRequest request, ServerCallContext context)
        {
            return Task.FromResult(Client.Ping());
        }

    }
}
