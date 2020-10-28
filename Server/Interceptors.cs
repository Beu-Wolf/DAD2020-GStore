using Grpc.Core;
using Grpc.Core.Interceptors;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Server
{
    public class DelayMessagesInterceptor : Interceptor
    {
        private readonly int MinDelay;
        private readonly int MaxDelay;

        private readonly object FreezeLock;
        public Boolean FreezeCommands { get; set; }

        private readonly Random Rnd = new Random();

        public DelayMessagesInterceptor(int minDelay, int maxDelay, object freezeLock)
        {
            MinDelay = minDelay;
            MaxDelay = maxDelay;
            FreezeLock = freezeLock;
            FreezeCommands = false;
        }

        public override Task<TResponse> UnaryServerHandler<TRequest, TResponse>(TRequest request, ServerCallContext context, UnaryServerMethod<TRequest, TResponse> continuation)
        {
            lock (FreezeLock)
            {
                while (FreezeCommands) Monitor.Wait(FreezeLock);
            }

            Thread.Sleep(Rnd.Next(MinDelay, MaxDelay));
            return continuation(request, context);
        }
    }
}
