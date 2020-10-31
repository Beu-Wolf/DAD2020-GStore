using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;

namespace PuppetMaster
{
    public class PuppetMaster
    {
        private PuppetMasterForm Form;

        int ReplicationFactor;

        public PuppetMaster()
        {
            // We need to detect if this value was already assigned
            // Cannot use readonly since we can't initialize it in the constructor
            this.ReplicationFactor = -1;
        }

        public void ParseCommand(string command)
        {
            string[] args = command.Split((char[])null, StringSplitOptions.RemoveEmptyEntries);

            if (args.Length == 0) return;
            switch (args[0])
            {
                case "ReplicationFactor":
                    Task.Run(() => HandleReplicationFactorCommand(args));
                    break;
                case "Server":
                    Task.Run(() => HandleServerCommand(args));
                    break;
                case "Partition":
                    Task.Run(() => HandlePartitionCommand(args));
                    break;
                case "Client":
                    Task.Run(() => HandleClientCommand(args));
                    break;
                case "Status":
                    Task.Run(() => HandleStatusCommand(args));
                    break;
                case "Crash":
                    Task.Run(() => HandleCrashCommand(args));
                    break;
                case "Freeze":
                    Task.Run(() => HandleFreezeCommand(args));
                    break;
                case "Unfreeze":
                    Task.Run(() => HandleUnfreezeCommand(args));
                    break;
                case "Wait":
                    HandleWaitCommand(args);
                    break;
                default:
                    this.Form.Error($"Unknown command: {args[0]}");
                    break;
            }
        }

        private void HandleWaitCommand(string[] args)
        {
            if (args.Length != 1+1)
            {
                this.Form.Error("Wait: wrong number of arguments");
                goto WaitUsage;
            }

            return;
        WaitUsage:
            this.Form.Error("Wait usage: Wait x_ms");
        }

        private void HandleUnfreezeCommand(string[] args)
        {
            if (args.Length != 1+1)
            {
                this.Form.Error("Unfreeze: wrong number of arguments");
                goto UnfreezeUsage;
            }

            return;
        UnfreezeUsage:
            this.Form.Error("Unfreeze usage: Unreeze server_id");
        }

        private void HandleFreezeCommand(string[] args)
        {
            if (args.Length != 1+1)
            {
                this.Form.Error("Freeze: wrong number of arguments");
                goto FreezeUsage;
            }

            return;
        FreezeUsage:
            this.Form.Error("Freeze usage: Freeze server_id");
        }

        private void HandleCrashCommand(string[] args)
        {
            if (args.Length != 1+1)
            {
                this.Form.Error("Crash: wrong number of arguments");
                goto CrashUsage;
            }

            return;
        CrashUsage:
            this.Form.Error("Crash usage: Crash server_id");
        }

        private void HandleStatusCommand(string[] args)
        {

        }

        private void HandleClientCommand(string[] args)
        {
            if (args.Length != 1+3)
            {
                this.Form.Error("Client: wrong number of arguments");
                goto ClientUsage;
            }

            return;
        ClientUsage:
            this.Form.Error("Client usage: Client username client_URL script_file");
        }

        private void HandlePartitionCommand(string[] args)
        {
            if (args.Length < 1+3)
            {
                this.Form.Error("Partition: wrong number of arguments");
                goto PartitionUsage;
            }

            if(!int.TryParse(args[1], out int replicationFactor))
            {
                this.Form.Error("Partition: invalid type for argument r");
                return;
            }

            // check if replication factor consistency
            if (this.ReplicationFactor != -1 && replicationFactor != this.ReplicationFactor)
            {
                this.Form.Error($"Partition: replication factor already assigned to {this.ReplicationFactor}");
                return;
            }
            this.ReplicationFactor = replicationFactor;

            // check number of given servers
            if (this.ReplicationFactor != args.Length - 3)
            {
                this.Form.Error($"Partition: you must supply {this.ReplicationFactor} servers to create this partition");
                return;
            }
            this.ReplicationFactor = r;

            return;
        PartitionUsage:
            this.Form.Error("Partition usage: Partition r partition_name server_id_1 ... server_id_r");
        }

        private void HandleServerCommand(string[] args)
        {
            if (args.Length != 1+4)
            {
                this.Form.Error("Server: wrong number of arguments");
                goto ServerUsage;
            }

            return;
        ServerUsage:
            this.Form.Error("Server usage: Server server_id URL min_delay max_delay");
        }

        private void HandleReplicationFactorCommand(string[] args)
        {
            if (args.Length != 1+1)
            {
                this.Form.Error("Replication: wrong number of arguments");
                goto ReplicationUsage;
            }

            if (!int.TryParse(args[1], out int replicationFactor))
            {
                this.Form.Error("Replication: invalid type for argument r");
                return;
            }

            if (this.ReplicationFactor != -1 && replicationFactor != this.ReplicationFactor)
            {
                this.Form.Error($"Replication: replication factor already assigned to {this.ReplicationFactor}");
                return;
            }

            this.ReplicationFactor = replicationFactor;

            return;
        ReplicationUsage:
            this.Form.Error("ReplicationFactor usage: ReplicationFactor r");
        }

        public void LinkForm(PuppetMasterForm form)
        { 
            this.Form = form;
            this.Form.LinkPuppetMaster(this);
        }
    }
}
