using System;
using System.Collections.Generic;


class Client {
    static void Main(string[] args) {
        if(args.Length != 1) {
            Console.WriteLine("Usage: client.exe <script_file>");
            return;
        }

        try {
            string line;

            System.IO.StreamReader file = new System.IO.StreamReader(args[0]);
            while((line = file.ReadLine()) != null) {
                string[] cmd = line.Split();
                CommandDispatcher(cmd, file);
            }  

            file.Close();
        } catch (System.IO.FileNotFoundException) {
            Console.WriteLine("File not found. Exiting...");
            return;
        }
    }

    static void CommandDispatcher(string[] cmd, System.IO.StreamReader file) {
        switch (cmd[0]) {
            case "read":
                handle_read(cmd);
                break;
            case "write":
                handle_write(cmd);
                break;
            case "listServer":
                handle_listServer(cmd);
                break;
            case "listGlobal":
                handle_listGlobal(cmd);
                break;
            case "wait":
                handle_wait(cmd);
                break;
            case "begin-repeat":
                List<string[]> commands = new List<string[]>();
                string line;
                while((line = file.ReadLine()) != null && !line.Equals("end-repeat")) {
                    commands.Add(line.Split());
                }
                if(line == null) {
                    Console.WriteLine("Repeat command does not end. Exiting...");
                }
                handle_repeat(cmd, commands, file);
                break;
            case "end-repeat":
                Console.WriteLine("Invalid end-repeat: Not inside repeat statement!");
                break;
            default:
                Console.WriteLine("Command not recognized! >:(");
                break;
        }
    }

    static void handle_read(string[] cmd) {
        if(cmd.Length != 4) {
            Console.WriteLine("Invalid command format!");
            Console.WriteLine("Use: `read <partition_id> <object_id> <server_id>`");
            return;
        }

        string partitionId = cmd[1];
        string objectId = cmd[2];
        string serverId = cmd[3];

        // Console.WriteLine($"read {partitionId} {objectId} {serverId}");
        Console.WriteLine("read " + partitionId + " " + objectId + " " + serverId);
    }
    static void handle_write(string[] cmd) {
        if(cmd.Length != 4) {
            Console.WriteLine("Invalid command format!");
            Console.WriteLine("Use: `write <partition_id> <object_id> <value>`");
            return;
        }

        string partitionId = cmd[1];
        string objectId = cmd[2];
        string value = cmd[3];

        // Console.WriteLine($"write {partitionId} {objectId} {value}");
        Console.WriteLine("write " + partitionId + " " + objectId + " " + value);
    }
    static void handle_listServer(string[] cmd) {
        if(cmd.Length != 2) {
            Console.WriteLine("Invalid command format!");
            Console.WriteLine("Use: `listServer <server_id>`");
            return;
        }

        string serverId = cmd[1];

        // Console.WriteLine($"listServer {serverId}");
        Console.WriteLine("listServer " + " " + serverId);
    }
    static void handle_listGlobal(string[] cmd) {
        if(cmd.Length != 1) {
            Console.WriteLine("Invalid command format!");
            Console.WriteLine("Use: `listGlobal`");
            return;
        }

        // Console.WriteLine($"listGlobal");
        Console.WriteLine("listGlobal");
    }
    static void handle_wait(string[] cmd) {
        if(cmd.Length != 2) {
            Console.WriteLine("Invalid command format!");
            Console.WriteLine("Use: `wait <miliseconds>`");
            return;
        }

        string miliseconds = cmd[1];

        // Console.WriteLine($"listServer {miliseconds}");
        Console.WriteLine("wait " + miliseconds);
    }

    static void handle_repeat(string[] command, List<string[]> commands, System.IO.StreamReader file) {
        int n;
        if(!Int32.TryParse(command[1], out n)) {
            Console.WriteLine("Unable to parse repeat");
            Environment.Exit(-1);
        }

        Console.WriteLine("Iterating " + n + " times");

        for(var i = 1; i <= n; i++) {
            foreach (string[] cmd in commands) {
                string[] tmp_command = new string[cmd.Length];
                for(var arg_ix = 0; arg_ix < cmd.Length; arg_ix++) {
                    tmp_command[arg_ix] = cmd[arg_ix].Replace("$i", i.ToString());
                }
                CommandDispatcher(tmp_command, file);
            }
        }        
    }

}
