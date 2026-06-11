package io.github.xinfra.lab.xkv.ctl;

import io.github.xinfra.lab.xkv.ctl.cmd.ClusterCommand;
import io.github.xinfra.lab.xkv.ctl.cmd.GcCommand;
import io.github.xinfra.lab.xkv.ctl.cmd.RegionCommand;
import io.github.xinfra.lab.xkv.ctl.cmd.StoreCommand;
import io.github.xinfra.lab.xkv.common.tls.GrpcChannelFactory;
import io.github.xinfra.lab.xkv.proto.PDGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public final class XKvCtl {

    private static final String USAGE = """
            Usage: xkv-ctl --pd <endpoint> <command> [options]

            Global options:
              --pd <host:port>    PD endpoint (default: 127.0.0.1:2379)

            Commands:
              cluster health              Show cluster health status
              cluster id                  Show cluster ID
              cluster members             List PD members

              store list                  List all stores
              store info <store-id>       Show store details

              region list [--limit N]     List regions (default limit 16)
              region info <region-id>     Show region details

              gc safepoint                Show current GC safe-point
            """;

    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println(USAGE);
            System.exit(0);
        }

        String pdEndpoint = "127.0.0.1:2379";
        int cmdStart = 0;
        for (int i = 0; i < args.length - 1; i++) {
            if ("--pd".equals(args[i])) {
                pdEndpoint = args[i + 1];
                cmdStart = i + 2;
                break;
            }
        }

        if (cmdStart >= args.length) {
            System.err.println("Error: no command specified");
            System.out.println(USAGE);
            System.exit(1);
        }

        String[] cmdArgs = Arrays.copyOfRange(args, cmdStart, args.length);
        String group = cmdArgs[0];
        String action = cmdArgs.length > 1 ? cmdArgs[1] : "";
        String[] rest = cmdArgs.length > 2 ? Arrays.copyOfRange(cmdArgs, 2, cmdArgs.length) : new String[0];

        ManagedChannel channel = ManagedChannelBuilder
                .forTarget(pdEndpoint)
                .usePlaintext()
                .build();

        try {
            var stub = PDGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(10, TimeUnit.SECONDS);

            int exitCode = switch (group) {
                case "cluster" -> ClusterCommand.run(stub, action, rest);
                case "store" -> StoreCommand.run(stub, action, rest);
                case "region" -> RegionCommand.run(stub, action, rest);
                case "gc" -> GcCommand.run(stub, action, rest);
                default -> {
                    System.err.println("Unknown command group: " + group);
                    System.out.println(USAGE);
                    yield 1;
                }
            };
            System.exit(exitCode);
        } finally {
            channel.shutdown();
            try { channel.awaitTermination(3, TimeUnit.SECONDS); }
            catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
        }
    }
}
