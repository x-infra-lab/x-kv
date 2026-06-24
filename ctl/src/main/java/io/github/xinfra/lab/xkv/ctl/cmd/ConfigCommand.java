package io.github.xinfra.lab.xkv.ctl.cmd;

import io.github.xinfra.lab.xkv.proto.DebugGrpc;
import io.github.xinfra.lab.xkv.proto.Debugpb;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.concurrent.TimeUnit;

/**
 * CLI commands for online config management. Connects directly to a KV
 * store's Debug gRPC service (not PD).
 *
 * <pre>
 *   xkv-ctl config show  --store &lt;host:port&gt;
 *   xkv-ctl config set   --store &lt;host:port&gt; &lt;key&gt; &lt;value&gt;
 * </pre>
 */
public final class ConfigCommand {

    public static int run(String action, String[] args) {
        String storeAddr = null;
        int restStart = 0;
        for (int i = 0; i < args.length - 1; i++) {
            if ("--store".equals(args[i])) {
                storeAddr = args[i + 1];
                restStart = i + 2;
                break;
            }
        }
        if (storeAddr == null) {
            System.err.println("Error: --store <host:port> is required for config commands");
            System.err.println("Usage: xkv-ctl config {show|set <key> <value>} --store <host:port>");
            return 1;
        }

        ManagedChannel channel = ManagedChannelBuilder
                .forTarget(storeAddr)
                .usePlaintext()
                .build();

        try {
            var stub = DebugGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(10, TimeUnit.SECONDS);

            return switch (action) {
                case "show" -> showConfig(stub);
                case "set" -> {
                    String[] rest = new String[args.length - restStart];
                    System.arraycopy(args, restStart, rest, 0, rest.length);
                    if (rest.length < 2) {
                        System.err.println("Usage: xkv-ctl config set --store <host:port> <key> <value>");
                        yield 1;
                    }
                    yield setConfig(stub, rest[0], rest[1]);
                }
                default -> {
                    System.err.println("Unknown config command: " + action);
                    System.err.println("Usage: xkv-ctl config {show|set <key> <value>} --store <host:port>");
                    yield 1;
                }
            };
        } finally {
            channel.shutdown();
            try { channel.awaitTermination(3, TimeUnit.SECONDS); }
            catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
        }
    }

    private static int showConfig(DebugGrpc.DebugBlockingStub stub) {
        try {
            var resp = stub.getConfig(Debugpb.GetConfigRequest.newBuilder().build());
            System.out.printf("%-45s %s%n", "KEY", "VALUE");
            for (var entry : resp.getEntriesList()) {
                System.out.printf("%-45s %s%n", entry.getKey(), entry.getValue());
            }
            System.out.println("Total: " + resp.getEntriesCount() + " entries");
            return 0;
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        }
    }

    private static int setConfig(DebugGrpc.DebugBlockingStub stub, String key, String value) {
        try {
            var resp = stub.modifyConfig(Debugpb.ModifyConfigRequest.newBuilder()
                    .addEntries(Debugpb.ConfigEntry.newBuilder()
                            .setKey(key)
                            .setValue(value))
                    .build());
            if (!resp.getError().isEmpty()) {
                System.err.println("Error: " + resp.getError());
                return 1;
            }
            System.out.println("OK: " + key + " = " + value);
            return 0;
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        }
    }
}
