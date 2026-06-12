package io.github.xinfra.lab.xkv.ctl.cmd;

import io.github.xinfra.lab.xkv.proto.PDGrpc;
import io.github.xinfra.lab.xkv.proto.Pdpb;

public final class StoreCommand {

    public static int run(PDGrpc.PDBlockingStub stub, String action, String[] args) {
        return switch (action) {
            case "list" -> list(stub);
            case "info" -> {
                if (args.length < 1) {
                    System.err.println("Usage: xkv-ctl store info <store-id>");
                    yield 1;
                }
                yield info(stub, Long.parseLong(args[0]));
            }
            default -> {
                System.err.println("Unknown store command: " + action);
                System.err.println("Usage: xkv-ctl store {list|info <store-id>}");
                yield 1;
            }
        };
    }

    private static int list(PDGrpc.PDBlockingStub stub) {
        try {
            var resp = stub.getAllStores(Pdpb.GetAllStoresRequest.newBuilder().build());
            System.out.printf("%-8s %-25s %-25s %-10s%n",
                    "ID", "ADDRESS", "PEER_ADDRESS", "STATE");
            for (var s : resp.getStoresList()) {
                System.out.printf("%-8d %-25s %-25s %-10s%n",
                        s.getId(), s.getAddress(), s.getPeerAddress(),
                        s.getState().name());
            }
            System.out.println("Total: " + resp.getStoresCount() + " store(s)");
            return 0;
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        }
    }

    private static int info(PDGrpc.PDBlockingStub stub, long storeId) {
        try {
            var resp = stub.getStore(Pdpb.GetStoreRequest.newBuilder()
                    .setStoreId(storeId).build());
            if (!resp.hasStore()) {
                System.err.println("Store " + storeId + " not found");
                return 1;
            }
            var s = resp.getStore();
            System.out.println("Store ID:      " + s.getId());
            System.out.println("Address:       " + s.getAddress());
            System.out.println("Peer Address:  " + s.getPeerAddress());
            System.out.println("State:         " + s.getState().name());
            System.out.println("Version:       " + s.getVersion());
            if (s.getLabelsCount() > 0) {
                System.out.println("Labels:");
                for (var l : s.getLabelsList()) {
                    System.out.println("  " + l.getKey() + "=" + l.getValue());
                }
            }
            if (resp.hasStats()) {
                var st = resp.getStats();
                System.out.println("Stats:");
                System.out.printf("  Capacity:  %d MB%n", st.getCapacity() / (1024 * 1024));
                System.out.printf("  Available: %d MB%n", st.getAvailable() / (1024 * 1024));
                System.out.printf("  Used:      %d MB%n", st.getUsedSize() / (1024 * 1024));
                System.out.println("  Regions:   " + st.getRegionCount());
            }
            return 0;
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        }
    }
}
