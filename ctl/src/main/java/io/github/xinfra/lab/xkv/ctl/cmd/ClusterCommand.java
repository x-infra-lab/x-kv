package io.github.xinfra.lab.xkv.ctl.cmd;

import io.github.xinfra.lab.xkv.proto.PDGrpc;
import io.github.xinfra.lab.xkv.proto.Pdpb;

public final class ClusterCommand {

    public static int run(PDGrpc.PDBlockingStub stub, String action, String[] args) {
        return switch (action) {
            case "health" -> health(stub);
            case "id" -> clusterId(stub);
            case "members" -> members(stub);
            default -> {
                System.err.println("Unknown cluster command: " + action);
                System.err.println("Usage: xkv-ctl cluster {health|id|members}");
                yield 1;
            }
        };
    }

    private static int health(PDGrpc.PDBlockingStub stub) {
        try {
            var resp = stub.isBootstrapped(Pdpb.IsBootstrappedRequest.newBuilder().build());
            if (resp.getBootstrapped()) {
                var clusterResp = stub.getClusterInfo(
                        Pdpb.GetClusterInfoRequest.newBuilder().build());
                long clusterId = clusterResp.hasCluster() ? clusterResp.getCluster().getId() : 0;
                System.out.println("Cluster is healthy");
                System.out.println("  Cluster ID: " + clusterId);
                System.out.println("  Bootstrapped: true");

                var membersResp = stub.getMembers(Pdpb.GetMembersRequest.newBuilder().build());
                System.out.println("  PD members: " + membersResp.getMembersCount());
                if (membersResp.hasLeader()) {
                    System.out.println("  PD leader: " + membersResp.getLeader().getName()
                            + " (id=" + membersResp.getLeader().getMemberId() + ")");
                }

                var storesResp = stub.getAllStores(Pdpb.GetAllStoresRequest.newBuilder().build());
                long upCount = storesResp.getStoresList().stream()
                        .filter(s -> s.getState() == io.github.xinfra.lab.xkv.proto.Metapb.StoreState.Up)
                        .count();
                System.out.println("  Stores: " + storesResp.getStoresCount()
                        + " total, " + upCount + " up");
            } else {
                System.out.println("Cluster is NOT bootstrapped");
                return 1;
            }
            return 0;
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        }
    }

    private static int clusterId(PDGrpc.PDBlockingStub stub) {
        try {
            var resp = stub.getClusterInfo(Pdpb.GetClusterInfoRequest.newBuilder().build());
            long id = resp.hasCluster() ? resp.getCluster().getId() : 0;
            System.out.println(id);
            return 0;
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        }
    }

    private static int members(PDGrpc.PDBlockingStub stub) {
        try {
            var resp = stub.getMembers(Pdpb.GetMembersRequest.newBuilder().build());
            if (resp.hasLeader()) {
                System.out.println("Leader: " + formatMember(resp.getLeader()));
            }
            for (var m : resp.getMembersList()) {
                boolean isLeader = resp.hasLeader()
                        && m.getMemberId() == resp.getLeader().getMemberId();
                System.out.println("  " + formatMember(m)
                        + (isLeader ? " [leader]" : ""));
            }
            return 0;
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        }
    }

    private static String formatMember(io.github.xinfra.lab.xkv.proto.Pdpb.Member m) {
        return String.format("%s (id=%d, client=%s)",
                m.getName(), m.getMemberId(),
                m.getClientUrlsList().isEmpty() ? "n/a" :
                        String.join(",", m.getClientUrlsList()));
    }
}
