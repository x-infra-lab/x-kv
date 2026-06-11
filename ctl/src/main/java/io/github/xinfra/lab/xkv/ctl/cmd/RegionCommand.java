package io.github.xinfra.lab.xkv.ctl.cmd;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.proto.Metapb;
import io.github.xinfra.lab.xkv.proto.PDGrpc;
import io.github.xinfra.lab.xkv.proto.Pdpb;

public final class RegionCommand {

    public static int run(PDGrpc.PDBlockingStub stub, String action, String[] args) {
        return switch (action) {
            case "list" -> list(stub, args);
            case "info" -> {
                if (args.length < 1) {
                    System.err.println("Usage: xkv-ctl region info <region-id>");
                    yield 1;
                }
                yield info(stub, Long.parseLong(args[0]));
            }
            default -> {
                System.err.println("Unknown region command: " + action);
                System.err.println("Usage: xkv-ctl region {list [--limit N]|info <region-id>}");
                yield 1;
            }
        };
    }

    private static int list(PDGrpc.PDBlockingStub stub, String[] args) {
        int limit = 16;
        for (int i = 0; i < args.length - 1; i++) {
            if ("--limit".equals(args[i])) {
                limit = Integer.parseInt(args[i + 1]);
                break;
            }
        }

        try {
            var resp = stub.scanRegions(Pdpb.ScanRegionsRequest.newBuilder()
                    .setLimit(limit).build());
            System.out.printf("%-8s %-8s %-8s %-30s %-30s %-6s%n",
                    "ID", "EPOCH", "CONFVER", "START_KEY", "END_KEY", "PEERS");
            for (var r : resp.getRegionsList()) {
                System.out.printf("%-8d %-8d %-8d %-30s %-30s %-6d%n",
                        r.getId(),
                        r.getRegionEpoch().getVersion(),
                        r.getRegionEpoch().getConfVer(),
                        formatKey(r.getStartKey()),
                        formatKey(r.getEndKey()),
                        r.getPeersCount());
            }
            System.out.println("Listed " + resp.getRegionsCount() + " region(s)");
            return 0;
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        }
    }

    private static int info(PDGrpc.PDBlockingStub stub, long regionId) {
        try {
            var resp = stub.getRegionByID(Pdpb.GetRegionByIDRequest.newBuilder()
                    .setRegionId(regionId).build());
            if (!resp.hasRegion()) {
                System.err.println("Region " + regionId + " not found");
                return 1;
            }
            var r = resp.getRegion();
            System.out.println("Region ID:   " + r.getId());
            System.out.println("Start Key:   " + formatKey(r.getStartKey()));
            System.out.println("End Key:     " + formatKey(r.getEndKey()));
            System.out.println("Epoch:       ver=" + r.getRegionEpoch().getVersion()
                    + " conf_ver=" + r.getRegionEpoch().getConfVer());
            System.out.println("Peers:");
            for (var p : r.getPeersList()) {
                System.out.printf("  peer_id=%-6d store_id=%-6d role=%s%n",
                        p.getId(), p.getStoreId(), p.getRole().name());
            }
            if (resp.hasLeader()) {
                System.out.println("Leader:      peer_id=" + resp.getLeader().getId()
                        + " store_id=" + resp.getLeader().getStoreId());
            }
            return 0;
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        }
    }

    private static String formatKey(ByteString key) {
        if (key.isEmpty()) return "(empty)";
        if (isPrintable(key)) return key.toStringUtf8();
        return "0x" + bytesToHex(key.toByteArray());
    }

    private static boolean isPrintable(ByteString bs) {
        for (int i = 0; i < bs.size(); i++) {
            byte b = bs.byteAt(i);
            if (b < 0x20 || b > 0x7e) return false;
        }
        return true;
    }

    private static String bytesToHex(byte[] bytes) {
        var sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) sb.append(String.format("%02x", b));
        return sb.toString();
    }
}
