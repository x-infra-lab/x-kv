package io.github.xinfra.lab.xkv.ctl.cmd;

import io.github.xinfra.lab.xkv.proto.PDGrpc;
import io.github.xinfra.lab.xkv.proto.Pdpb;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public final class GcCommand {

    public static int run(PDGrpc.PDBlockingStub stub, String action, String[] args) {
        return switch (action) {
            case "safepoint" -> safepoint(stub);
            default -> {
                System.err.println("Unknown gc command: " + action);
                System.err.println("Usage: xkv-ctl gc safepoint");
                yield 1;
            }
        };
    }

    private static int safepoint(PDGrpc.PDBlockingStub stub) {
        try {
            var resp = stub.getGCSafePoint(
                    Pdpb.GetGCSafePointRequest.newBuilder().build());
            long ts = resp.getSafePoint();
            System.out.println("GC Safe Point: " + ts);
            if (ts > 0) {
                long physicalMs = ts >> 18;
                System.out.println("  Physical:  " + formatTimestamp(physicalMs));
                System.out.println("  Logical:   " + (ts & 0x3FFFF));
            }

            var svcResp = stub.getAllServiceGCSafePoints(
                    Pdpb.GetAllServiceGCSafePointsRequest.newBuilder().build());
            if (svcResp.getSafePointsCount() > 0) {
                System.out.println("Service GC Safe Points:");
                for (var sp : svcResp.getSafePointsList()) {
                    System.out.printf("  service=%-20s safe_point=%-20d expires=%s%n",
                            sp.getServiceId(), sp.getSafePoint(),
                            formatTimestamp(sp.getExpiredAt()));
                }
            }
            return 0;
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        }
    }

    private static String formatTimestamp(long epochMs) {
        return DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
                .withZone(ZoneId.systemDefault())
                .format(Instant.ofEpochMilli(epochMs));
    }
}
