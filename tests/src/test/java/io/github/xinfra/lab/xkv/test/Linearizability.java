package io.github.xinfra.lab.xkv.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Per-key linearizability checker for the single-register model
 * (WRITE(k,v) / READ(k)→v). Implements Wing-Gong-style backtracking over the
 * space of valid linearization orders.
 *
 * <p>Ported from legacy/tiny-tests with no algorithmic changes.
 */
public class Linearizability {

    public enum OpType { READ, WRITE }

    public enum Outcome { SUCCESS, FAIL, INDETERMINATE }

    public static final class Op {
        public final int clientId;
        public final OpType type;
        public final String key;
        public final byte[] value;
        public final long invokeNanos;
        public final long returnNanos;
        public final Outcome outcome;

        public Op(int clientId, OpType type, String key, byte[] value,
                  long invokeNanos, long returnNanos) {
            this(clientId, type, key, value, invokeNanos, returnNanos, Outcome.SUCCESS);
        }

        public Op(int clientId, OpType type, String key, byte[] value,
                  long invokeNanos, long returnNanos, Outcome outcome) {
            this.clientId = clientId;
            this.type = type;
            this.key = key;
            this.value = value;
            this.invokeNanos = invokeNanos;
            this.returnNanos = returnNanos;
            this.outcome = outcome;
        }

        @Override public String toString() {
            return String.format("%s[%s,c%d,k=%s,v=%s,t=%d-%d]",
                    type, outcome, clientId, key,
                    value == null ? "null" : (value.length <= 8 ? hex(value)
                            : (value.length + "B@" + hex(Arrays.copyOf(value, 4)))),
                    invokeNanos, returnNanos);
        }

        private static String hex(byte[] b) {
            StringBuilder sb = new StringBuilder();
            for (byte v : b) sb.append(String.format("%02x", v));
            return sb.toString();
        }
    }

    public static final class Violation extends RuntimeException {
        public final String key;
        public final List<Op> prefix;
        public final Op stuck;
        public final String reason;

        Violation(String key, List<Op> prefix, Op stuck, String reason) {
            super("linearizability violation on key=" + key + ": " + reason
                    + "\n  stuck op:    " + stuck
                    + "\n  best prefix: " + prefix);
            this.key = key;
            this.prefix = prefix;
            this.stuck = stuck;
            this.reason = reason;
        }
    }

    public static void check(Iterable<Op> ops) {
        Map<String, List<Op>> perKey = new HashMap<>();
        for (Op op : ops) {
            perKey.computeIfAbsent(op.key, k -> new ArrayList<>()).add(op);
        }
        for (Map.Entry<String, List<Op>> e : perKey.entrySet()) {
            checkSingleKey(e.getKey(), e.getValue());
        }
    }

    public static void check(ConcurrentLinkedQueue<Op> queue) {
        List<Op> snapshot = new ArrayList<>(queue);
        check(snapshot);
    }

    private static void checkSingleKey(String key, List<Op> ops) {
        List<Op> active = new ArrayList<>(ops.size());
        for (Op op : ops) if (op.outcome != Outcome.FAIL) active.add(op);
        active.sort((a, b) -> Long.compare(a.invokeNanos, b.invokeNanos));
        ops = active;

        java.util.Set<String> writtenValues = new java.util.HashSet<>();
        writtenValues.add(hash(null));
        for (Op op : ops) if (op.type == OpType.WRITE) writtenValues.add(hash(op.value));
        for (Op op : ops) {
            if (op.type == OpType.READ && op.outcome == Outcome.SUCCESS
                    && !writtenValues.contains(hash(op.value))) {
                throw new Violation(key, java.util.Collections.emptyList(), op,
                        "SUCCESS READ observed value never written by any client");
            }
        }

        int n = ops.size();
        if (n == 0) return;

        boolean[][] mustPrecede = new boolean[n][n];
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                if (i == j) continue;
                if (ops.get(j).returnNanos < ops.get(i).invokeNanos) mustPrecede[i][j] = true;
            }
        }

        byte[] decision = new byte[n];
        List<Op> prefix = new ArrayList<>(n);
        byte[][] currentState = new byte[1][];
        java.util.Set<Long> deadEnds = new java.util.HashSet<>();
        if (!tryLinearize(ops, mustPrecede, decision, prefix, currentState, deadEnds, 0)) {
            Op stuck = null;
            for (int i = 0; i < n; i++) if (decision[i] != 1) { stuck = ops.get(i); break; }
            throw new Violation(key, new ArrayList<>(prefix), stuck,
                    "no valid linearization exists (Knossos with maybe-applied branches)");
        }
    }

    private static boolean tryLinearize(List<Op> ops, boolean[][] mustPrecede,
                                         byte[] decision, List<Op> prefix,
                                         byte[][] state,
                                         java.util.Set<Long> deadEnds, int placed) {
        int n = ops.size();
        boolean allDecided = true;
        for (int i = 0; i < n; i++) if (decision[i] == 0) { allDecided = false; break; }
        if (allDecided) return true;

        long stateHash = hashState(decision, state[0]);
        if (deadEnds.contains(stateHash)) return false;

        for (int i = 0; i < n; i++) {
            if (decision[i] != 0) continue;
            boolean blocked = false;
            for (int j = 0; j < n; j++) {
                if (j == i || decision[j] != 0) continue;
                if (mustPrecede[i][j]) { blocked = true; break; }
            }
            if (blocked) continue;

            Op op = ops.get(i);

            boolean tryLin = true;
            if (op.type == OpType.READ && !Arrays.equals(state[0], op.value)) {
                tryLin = false;
            }
            if (tryLin) {
                byte[] prevState = state[0];
                if (op.type == OpType.WRITE) state[0] = op.value;
                decision[i] = 1;
                prefix.add(op);
                if (tryLinearize(ops, mustPrecede, decision, prefix, state, deadEnds, placed + 1)) {
                    return true;
                }
                state[0] = prevState;
                decision[i] = 0;
                prefix.remove(prefix.size() - 1);
            }

            if (op.outcome == Outcome.INDETERMINATE) {
                decision[i] = -1;
                if (tryLinearize(ops, mustPrecede, decision, prefix, state, deadEnds, placed)) {
                    return true;
                }
                decision[i] = 0;
            }
        }
        deadEnds.add(stateHash);
        return false;
    }

    private static long hashState(byte[] decision, byte[] value) {
        long h = 0xcbf29ce484222325L;
        for (byte b : decision) {
            h ^= (b & 0xFF);
            h *= 0x100000001b3L;
        }
        long vh = value == null ? -1 : Arrays.hashCode(value);
        h ^= vh;
        h *= 0x100000001b3L;
        return h;
    }

    private static String hash(byte[] v) {
        if (v == null) return "null";
        return Integer.toHexString(Arrays.hashCode(v));
    }

    private Linearizability() {}
}
