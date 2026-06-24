package io.github.xinfra.lab.xkv.kv.cdc;

import io.github.xinfra.lab.xkv.kv.engine.StorageEngine;
import io.github.xinfra.lab.xkv.kv.mvcc.MvccKey;
import io.github.xinfra.lab.xkv.kv.mvcc.Write;
import io.github.xinfra.lab.xkv.proto.Cdcpb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Scans the Write CF for committed writes in {@code (checkpointTs, scanTs]}
 * within a key range, producing {@link CdcEvent}s that let a new CDC
 * subscriber catch up on historical data before switching to live events.
 *
 * <p>Algorithm:
 * <ol>
 *   <li>Seek to {@code MvccKey.encode(startKey, scanTs)} — the newest
 *       version of {@code startKey} at or before {@code scanTs}.</li>
 *   <li>For each MVCC entry, extract the userKey and commitTs. Skip
 *       ROLLBACK/LOCK records and entries outside the target range.</li>
 *   <li>For PUT writes, resolve the value from Default CF using the
 *       write's startTs (or use the short-value if inlined).</li>
 *   <li>After processing the latest visible write for a userKey, skip
 *       past all remaining versions of that key.</li>
 * </ol>
 *
 * <p>Only the <em>latest</em> committed write per userKey within the
 * scan window is emitted — this matches the subscriber's need to see
 * the current state, not the full history.
 */
public final class CdcIncrementalScanner {

    private CdcIncrementalScanner() {}

    /**
     * Scan committed writes in {@code (checkpointTs, scanTs]} for keys in
     * {@code [startKey, endKey)}. If {@code endKey} is null or empty, scan
     * to the end of the key space.
     *
     * @param engine       storage engine for iterator + point lookups
     * @param snapshot     snapshot to read from (caller manages lifecycle)
     * @param regionId     region id for emitted events
     * @param startKey     inclusive lower bound (user key), or null for beginning
     * @param endKey       exclusive upper bound (user key), or null for end
     * @param checkpointTs exclusive lower bound on commitTs
     * @param scanTs       inclusive upper bound on commitTs
     * @return list of CDC events in key order
     */
    public static List<CdcEvent> scan(StorageEngine engine,
                                       StorageEngine.Snapshot snapshot,
                                       long regionId,
                                       byte[] startKey,
                                       byte[] endKey,
                                       long checkpointTs,
                                       long scanTs) {
        List<CdcEvent> events = new ArrayList<>();

        byte[] seekKey = (startKey != null && startKey.length > 0)
                ? MvccKey.encode(startKey, scanTs)
                : new byte[]{0};

        try (var ro = engine.newReadOptions().snapshot(snapshot);
             var it = engine.newIterator(StorageEngine.Cf.WRITE, ro)) {

            it.seek(seekKey);

            byte[] currentUserKey = null;

            while (it.isValid()) {
                byte[] mvccKey = it.key();
                byte[] userKey = MvccKey.userKey(mvccKey);
                long commitTs = MvccKey.ts(mvccKey);

                // Past the end key — done.
                if (endKey != null && endKey.length > 0
                        && Arrays.compareUnsigned(userKey, endKey) >= 0) {
                    break;
                }

                // Skip older versions of a userKey we already processed.
                if (currentUserKey != null && Arrays.equals(userKey, currentUserKey)) {
                    it.next();
                    continue;
                }

                // commitTs above scanTs — this version is too new. But there
                // might be an older version of this key within the window, so
                // advance within the same userKey.
                if (commitTs > scanTs) {
                    it.next();
                    continue;
                }

                // commitTs at or below checkpointTs — this key has no new
                // writes in the window. Skip all remaining versions of it.
                if (commitTs <= checkpointTs) {
                    currentUserKey = userKey;
                    it.seek(MvccKey.afterAllVersionsOf(userKey));
                    continue;
                }

                // commitTs ∈ (checkpointTs, scanTs] — this is a relevant write.
                Write w;
                try {
                    w = Write.decode(it.value());
                } catch (Throwable t) {
                    currentUserKey = userKey;
                    it.seek(MvccKey.afterAllVersionsOf(userKey));
                    continue;
                }

                currentUserKey = userKey;

                if (w.type() == Write.Type.ROLLBACK || w.type() == Write.Type.LOCK) {
                    // Not a data-carrying write — skip to find an older
                    // version of this key that might be a real write.
                    it.next();
                    currentUserKey = null; // allow re-processing this userKey
                    continue;
                }

                if (w.type() == Write.Type.PUT) {
                    byte[] value;
                    if (w.hasShortValue()) {
                        value = w.shortValue();
                    } else {
                        value = engine.get(StorageEngine.Cf.DEFAULT,
                                MvccKey.encode(userKey, w.startTs()),
                                ro);
                    }
                    events.add(new CdcEvent(regionId, Cdcpb.Row.OpType.PUT,
                            userKey, value, null, w.startTs(), commitTs));
                } else if (w.type() == Write.Type.DELETE) {
                    events.add(new CdcEvent(regionId, Cdcpb.Row.OpType.DELETE,
                            userKey, null, null, w.startTs(), commitTs));
                }

                // Skip past all remaining versions of this userKey.
                it.seek(MvccKey.afterAllVersionsOf(userKey));
            }
        }

        return events;
    }
}
