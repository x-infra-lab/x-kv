package io.github.xinfra.lab.xkv.pd.state;

import com.google.protobuf.ByteString;
import io.github.xinfra.lab.xkv.proto.Metapb;
import io.github.xinfra.lab.xkv.proto.PdInternalpb;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * RocksDB-backed implementation of {@link PdStateMachine}.
 *
 * <p>All durable state (stores, regions, cluster metadata, id allocator) is
 * persisted to a local RocksDB instance. An in-memory cache mirrors the
 * on-disk state for fast reads. Mutations are write-through: RocksDB first,
 * then cache update.
 *
 * <p>On restart, the constructor loads state from RocksDB into the caches,
 * eliminating the need for full raft snapshot install + entry replay.
 *
 * <h3>Key layout (single default CF)</h3>
 * <pre>
 *   [0x01]                    → bootstrapped (1 byte: 0/1)
 *   [0x02]                    → cluster (Metapb.Cluster protobuf)
 *   [0x03]                    → idAllocatorNext (8-byte big-endian)
 *   [0x10][8B storeId BE]     → Metapb.Store protobuf
 *   [0x20][8B regionId BE]    → Metapb.Region protobuf
 *   [0x30][startKey bytes]    → 8-byte regionId BE (start key index)
 * </pre>
 *
 * <h3>Volatile state (not persisted)</h3>
 * <ul>
 *   <li>{@code leaders} — region leader routing from heartbeat</li>
 *   <li>{@code regionStats} — approximate size/keys from heartbeat</li>
 * </ul>
 */
public final class RocksDbPdStateMachine implements PdStateMachine {
    private static final Logger log = LoggerFactory.getLogger(RocksDbPdStateMachine.class);

    static {
        RocksDB.loadLibrary();
    }

    private static final byte PREFIX_BOOTSTRAPPED = 0x01;
    private static final byte PREFIX_CLUSTER = 0x02;
    private static final byte PREFIX_ID_ALLOC = 0x03;
    private static final byte PREFIX_TSO_BOUND = 0x04;
    private static final byte PREFIX_STORE = 0x10;
    private static final byte PREFIX_REGION = 0x20;
    private static final byte PREFIX_REGION_START_KEY = 0x30;
    private static final byte PREFIX_MEMBER = 0x40;

    private final RocksDB db;
    private final WriteOptions syncWrite;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private boolean bootstrapped;
    private Metapb.Cluster cluster;
    private final HashMap<Long, Metapb.Store> stores = new HashMap<>();
    private final HashMap<Long, Metapb.Region> regionsById = new HashMap<>();
    private final NavigableMap<ByteString, Metapb.Region> regionsByStart = new TreeMap<>(
            (a, b) -> ByteString.unsignedLexicographicalComparator().compare(a, b));
    private final AtomicLong idAllocator = new AtomicLong(1000);
    private volatile long tsoBound;

    private final HashMap<Long, MemberInfo> membersMap = new HashMap<>();
    private final HashMap<Long, Metapb.Peer> leaders = new HashMap<>();
    private final ConcurrentHashMap<Long, RegionStats> regionStats = new ConcurrentHashMap<>();
    private final io.github.xinfra.lab.xkv.pd.state.placement.PlacementRuleManager placementRules =
            new io.github.xinfra.lab.xkv.pd.state.placement.PlacementRuleManager();
    private final io.github.xinfra.lab.xkv.pd.state.keyspace.KeyspaceManager keyspaces =
            new io.github.xinfra.lab.xkv.pd.state.keyspace.KeyspaceManager();
    private final io.github.xinfra.lab.xkv.pd.state.keyspace.ResourceGroupManager resourceGroups =
            new io.github.xinfra.lab.xkv.pd.state.keyspace.ResourceGroupManager();
    private volatile java.util.function.LongConsumer tsoBoundListener;
    private volatile java.util.function.BiConsumer<Long, String> memberAddListener;

    public RocksDbPdStateMachine(Path dataDir) {
        try {
            Files.createDirectories(dataDir);
        } catch (java.io.IOException e) {
            throw new RuntimeException("failed to create PD state dir: " + dataDir, e);
        }

        var opts = new Options()
                .setCreateIfMissing(true)
                .setMaxOpenFiles(64)
                .setKeepLogFileNum(3L);
        this.syncWrite = new WriteOptions().setSync(true);

        try {
            this.db = RocksDB.open(opts, dataDir.toString());
        } catch (RocksDBException e) {
            throw new RuntimeException("failed to open PD state RocksDB at " + dataDir, e);
        }

        loadFromDisk();
        log.info("PD state machine opened: bootstrapped={}, stores={}, regions={}, idAlloc={}",
                bootstrapped, stores.size(), regionsById.size(), idAllocator.get());
    }

    // ---- Load state from RocksDB on startup ----

    private void loadFromDisk() {
        try {
            byte[] val = db.get(new byte[]{PREFIX_BOOTSTRAPPED});
            if (val != null && val.length == 1 && val[0] == 1) {
                bootstrapped = true;
            }

            val = db.get(new byte[]{PREFIX_CLUSTER});
            if (val != null) {
                cluster = Metapb.Cluster.parseFrom(val);
            }

            val = db.get(new byte[]{PREFIX_ID_ALLOC});
            if (val != null && val.length == 8) {
                idAllocator.set(decodeLong(val));
            }

            val = db.get(new byte[]{PREFIX_TSO_BOUND});
            if (val != null && val.length == 8) {
                tsoBound = decodeLong(val);
            }

            try (RocksIterator it = db.newIterator()) {
                it.seek(new byte[]{PREFIX_STORE});
                while (it.isValid() && it.key()[0] == PREFIX_STORE) {
                    var store = Metapb.Store.parseFrom(it.value());
                    stores.put(store.getId(), store);
                    it.next();
                }
            }

            try (RocksIterator it = db.newIterator()) {
                it.seek(new byte[]{PREFIX_REGION});
                while (it.isValid() && it.key()[0] == PREFIX_REGION) {
                    var region = Metapb.Region.parseFrom(it.value());
                    regionsById.put(region.getId(), region);
                    regionsByStart.put(region.getStartKey(), region);
                    it.next();
                }
            }

            try (RocksIterator it = db.newIterator()) {
                it.seek(new byte[]{PREFIX_MEMBER});
                while (it.isValid() && it.key()[0] == PREFIX_MEMBER) {
                    var entry = PdInternalpb.MemberEntry.parseFrom(it.value());
                    membersMap.put(entry.getId(), new MemberInfo(
                            entry.getId(), entry.getName(),
                            entry.getRaftAddress(), entry.getClientAddress()));
                    it.next();
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("failed to load PD state from RocksDB", e);
        }
    }

    /**
     * Returns true if this state machine has persisted state from a prior run.
     * Used by {@code PdRaftNode} to skip snapshot install on restart.
     */
    public boolean hasPersistedState() {
        try {
            byte[] val = db.get(new byte[]{PREFIX_BOOTSTRAPPED});
            return val != null;
        } catch (RocksDBException e) {
            return false;
        }
    }

    // ---- PdStateMachine implementation ----

    @Override
    public boolean isBootstrapped() {
        lock.readLock().lock();
        try { return bootstrapped; } finally { lock.readLock().unlock(); }
    }

    @Override
    public void bootstrap(Metapb.Store firstStore, Metapb.Region firstRegion) {
        lock.writeLock().lock();
        try {
            if (bootstrapped) {
                log.info("bootstrap idempotent: cluster already up");
                return;
            }
            this.cluster = Metapb.Cluster.newBuilder().setId(1).setMaxPeerCount(3).build();
            this.stores.put(firstStore.getId(), firstStore);
            this.regionsById.put(firstRegion.getId(), firstRegion);
            this.regionsByStart.put(firstRegion.getStartKey(), firstRegion);
            this.bootstrapped = true;
            long newId = Math.max(idAllocator.get(),
                    Math.max(firstStore.getId(), firstRegion.getId()) + 1);
            this.idAllocator.set(newId);

            try (var batch = new WriteBatch()) {
                batch.put(new byte[]{PREFIX_BOOTSTRAPPED}, new byte[]{1});
                batch.put(new byte[]{PREFIX_CLUSTER}, cluster.toByteArray());
                batch.put(storeKey(firstStore.getId()), firstStore.toByteArray());
                batch.put(regionKey(firstRegion.getId()), firstRegion.toByteArray());
                batch.put(regionStartKeyKey(firstRegion.getStartKey()),
                        encodeLong(firstRegion.getId()));
                batch.put(new byte[]{PREFIX_ID_ALLOC}, encodeLong(newId));
                db.write(syncWrite, batch);
            } catch (RocksDBException e) {
                throw new RuntimeException("bootstrap write failed", e);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Metapb.Cluster cluster() {
        lock.readLock().lock();
        try { return cluster; } finally { lock.readLock().unlock(); }
    }

    @Override
    public void putStore(Metapb.Store store) {
        lock.writeLock().lock();
        try {
            stores.put(store.getId(), store);
            try {
                db.put(syncWrite, storeKey(store.getId()), store.toByteArray());
            } catch (RocksDBException e) {
                throw new RuntimeException("putStore write failed", e);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Optional<Metapb.Store> getStore(long storeId) {
        lock.readLock().lock();
        try { return Optional.ofNullable(stores.get(storeId)); }
        finally { lock.readLock().unlock(); }
    }

    @Override
    public Iterable<Metapb.Store> allStores() {
        lock.readLock().lock();
        try { return new ArrayList<>(stores.values()); }
        finally { lock.readLock().unlock(); }
    }

    @Override
    public Optional<Metapb.Region> getRegion(long regionId) {
        lock.readLock().lock();
        try { return Optional.ofNullable(regionsById.get(regionId)); }
        finally { lock.readLock().unlock(); }
    }

    @Override
    public Optional<Metapb.Region> getRegionByKey(byte[] key) {
        ByteString k = ByteString.copyFrom(key);
        lock.readLock().lock();
        try {
            var floor = regionsByStart.floorEntry(k);
            if (floor == null) return Optional.empty();
            var r = floor.getValue();
            ByteString end = r.getEndKey();
            if (!end.isEmpty() && cmp(end, k) <= 0) return Optional.empty();
            return Optional.of(r);
        } finally {
            lock.readLock().unlock();
        }
    }

    private static int cmp(ByteString a, ByteString b) {
        return ByteString.unsignedLexicographicalComparator().compare(a, b);
    }

    @Override
    public void updateRegion(Metapb.Region region) {
        lock.writeLock().lock();
        try {
            var prev = regionsById.get(region.getId());
            if (prev != null) {
                long prevConf = prev.getRegionEpoch().getConfVer();
                long prevVer = prev.getRegionEpoch().getVersion();
                long newConf = region.getRegionEpoch().getConfVer();
                long newVer = region.getRegionEpoch().getVersion();
                if (newConf < prevConf || newVer < prevVer) {
                    log.debug("dropping stale region update id={} epoch=({},{}) < prev=({},{})",
                            region.getId(), newConf, newVer, prevConf, prevVer);
                    return;
                }
                regionsByStart.remove(prev.getStartKey());
            }
            regionsById.put(region.getId(), region);
            regionsByStart.put(region.getStartKey(), region);

            try (var batch = new WriteBatch()) {
                batch.put(regionKey(region.getId()), region.toByteArray());
                if (prev != null && !prev.getStartKey().equals(region.getStartKey())) {
                    batch.delete(regionStartKeyKey(prev.getStartKey()));
                }
                batch.put(regionStartKeyKey(region.getStartKey()),
                        encodeLong(region.getId()));
                db.write(syncWrite, batch);
            } catch (RocksDBException e) {
                throw new RuntimeException("updateRegion write failed", e);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void updateLeader(long regionId, Metapb.Peer leader) {
        lock.writeLock().lock();
        try { leaders.put(regionId, leader); }
        finally { lock.writeLock().unlock(); }
    }

    @Override
    public Optional<Metapb.Peer> getLeader(long regionId) {
        lock.readLock().lock();
        try { return Optional.ofNullable(leaders.get(regionId)); }
        finally { lock.readLock().unlock(); }
    }

    @Override
    public Iterable<Metapb.Region> scanRegions(byte[] startKey, byte[] endKey, int limit) {
        ByteString s = ByteString.copyFrom(startKey == null ? new byte[0] : startKey);
        ByteString e = endKey == null ? null : ByteString.copyFrom(endKey);
        lock.readLock().lock();
        try {
            var out = new ArrayList<Metapb.Region>();
            ByteString cursor = s;
            var floor = regionsByStart.floorEntry(cursor);
            if (floor != null && (floor.getValue().getEndKey().isEmpty()
                    || cmp(floor.getValue().getEndKey(), s) > 0)) {
                cursor = floor.getKey();
            } else {
                var ceil = regionsByStart.ceilingEntry(cursor);
                cursor = ceil == null ? null : ceil.getKey();
            }
            while (cursor != null && out.size() < limit) {
                var entry = regionsByStart.ceilingEntry(cursor);
                if (entry == null) break;
                if (e != null && cmp(entry.getKey(), e) >= 0) break;
                out.add(entry.getValue());
                ByteString next = entry.getValue().getEndKey();
                if (next.isEmpty()) break;
                cursor = next;
            }
            return out;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void putMember(MemberInfo member) {
        lock.writeLock().lock();
        try {
            membersMap.put(member.id(), member);
            var entry = PdInternalpb.MemberEntry.newBuilder()
                    .setId(member.id())
                    .setName(member.name())
                    .setRaftAddress(member.raftAddress())
                    .setClientAddress(member.clientAddress())
                    .build();
            db.put(syncWrite, memberKey(member.id()), entry.toByteArray());
        } catch (RocksDBException e) {
            throw new RuntimeException("putMember write failed", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void removeMember(long memberId) {
        lock.writeLock().lock();
        try {
            membersMap.remove(memberId);
            db.delete(syncWrite, memberKey(memberId));
        } catch (RocksDBException e) {
            throw new RuntimeException("removeMember write failed", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Optional<MemberInfo> getMember(long memberId) {
        lock.readLock().lock();
        try { return Optional.ofNullable(membersMap.get(memberId)); }
        finally { lock.readLock().unlock(); }
    }

    @Override
    public java.util.List<MemberInfo> allMembers() {
        lock.readLock().lock();
        try { return new ArrayList<>(membersMap.values()); }
        finally { lock.readLock().unlock(); }
    }

    @Override
    public long allocId(int count) {
        if (count <= 0) throw new IllegalArgumentException();
        long base = idAllocator.getAndAdd(count);
        long next = base + count;
        try {
            db.put(syncWrite, new byte[]{PREFIX_ID_ALLOC}, encodeLong(next));
        } catch (RocksDBException e) {
            throw new RuntimeException("allocId write failed", e);
        }
        return base;
    }

    @Override
    public void updateRegionStats(long regionId, long approximateSize, long approximateKeys) {
        regionStats.put(regionId, new RegionStats(approximateSize, approximateKeys));
    }

    @Override
    public Optional<RegionStats> getRegionStats(long regionId) {
        return Optional.ofNullable(regionStats.get(regionId));
    }

    @Override
    public java.util.Map<Long, RegionStats> allRegionStats() {
        return java.util.Collections.unmodifiableMap(regionStats);
    }

    @Override
    public byte[] dumpSnapshot() {
        lock.readLock().lock();
        try {
            var snap = PdInternalpb.PdSnapshot.newBuilder()
                    .setBootstrapped(bootstrapped)
                    .setIdAllocatorNext(idAllocator.get())
                    .setTsoPhysicalBound(tsoBound);
            if (cluster != null) snap.setCluster(cluster);
            for (var s : stores.values()) snap.addStores(s);
            for (var r : regionsById.values()) snap.addRegions(r);
            for (var rule : placementRules.getRules()) snap.addPlacementRules(rule.toProto());
            for (var ks : keyspaces.encode()) snap.addKeyspaces(ks);
            for (var rg : resourceGroups.encode()) snap.addResourceGroups(rg);
            for (var m : membersMap.values()) {
                snap.addMembers(PdInternalpb.MemberEntry.newBuilder()
                        .setId(m.id()).setName(m.name())
                        .setRaftAddress(m.raftAddress())
                        .setClientAddress(m.clientAddress()));
            }
            return snap.build().toByteArray();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void installSnapshot(byte[] snapshot) {
        lock.writeLock().lock();
        try {
            var snap = PdInternalpb.PdSnapshot.parseFrom(snapshot);

            stores.clear();
            regionsById.clear();
            regionsByStart.clear();
            membersMap.clear();

            bootstrapped = snap.getBootstrapped();
            cluster = snap.hasCluster() ? snap.getCluster() : null;
            for (var s : snap.getStoresList()) stores.put(s.getId(), s);
            for (var r : snap.getRegionsList()) {
                regionsById.put(r.getId(), r);
                regionsByStart.put(r.getStartKey(), r);
            }
            if (snap.getIdAllocatorNext() > 0) {
                idAllocator.set(snap.getIdAllocatorNext());
            }
            if (snap.getTsoPhysicalBound() > 0) {
                tsoBound = snap.getTsoPhysicalBound();
            }

            for (var rp : snap.getPlacementRulesList()) {
                placementRules.setRule(
                        io.github.xinfra.lab.xkv.pd.state.placement.PlacementRule.fromProto(rp));
            }
            if (snap.getKeyspacesCount() > 0) {
                keyspaces.decode(snap.getKeyspacesList());
            }
            if (snap.getResourceGroupsCount() > 0) {
                resourceGroups.decode(snap.getResourceGroupsList());
            }
            for (var me : snap.getMembersList()) {
                membersMap.put(me.getId(), new MemberInfo(
                        me.getId(), me.getName(), me.getRaftAddress(), me.getClientAddress()));
            }
            clearAndWriteAll();
            log.info("PD snapshot installed: {} stores, {} regions, {} rules, {} keyspaces, {} resource_groups, idAlloc={}",
                    stores.size(), regionsById.size(), placementRules.ruleCount(),
                    keyspaces.size(), resourceGroups.size(), idAllocator.get());
        } catch (Exception e) {
            throw new RuntimeException("PD snapshot install failed", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void clearAndWriteAll() throws RocksDBException {
        // Delete all existing keys in the DB.
        try (RocksIterator it = db.newIterator()) {
            it.seekToFirst();
            while (it.isValid()) {
                db.delete(it.key());
                it.next();
            }
        }

        try (var batch = new WriteBatch()) {
            batch.put(new byte[]{PREFIX_BOOTSTRAPPED},
                    new byte[]{(byte) (bootstrapped ? 1 : 0)});
            if (cluster != null) {
                batch.put(new byte[]{PREFIX_CLUSTER}, cluster.toByteArray());
            }
            batch.put(new byte[]{PREFIX_ID_ALLOC}, encodeLong(idAllocator.get()));
            if (tsoBound > 0) {
                batch.put(new byte[]{PREFIX_TSO_BOUND}, encodeLong(tsoBound));
            }
            for (var s : stores.values()) {
                batch.put(storeKey(s.getId()), s.toByteArray());
            }
            for (var r : regionsById.values()) {
                batch.put(regionKey(r.getId()), r.toByteArray());
                batch.put(regionStartKeyKey(r.getStartKey()),
                        encodeLong(r.getId()));
            }
            for (var m : membersMap.values()) {
                var entry = PdInternalpb.MemberEntry.newBuilder()
                        .setId(m.id()).setName(m.name())
                        .setRaftAddress(m.raftAddress())
                        .setClientAddress(m.clientAddress())
                        .build();
                batch.put(memberKey(m.id()), entry.toByteArray());
            }
            db.write(syncWrite, batch);
        }
    }

    @Override
    public void applyCommand(byte[] command) {
        try {
            var cmd = PdInternalpb.PdCommand.parseFrom(command);
            switch (cmd.getType()) {
                case CMD_BOOTSTRAP -> {
                    var payload = cmd.getBootstrap();
                    bootstrap(payload.getStore(), payload.getRegion());
                }
                case CMD_PUT_STORE -> putStore(cmd.getStore());
                case CMD_UPDATE_REGION -> updateRegion(cmd.getRegion());
                case CMD_ALLOC_ID -> {
                    var payload = cmd.getAllocId();
                    long needed = payload.getBaseId() + payload.getCount();
                    long prev = idAllocator.getAndUpdate(cur -> Math.max(cur, needed));
                    if (needed > prev) {
                        try {
                            db.put(syncWrite, new byte[]{PREFIX_ID_ALLOC},
                                    encodeLong(idAllocator.get()));
                        } catch (RocksDBException e) {
                            log.warn("allocId persist failed: {}", e.getMessage());
                        }
                    }
                }
                case CMD_SET_PLACEMENT_RULE -> {
                    var payload = cmd.getPlacementRule();
                    if (payload.hasRule()) {
                        placementRules.setRule(
                                io.github.xinfra.lab.xkv.pd.state.placement.PlacementRule.fromProto(
                                        payload.getRule()));
                    }
                }
                case CMD_DELETE_PLACEMENT_RULE -> {
                    var payload = cmd.getPlacementRule();
                    placementRules.deleteRule(payload.getGroupId(), payload.getRuleId());
                }
                case CMD_SET_KEYSPACE -> {
                    var payload = cmd.getKeyspace();
                    if (payload.hasKeyspace()) {
                        keyspaces.setKeyspace(payload.getKeyspace());
                    }
                }
                case CMD_SET_RESOURCE_GROUP -> {
                    var payload = cmd.getResourceGroup();
                    if (payload.hasGroup()) {
                        resourceGroups.setGroup(payload.getGroup());
                    }
                }
                case CMD_DELETE_RESOURCE_GROUP -> {
                    var payload = cmd.getResourceGroup();
                    resourceGroups.deleteGroup(payload.getName());
                }
                case CMD_ADD_MEMBER -> {
                    var payload = cmd.getMemberChange();
                    putMember(new MemberInfo(payload.getMemberId(), payload.getName(),
                            payload.getRaftAddress(), payload.getClientAddress()));
                    var mal = memberAddListener;
                    if (mal != null && !payload.getRaftAddress().isEmpty()) {
                        mal.accept(payload.getMemberId(), payload.getRaftAddress());
                    }
                }
                case CMD_REMOVE_MEMBER -> {
                    var payload = cmd.getMemberChange();
                    removeMember(payload.getMemberId());
                }
                case CMD_EXTEND_TSO_BOUND -> {
                    long newBound = cmd.getTsoBound();
                    if (newBound > tsoBound) {
                        saveTsoBound(newBound);
                        var listener = tsoBoundListener;
                        if (listener != null) listener.accept(newBound);
                    }
                }
                default -> log.warn("unknown PD command type: {}", cmd.getType());
            }
        } catch (Exception e) {
            log.warn("applyCommand failed: {}", e.getMessage());
        }
    }

    public void saveTsoBound(long bound) {
        try {
            db.put(syncWrite, new byte[]{PREFIX_TSO_BOUND}, encodeLong(bound));
            this.tsoBound = bound;
        } catch (RocksDBException e) {
            throw new RuntimeException("saveTsoBound write failed", e);
        }
    }

    public long loadTsoBound() {
        return tsoBound;
    }

    public void setTsoBoundListener(java.util.function.LongConsumer listener) {
        this.tsoBoundListener = listener;
    }

    public void setMemberAddListener(java.util.function.BiConsumer<Long, String> listener) {
        this.memberAddListener = listener;
    }

    @Override public void onBecomeLeader() {}
    @Override public void onLoseLeader() {}

    @Override
    public io.github.xinfra.lab.xkv.pd.state.placement.PlacementRuleManager placementRuleManager() {
        return placementRules;
    }

    @Override
    public io.github.xinfra.lab.xkv.pd.state.keyspace.KeyspaceManager keyspaceManager() {
        return keyspaces;
    }

    @Override
    public io.github.xinfra.lab.xkv.pd.state.keyspace.ResourceGroupManager resourceGroupManager() {
        return resourceGroups;
    }

    @Override
    public void close() {
        syncWrite.close();
        db.close();
    }

    @Override
    public int storeCount() {
        lock.readLock().lock();
        try { return stores.size(); } finally { lock.readLock().unlock(); }
    }

    @Override
    public int regionCount() {
        lock.readLock().lock();
        try { return regionsById.size(); } finally { lock.readLock().unlock(); }
    }

    @Override
    public Collection<Metapb.Region> allRegions() {
        lock.readLock().lock();
        try { return new ArrayList<>(regionsById.values()); }
        finally { lock.readLock().unlock(); }
    }

    // ---- Key encoding helpers ----

    private static byte[] storeKey(long storeId) {
        byte[] key = new byte[9];
        key[0] = PREFIX_STORE;
        putLong(key, 1, storeId);
        return key;
    }

    private static byte[] regionKey(long regionId) {
        byte[] key = new byte[9];
        key[0] = PREFIX_REGION;
        putLong(key, 1, regionId);
        return key;
    }

    private static byte[] memberKey(long memberId) {
        byte[] key = new byte[9];
        key[0] = PREFIX_MEMBER;
        putLong(key, 1, memberId);
        return key;
    }

    private static byte[] regionStartKeyKey(ByteString startKey) {
        byte[] sk = startKey.toByteArray();
        byte[] key = new byte[1 + sk.length];
        key[0] = PREFIX_REGION_START_KEY;
        System.arraycopy(sk, 0, key, 1, sk.length);
        return key;
    }

    private static byte[] encodeLong(long v) {
        byte[] b = new byte[8];
        putLong(b, 0, v);
        return b;
    }

    private static long decodeLong(byte[] b) {
        return ByteBuffer.wrap(b).getLong();
    }

    private static void putLong(byte[] dest, int offset, long v) {
        dest[offset]     = (byte) (v >>> 56);
        dest[offset + 1] = (byte) (v >>> 48);
        dest[offset + 2] = (byte) (v >>> 40);
        dest[offset + 3] = (byte) (v >>> 32);
        dest[offset + 4] = (byte) (v >>> 24);
        dest[offset + 5] = (byte) (v >>> 16);
        dest[offset + 6] = (byte) (v >>> 8);
        dest[offset + 7] = (byte) v;
    }
}
