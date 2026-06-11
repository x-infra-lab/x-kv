package io.github.xinfra.lab.xkv.test;

import io.github.xinfra.lab.xkv.proto.Coprocessor;
import io.github.xinfra.lab.xkv.proto.Errorpb;
import io.github.xinfra.lab.xkv.proto.Kvrpcpb;
import io.github.xinfra.lab.xkv.proto.Metapb;
import io.github.xinfra.lab.xkv.proto.PDGrpc;
import io.github.xinfra.lab.xkv.proto.Pdpb;
import io.github.xinfra.lab.xkv.proto.TikvGrpc;
import io.github.xinfra.lab.xkv.proto.Tikvpb;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 0 verification — proto sources generate the full set of message and
 * gRPC service classes; the load-bearing fields (the ones that broke v1
 * because they did not exist) are present.
 */
final class ProtoSmokeTest {

    @Test
    void allGrpcServicesGenerated() {
        // Service classes (one per service) — protoc / grpc-java plugin output.
        assertThat(PDGrpc.SERVICE_NAME).isEqualTo("pdpb.PD");
        assertThat(TikvGrpc.SERVICE_NAME).isEqualTo("tikvpb.Tikv");
    }

    @Test
    void contextHasFullV2Surface() {
        // These fields didn't exist in v1 and prevented follower-read,
        // resource isolation, and proper deadline handling.
        var c = Kvrpcpb.Context.newBuilder()
                .setTerm(7)
                .setReplicaRead(Kvrpcpb.ReplicaReadType.FOLLOWER)
                .setIsolationLevel(Kvrpcpb.IsolationLevel.SI)
                .setMaxExecutionDurationMs(30_000)
                .setPriority(Kvrpcpb.CommandPri.High)
                .setClusterId(42)
                .build();
        assertThat(c.getTerm()).isEqualTo(7);
        assertThat(c.getReplicaRead()).isEqualTo(Kvrpcpb.ReplicaReadType.FOLLOWER);
        assertThat(c.getMaxExecutionDurationMs()).isEqualTo(30_000);
    }

    @Test
    void prewriteSupportsAsyncCommitAndOnePc() {
        // v1 proto had `service Transaction` declaring async-commit but no
        // request fields — async commit was effectively dead code. v2 wires
        // every field needed by the protocol.
        var p = Kvrpcpb.PrewriteRequest.newBuilder()
                .setUseAsyncCommit(true)
                .setTryOnePc(true)
                .setMaxCommitTs(100_000_000L)
                .setForUpdateTs(1)
                .build();
        assertThat(p.getUseAsyncCommit()).isTrue();
        assertThat(p.getTryOnePc()).isTrue();
        assertThat(p.getMaxCommitTs()).isEqualTo(100_000_000L);
    }

    @Test
    void keyErrorCarriesDeadlock() {
        var ke = Kvrpcpb.KeyError.newBuilder()
                .setDeadlock(Kvrpcpb.Deadlock.newBuilder().setLockTs(1).setDeadlockKeyHash(123))
                .build();
        assertThat(ke.hasDeadlock()).isTrue();
        assertThat(ke.getDeadlock().getDeadlockKeyHash()).isEqualTo(123);
    }

    @Test
    void regionErrorHasAllV2Cases() {
        var e = Errorpb.Error.newBuilder()
                .setMaxTimestampNotSynced(Errorpb.MaxTimestampNotSynced.getDefaultInstance())
                .build();
        assertThat(e.hasMaxTimestampNotSynced()).isTrue();

        var e2 = Errorpb.Error.newBuilder()
                .setRecoveryInProgress(Errorpb.RecoveryInProgress.newBuilder().setRegionId(99))
                .build();
        assertThat(e2.hasRecoveryInProgress()).isTrue();
    }

    @Test
    void peerRoleHasJointStates() {
        // v1's PeerRole was Voter/Learner only — not enough for joint
        // consensus membership changes.
        assertThat(Metapb.PeerRole.values())
                .contains(Metapb.PeerRole.IncomingVoter, Metapb.PeerRole.DemotingVoter);
    }

    @Test
    void pdHasServiceSafePoint() {
        // The single biggest GC bug in v1: no service safe-point. This
        // must exist in v2 proto.
        var r = Pdpb.UpdateServiceGCSafePointRequest.newBuilder()
                .setServiceId(com.google.protobuf.ByteString.copyFromUtf8("br-job-001"))
                .setTtl(300)
                .setSafePoint(123L)
                .build();
        assertThat(r.getTtl()).isEqualTo(300);
        assertThat(r.getServiceId().toStringUtf8()).isEqualTo("br-job-001");
    }

    @Test
    void coprocessorHasStreamingResponse() {
        // v1 coprocessor used a one-shot Response only — large scans OOMed.
        var sr = Coprocessor.StreamResponse.newBuilder().build();
        assertThat(sr).isNotNull();
    }

    @Test
    void batchCommandsExists() {
        // The fundamental throughput unlock — one bidi stream multiplexes
        // arbitrary KV RPCs.
        assertThat(Tikvpb.BatchCommandsRequest.getDescriptor().getFullName())
                .isEqualTo("tikvpb.BatchCommandsRequest");
    }
}
