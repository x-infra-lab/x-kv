package io.github.xinfra.lab.xkv.pd.transport;

import io.github.xinfra.lab.raft.proto.Eraftpb;
import io.github.xinfra.lab.xkv.proto.PdInternalpb;
import io.github.xinfra.lab.xkv.proto.PDRaftGrpc;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * gRPC service for receiving inbound PD raft messages.
 */
public final class PdRaftServiceImpl extends PDRaftGrpc.PDRaftImplBase {
    private static final Logger log = LoggerFactory.getLogger(PdRaftServiceImpl.class);

    private final PdRaftTransport transport;

    public PdRaftServiceImpl(PdRaftTransport transport) {
        this.transport = transport;
    }

    @Override
    public StreamObserver<PdInternalpb.PdRaftMessage> raft(
            StreamObserver<PdInternalpb.PdRaftDone> responseObserver) {
        return new StreamObserver<>() {
            @Override
            public void onNext(PdInternalpb.PdRaftMessage wire) {
                try {
                    var msg = Eraftpb.Message.parseFrom(wire.getMessage());
                    transport.deliver(msg);
                } catch (Throwable t) {
                    log.debug("pd-raft inbound parse failed: {}", t.getMessage());
                }
            }

            @Override
            public void onError(Throwable t) {
                log.debug("pd-raft inbound stream error: {}", t.getMessage());
            }

            @Override
            public void onCompleted() {
                responseObserver.onNext(PdInternalpb.PdRaftDone.newBuilder().build());
                responseObserver.onCompleted();
            }
        };
    }
}
