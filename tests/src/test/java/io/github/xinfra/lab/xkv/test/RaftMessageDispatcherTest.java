package io.github.xinfra.lab.xkv.test;

import io.github.xinfra.lab.raft.proto.Eraftpb;
import io.github.xinfra.lab.xkv.kv.transport.RaftMessageDispatcher;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies {@link RaftMessageDispatcher}'s missing-region handler:
 * fires once per unknown region (not on every retry), and once
 * {@link RaftMessageDispatcher#register} lands, subsequent messages
 * route to the new transport.
 */
final class RaftMessageDispatcherTest {

    @Test
    void missingHandlerFiresOncePerUnknownRegion() {
        var dispatcher = new RaftMessageDispatcher();
        var calls = new AtomicInteger();
        dispatcher.setMissingHandler((regionId, msg) -> calls.incrementAndGet());

        // Three messages for the same unknown region — handler fires ONCE.
        for (int i = 0; i < 3; i++) {
            dispatcher.deliver(99, Eraftpb.Message.newBuilder()
                    .setMsgType(Eraftpb.MessageType.MsgAppend).build());
        }
        assertThat(calls.get()).isEqualTo(1);

        // After the spawn "completes", the dispatcher unlocks. Even without
        // a registered transport, a NEW message fires the handler again.
        dispatcher.onSpawnDone(99);
        dispatcher.deliver(99, Eraftpb.Message.newBuilder()
                .setMsgType(Eraftpb.MessageType.MsgAppend).build());
        assertThat(calls.get()).isEqualTo(2);
    }

    @Test
    void registerSilencesMissingHandler() throws Exception {
        var dispatcher = new RaftMessageDispatcher();
        var calls = new AtomicInteger();
        var latch = new CountDownLatch(1);
        var delivered = new ConcurrentLinkedQueue<Eraftpb.Message>();

        dispatcher.setMissingHandler((regionId, msg) -> {
            calls.incrementAndGet();
            latch.countDown();
        });
        dispatcher.deliver(42, Eraftpb.Message.newBuilder()
                .setMsgType(Eraftpb.MessageType.MsgAppend).build());
        assertThat(latch.await(1, TimeUnit.SECONDS)).isTrue();
        assertThat(calls.get()).isEqualTo(1);

        // Register a "transport" via reflection isn't great; use a real one
        // but capture deliveries into our queue. The dispatcher needs a
        // GrpcRaftTransport for the type signature; we can build a minimal
        // one with a custom receiver.
        var t = new io.github.xinfra.lab.xkv.kv.transport.GrpcRaftTransport(42, 1);
        t.setReceiver(delivered::offer);
        dispatcher.register(42, t);

        // Subsequent message routes to the transport; handler doesn't fire.
        var msg = Eraftpb.Message.newBuilder().setMsgType(Eraftpb.MessageType.MsgHeartbeat).build();
        dispatcher.deliver(42, msg);
        assertThat(delivered.poll()).isNotNull();
        assertThat(calls.get()).as("register clears spawn-in-flight").isEqualTo(1);
    }
}
