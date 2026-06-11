package io.github.xinfra.lab.xkv.common.logging;

import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import org.slf4j.MDC;

public final class MdcServerInterceptor implements ServerInterceptor {

    private final String nodeIdKey;
    private final String nodeIdValue;

    public MdcServerInterceptor(String nodeIdKey, long nodeId) {
        this.nodeIdKey = nodeIdKey;
        this.nodeIdValue = String.valueOf(nodeId);
    }

    public static MdcServerInterceptor forStore(long storeId) {
        return new MdcServerInterceptor("store_id", storeId);
    }

    public static MdcServerInterceptor forPd(long nodeId) {
        return new MdcServerInterceptor("node_id", nodeId);
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers,
            ServerCallHandler<ReqT, RespT> next) {

        MDC.put(nodeIdKey, nodeIdValue);
        MDC.put("rpc_method", call.getMethodDescriptor().getFullMethodName());

        var listener = next.startCall(call, headers);
        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(listener) {
            @Override
            public void onComplete() {
                try {
                    super.onComplete();
                } finally {
                    clearMdc();
                }
            }

            @Override
            public void onCancel() {
                try {
                    super.onCancel();
                } finally {
                    clearMdc();
                }
            }

            private void clearMdc() {
                MDC.remove(nodeIdKey);
                MDC.remove("rpc_method");
                MDC.remove("region_id");
            }
        };
    }
}
