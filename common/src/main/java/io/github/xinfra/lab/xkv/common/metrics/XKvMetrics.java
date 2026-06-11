package io.github.xinfra.lab.xkv.common.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public final class XKvMetrics {

    private static final AtomicReference<PrometheusMeterRegistry> INSTANCE =
            new AtomicReference<>();

    private XKvMetrics() {}

    public static PrometheusMeterRegistry registry() {
        var r = INSTANCE.get();
        if (r != null) return r;
        return init("unknown");
    }

    public static synchronized PrometheusMeterRegistry init(String component) {
        var existing = INSTANCE.get();
        if (existing != null) return existing;
        var reg = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        reg.config().commonTags(List.of(Tag.of("component", component)));
        INSTANCE.set(reg);
        return reg;
    }

    public static synchronized void reset() {
        var old = INSTANCE.getAndSet(null);
        if (old != null) old.close();
    }

    public static MeterRegistry registryOrNoop() {
        var r = INSTANCE.get();
        return r != null ? r : io.micrometer.core.instrument.Metrics.globalRegistry;
    }

    public static Counter errorCounter(String component, String operation) {
        return Counter.builder("xkv_errors_total")
                .tag("component", component)
                .tag("operation", operation)
                .register(registryOrNoop());
    }
}
