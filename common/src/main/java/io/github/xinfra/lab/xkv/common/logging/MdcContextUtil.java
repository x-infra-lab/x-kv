package io.github.xinfra.lab.xkv.common.logging;

import org.slf4j.MDC;

public final class MdcContextUtil {

    private MdcContextUtil() {}

    public static void setRegion(long regionId) {
        MDC.put("region_id", String.valueOf(regionId));
    }

    public static void clearRegion() {
        MDC.remove("region_id");
    }
}
