package io.github.xinfra.lab.xkv.common.tls;

import java.nio.file.Path;

public record TlsConfig(Path certChain, Path privateKey, Path trustCerts, boolean mtls) {

    public static TlsConfig of(Path certChain, Path privateKey, Path trustCerts) {
        return new TlsConfig(certChain, privateKey, trustCerts, true);
    }

    public static TlsConfig clientOnly(Path trustCerts) {
        return new TlsConfig(null, null, trustCerts, false);
    }
}
