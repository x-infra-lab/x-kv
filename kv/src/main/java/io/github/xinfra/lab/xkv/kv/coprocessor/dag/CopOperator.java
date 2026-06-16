package io.github.xinfra.lab.xkv.kv.coprocessor.dag;

public interface CopOperator extends AutoCloseable {

    void open();

    CopRecord next();

    @Override
    void close();
}
