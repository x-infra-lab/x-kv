package io.github.xinfra.lab.xkv.kv.coprocessor.dag;

import io.github.xinfra.lab.xkv.kv.coprocessor.eval.CopDatum;
import io.github.xinfra.lab.xkv.kv.coprocessor.eval.CopDatumComparator;
import io.github.xinfra.lab.xkv.kv.coprocessor.eval.ExprEvaluator;
import io.github.xinfra.lab.xkv.proto.Tipb;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Vectorized TopN operator. Materializes all input chunks into a
 * bounded heap, then returns sorted results in chunks.
 */
public final class VecTopNOp implements VecOperator {

    private static final int MAX_HEAP_SIZE = 65536;

    private final VecOperator child;
    private final List<Tipb.ByItem> orderByItems;
    private final int heapSize;
    private final int offset;

    private List<ScoredRecord> sorted;
    private int pos;

    public VecTopNOp(VecOperator child, List<Tipb.ByItem> orderByItems,
                      long topNLimit, long topNOffset) {
        this.child = child;
        this.orderByItems = orderByItems;
        this.heapSize = (int) Math.min(topNLimit + topNOffset, MAX_HEAP_SIZE);
        this.offset = (int) topNOffset;
    }

    @Override
    public void open() {
        child.open();

        Comparator<ScoredRecord> comparator = buildComparator();
        PriorityQueue<ScoredRecord> heap = new PriorityQueue<>(
                Math.max(heapSize, 1), comparator.reversed());

        CopChunk chunk;
        while ((chunk = child.nextChunk(1024)) != null) {
            for (int i = 0; i < chunk.size(); i++) {
                CopRecord record = chunk.get(i);
                CopDatum[] sortKeys = new CopDatum[orderByItems.size()];
                for (int j = 0; j < orderByItems.size(); j++) {
                    sortKeys[j] = ExprEvaluator.eval(orderByItems.get(j).getExpr(), record.row());
                }
                heap.offer(new ScoredRecord(record, sortKeys));
                if (heap.size() > heapSize) heap.poll();
            }
        }

        sorted = new ArrayList<>(heap);
        sorted.sort(comparator);
        pos = offset;
    }

    @Override
    public CopChunk nextChunk(int batchSize) {
        if (pos >= sorted.size()) return null;

        int end = Math.min(pos + batchSize, sorted.size());
        CopChunk chunk = new CopChunk(end - pos);
        for (int i = pos; i < end; i++) {
            chunk.add(sorted.get(i).record());
        }
        pos = end;
        return chunk;
    }

    @Override
    public void close() {
        child.close();
        sorted = null;
    }

    private Comparator<ScoredRecord> buildComparator() {
        return (a, b) -> {
            for (int i = 0; i < orderByItems.size(); i++) {
                int c = CopDatumComparator.compare(a.sortKeys[i], b.sortKeys[i]);
                if (orderByItems.get(i).getDesc()) c = -c;
                if (c != 0) return c;
            }
            return 0;
        };
    }

    private record ScoredRecord(CopRecord record, CopDatum[] sortKeys) {}
}
