package io.github.xinfra.lab.xkv.kv.coprocessor.dag;

import io.github.xinfra.lab.xkv.kv.coprocessor.eval.CopDatum;
import io.github.xinfra.lab.xkv.kv.coprocessor.eval.CopDatumComparator;
import io.github.xinfra.lab.xkv.kv.coprocessor.eval.ExprEvaluator;
import io.github.xinfra.lab.xkv.proto.Tipb;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public final class TopNOp implements CopOperator {

    private static final int MAX_TOPN_HEAP_SIZE = 65536;

    private final CopOperator child;
    private final List<Tipb.ByItem> orderByItems;
    private final int heapSize;
    private final int offset;

    private Comparator<ScoredRecord> comparator;
    private List<ScoredRecord> sorted;
    private int pos;

    public TopNOp(CopOperator child, List<Tipb.ByItem> orderByItems,
                   long topNLimit, long topNOffset) {
        this.child = child;
        this.orderByItems = orderByItems;
        this.heapSize = (int) Math.min(topNLimit + topNOffset, MAX_TOPN_HEAP_SIZE);
        this.offset = (int) topNOffset;
    }

    @Override
    public void open() {
        child.open();

        comparator = buildComparator();
        PriorityQueue<ScoredRecord> heap = new PriorityQueue<>(
                Math.max(heapSize, 1), comparator.reversed());

        CopRecord record;
        while ((record = child.next()) != null) {
            CopDatum[] sortKeys = new CopDatum[orderByItems.size()];
            for (int i = 0; i < orderByItems.size(); i++) {
                sortKeys[i] = ExprEvaluator.eval(orderByItems.get(i).getExpr(), record.row());
            }
            heap.offer(new ScoredRecord(record, sortKeys));
            if (heap.size() > heapSize) heap.poll();
        }

        sorted = new ArrayList<>(heap);
        sorted.sort(comparator);
        pos = offset;
    }

    @Override
    public CopRecord next() {
        if (pos >= sorted.size()) return null;
        return sorted.get(pos++).record();
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
