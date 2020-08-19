package com.amazon.ti.pipeline;

import com.amazon.ti.model.record.Record;
import com.amazon.ti.model.buffer.Buffer;
import com.amazon.ti.model.processor.Processor;
import com.amazon.ti.model.sink.Sink;

import java.util.Collection;
import java.util.List;

@SuppressWarnings({"rawtypes", "unchecked"})
public class ProcessWorker implements Runnable {
    private final Buffer readBuffer;
    private final List<Processor> processors;
    private final Collection<Sink> sinks;
    private final Pipeline pipeline;
    private boolean isQueueEmpty = false;

    public ProcessWorker(
            final Buffer readBuffer,
            final List<Processor> processors,
            final Collection<Sink> sinks,
            final Pipeline pipeline) {
        this.readBuffer = readBuffer;
        this.processors = processors;
        this.sinks = sinks;
        this.pipeline = pipeline;
    }

    @Override
    public void run() {
        try {
            boolean isHalted = false;
            do {
                isHalted = isHalted || pipeline.isStopRequested();
                Collection records = readBuffer.readBatch();
                if (records != null && !records.isEmpty()) {
                    for (final Processor processor : processors) {
                        records = processor.execute(records);
                    }
                    postToSink(records);
                } else {
                    isQueueEmpty = true;
                }
            } while (!isHalted || !isBufferEmpty()); //If pipeline is stopped, we try to empty the already buffered records ?
        } catch (final Exception ex) {
            throw new IllegalStateException(ex);
        }
    }

    /**
     * TODO Implement this from Buffer [Probably AtomicBoolean]
     *
     * @return
     */
    private boolean isBufferEmpty() {
        return isQueueEmpty;
    }

    /**
     * TODO Add isolator pattern - Fail if one of the Sink fails [isolator Pattern]
     */
    private boolean postToSink(Collection<Record> records) {
        sinks.forEach(sink -> sink.output(records));
        return true;
    }
}
