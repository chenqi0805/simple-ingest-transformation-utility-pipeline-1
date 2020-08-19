package com.amazon.ti.pipeline;

import com.amazon.ti.model.record.Record;
import com.amazon.ti.plugins.sink.TestSink;
import com.amazon.ti.plugins.source.TestSource;
import com.amazon.ti.model.source.Source;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class PipelineTest {

    @Test
    public void testExecute() throws InterruptedException {
        final Source<Record<String>> testSource = new TestSource();
        final TestSink testSink = new TestSink();
        final Pipeline testPipeline = new Pipeline("test-pipeline", testSource, Collections.singletonList(testSink), null);
        List<Record<String>> preRun = testSink.getCollectedRecords();
        assertThat("Sink records are not empty before Pipeline execution", preRun.isEmpty());
        testPipeline.execute();
        testPipeline.stop();
        List<Record<String>> postRun = testSink.getCollectedRecords();
        assertThat("Pipeline sink has records different from expected", postRun, is(TestSource.TEST_DATA));
    }
}
