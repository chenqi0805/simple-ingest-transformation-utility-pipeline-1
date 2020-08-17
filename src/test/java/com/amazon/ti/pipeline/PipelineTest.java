package com.amazon.ti.pipeline;

import com.amazon.ti.Record;
import com.amazon.ti.plugins.sink.TestSink;
import com.amazon.ti.plugins.source.TestSource;
import com.amazon.ti.source.Source;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class PipelineTest {

    @Test
    public void testExecute() {
        final Source<Record<String>> testSource = new TestSource();
        final TestSink testSink = new TestSink();
        final Pipeline testPipeline = new Pipeline("test-pipeline", testSource, Collections.singletonList(testSink));
        List<Record<String>> preRun = testSink.getCollectedRecords();
        assertThat("Sink records are not empty before Pipeline execution", preRun.isEmpty());
        testPipeline.execute();
        List<Record<String>> postRun = testSink.getCollectedRecords();
        assertThat("Pipeline sink has records different from expected", postRun, is(TestSource.TEST_DATA));
    }
}
