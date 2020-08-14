package com.amazon.ti.processor.state;

import com.amazon.ti.plugins.processor.state.LmdbProcessorState;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public class LmdbProcessorStateTest extends ProcessorStateTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Override
    public void setProcessorState() throws Exception {
        this.processorState = new LmdbProcessorState(temporaryFolder.newFolder(), "testDb", DataClass.class);
    }
}
