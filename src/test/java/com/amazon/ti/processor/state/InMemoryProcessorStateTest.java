package com.amazon.ti.processor.state;

import com.amazon.ti.plugins.processor.state.InMemoryProcessorState;

public class InMemoryProcessorStateTest extends ProcessorStateTest {

    @Override
    public void setProcessorState() throws Exception {
        this.processorState = new InMemoryProcessorState<>();
    }
}
