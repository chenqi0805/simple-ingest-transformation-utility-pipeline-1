package com.amazon.ti.plugins.processor.state;

import com.amazon.ti.processor.state.ProcessorState;
import java.util.HashMap;
import java.util.Map;

public class InMemoryProcessorState<T> implements ProcessorState<T> {

    final Map<String, T> inMemoryState;

    public InMemoryProcessorState() {
        inMemoryState = new HashMap<>();
    }

    @Override
    public void put(String key, T value) {
        inMemoryState.put(key, value);
    }

    @Override
    public T get(String key) {
        return inMemoryState.get(key);
    }

    @Override
    public Map<String, T> getAll() {
        return new HashMap<>(inMemoryState);
    }

    @Override
    public void delete() {
        inMemoryState.clear();
    }

    @Override
    public void close(){
        //Nothing needed here
    }
}
