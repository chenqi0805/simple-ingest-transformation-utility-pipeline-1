package com.amazon.ti.examples.file;

import com.amazon.ti.Record;
import com.amazon.ti.processor.Processor;

import java.util.ArrayList;
import java.util.Collection;

/**
 * An simple String implementation of {@link Processor} which generates new Records with upper case content. The current
 * simpler implementation does not handle errors (if any).
 */
public class StringProcessor implements Processor<Record<String>, Record<String>> {

    @Override
    public Collection<Record<String>> execute(final Collection<Record<String>> records) {
        final Collection<Record<String>> modifiedRecords = new ArrayList<>(records.size());
        for(Record<String> record : records) {
            final String recordData = record.getData();
            modifiedRecords.add(new Record<>(recordData.toUpperCase()));
        }
        return modifiedRecords;
    }
}
