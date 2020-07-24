package com.amazon.ti.examples.console;

import com.amazon.ti.Record;
import com.amazon.ti.buffer.Buffer;
import com.amazon.ti.source.Source;

import java.util.Scanner;

public class StdInSource implements Source<Record<String>> {
    private final Scanner reader;
    private boolean haltFlag;

    public StdInSource() {
        reader = new Scanner(System.in);
        haltFlag = false;
    }

    @Override
    public void start(final Buffer<Record<String>> buffer) {
        if (buffer == null) {
            //exception scenario
            return;
        }
        String line = "";
        while (!haltFlag && !"exit".equalsIgnoreCase(line)) {
            line = reader.nextLine();
            final Record<String> record = new Record<>(line);
            buffer.put(record);
        }
    }

    @Override
    public void stop() {
        haltFlag = true;
    }
}
