package com.awslabs.aws.ti.examples.console;

import com.awslabs.aws.ti.Record;
import com.awslabs.aws.ti.buffer.TIBuffer;
import com.awslabs.aws.ti.source.Source;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class StdInSource implements Source {
    private final Scanner reader;
    private boolean haltFlag;

    public StdInSource() {
        reader = new Scanner(System.in);
        haltFlag = false;
    }

    @Override
    public void start(final TIBuffer buffer) {
        if(buffer == null) {
            //exception scenario
            return;
        }
        String line = "";
        while(!haltFlag && !"exit".equalsIgnoreCase(line)) {
            line = reader.nextLine();
            final byte[] lineBytes = line.getBytes(StandardCharsets.UTF_8);
            final Record record = new Record(ByteBuffer.wrap(lineBytes));
            buffer.put(record);
        }
    }

    @Override
    public void stop() {
        haltFlag = true;
    }
}
