package com.amazon.ti.plugins.processor.state;

import com.amazon.ti.processor.state.ProcessorState;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;

/**
 * This class uses the default implementation of LMDB. This uses a ByteBuffer for keys and values,
 * so conversions to/from are handled in this class. We have the option of writing a BufferProxy
 * to let LMDB take care of more of that. That decision is TBD.
 */
public class LmdbProcessorState<T> implements ProcessorState<T> {

    static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final Dbi<ByteBuffer> db;
    private final Env<ByteBuffer> env;
    private final Class<T> clazz; //Needed for deserialization

    public LmdbProcessorState(final File dbPath, final String dbName, final Class<T> clazz) {
        env = Env.create()
                .setMapSize(10_485_760)
                .setMaxDbs(1)
                .open(dbPath);
        db = env.openDbi(dbName, DbiFlags.MDB_CREATE);
        this.clazz = clazz;
    }

    private ByteBuffer stringToDirectByteBuffer(final String in, final int size) {
        final ByteBuffer buffer = ByteBuffer.allocateDirect(size);
        buffer.put(in.getBytes(StandardCharsets.UTF_8)).flip();
        return buffer;
    }

    private T byteBufferToObject(final ByteBuffer valueBuffer) {
        try {
            final String valueAsString = StandardCharsets.UTF_8.decode(valueBuffer).toString();
            final T obj = OBJECT_MAPPER.readValue(valueAsString, clazz);
            return obj;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public void put(String key, T value) {
        try {
            final ByteBuffer keyBuffer = stringToDirectByteBuffer(key, env.getMaxKeySize());
            final String valueAsString = OBJECT_MAPPER.writeValueAsString(value);
            final ByteBuffer valueBuffer = stringToDirectByteBuffer(valueAsString, 700);
            db.put(keyBuffer, valueBuffer);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public T get(String key) {
        try(Txn<ByteBuffer> txn = env.txnRead()) {
            final ByteBuffer value = db.get(txn, stringToDirectByteBuffer(key, env.getMaxKeySize()));
            if(value == null) {
                return null;
            }
            return byteBufferToObject(value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, T> getAll() {
        final Txn<ByteBuffer> txn = env.txnRead();
        final Map<String, T> dbMap = new HashMap<>();
        db.iterate(txn).iterator().forEachRemaining(byteBufferKeyVal -> {
            dbMap.put(
                    StandardCharsets.UTF_8.decode(byteBufferKeyVal.key()).toString(),
                    byteBufferToObject(byteBufferKeyVal.val()));
        });
        return dbMap;
    }

    @Override
    public void delete() {
        final Txn<ByteBuffer> txn = env.txnWrite();
        db.drop(txn, true);
        txn.commit();
    }

    @Override
    public void close() {
        env.close();
    }
}
