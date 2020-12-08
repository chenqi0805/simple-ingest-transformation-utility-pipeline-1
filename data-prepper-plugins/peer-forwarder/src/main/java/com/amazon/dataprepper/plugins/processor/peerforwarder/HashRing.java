package com.amazon.dataprepper.plugins.processor.peerforwarder;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

@NotThreadSafe
public class HashRing {
    private final List<String> serverIps = new ArrayList<>();
    private final int numVirtualNodes;
    private final TreeMap<Long, String> virtualNodes = new TreeMap<>();

    public HashRing(final List<String> serverIps, final int numVirtualNodes) {
        Objects.requireNonNull(serverIps);
        this.numVirtualNodes = numVirtualNodes;
        for (final String serverIp: serverIps) {
            addServerIp(serverIp);
        }
    }

    public List<String> getServerIps() {
        return serverIps;
    }

    private void addServerIp(final String serverIp) {
        serverIps.add(serverIp);
        final byte[] serverIpInBytes = serverIp.getBytes();
        final Checksum crc32 = new CRC32();
        final ByteBuffer intBuffer = ByteBuffer.allocate(4);
        for (int i = 0; i < numVirtualNodes; i++) {
            crc32.update(serverIpInBytes, 0, serverIpInBytes.length);
            intBuffer.putInt(i);
            crc32.update(intBuffer.array(), 0, intBuffer.array().length);
            virtualNodes.putIfAbsent(crc32.getValue(), serverIp);
            crc32.reset();
            intBuffer.clear();
        }
    }

    public Optional<String> getServerIp(final String traceId) {
        if (virtualNodes.isEmpty()) {
            return Optional.empty();
        }
        final byte[] traceIdInBytes = traceId.getBytes();
        final Checksum crc32 = new CRC32();
        crc32.update(traceIdInBytes, 0, traceIdInBytes.length);
        final long hashcode = crc32.getValue();
        // obtain Map.Entry with key greater than the hashcode
        final Map.Entry<Long, String> entry = virtualNodes.higherEntry(hashcode);
        if (entry == null) {
            // return first node if no key is greater than the hashcode
            return Optional.of(virtualNodes.firstEntry().getValue());
        } else {
            return Optional.of(entry.getValue());
        }
    }
}