/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.TreeMap;

import static dev.morling.onebrc.CalculateAverage_jmiettinen.MAX_KEY_SIZE;

public class CalculateAverage_jmiettinen {
    private static final String FILE = "./measurements.txt";

    private static final int MIN_SEGMENT_SIZE = 64 * 1024 * 1024;
    static final int MAX_KEY_SIZE = 100;

    /*
     * My results on this computer:
     *
     * CalculateAverage: 2m37.788s
     * CalculateAverage_royvanrijn: 0m29.639s
     * CalculateAverage_spullara: 0m2.013s
     *
     */

    private static Tools.ByteArrayToResultMap readSegment(Tools.FileSegment segment, String filename) {
        var resultMap = new Tools.ByteArrayToResultMap();
        long segmentEnd = segment.end();
        try {
            try (var fileChannel = (FileChannel) Files.newByteChannel(Path.of(filename), StandardOpenOption.READ)) {
                var bb = fileChannel.map(FileChannel.MapMode.READ_ONLY, segment.start(), segmentEnd - segment.start());
                // Up to 100 characters for a city name
                var buffer = new byte[100];
                int startLine;
                int limit = bb.limit();
                while ((startLine = bb.position()) < limit) {
                    int currentPosition = startLine;
                    byte b;
                    int offset = 0;
                    int hash = 0;
                    // TODO Read more than a byte at a time.
                    while (currentPosition != segmentEnd && (b = bb.get(currentPosition++)) != ';') {
                        buffer[offset++] = b;
                        hash = 31 * hash + b;
                    }
                    int temp;
                    int negative = 1;
                    // Inspired by @yemreinci to unroll this even further
                    if (bb.get(currentPosition) == '-') {
                        negative = -1;
                        currentPosition++;
                    }
                    if (bb.get(currentPosition + 1) == '.') {
                        temp = negative * ((bb.get(currentPosition) - '0') * 10 + (bb.get(currentPosition + 2) - '0'));
                        currentPosition += 3;
                    }
                    else {
                        temp = negative * ((bb.get(currentPosition) - '0') * 100 + ((bb.get(currentPosition + 1) - '0') * 10 + (bb.get(currentPosition + 3) - '0')));
                        currentPosition += 4;
                    }
                    if (bb.get(currentPosition) == '\r') {
                        currentPosition++;
                    }
                    currentPosition++;
                    resultMap.putOrMerge(buffer, 0, offset, (short) temp, hash);
                    bb.position(currentPosition);
                }
                return resultMap;
            }
        }
        catch (IOException e) {
            throw new RuntimeException("IOException", e);
        }
    }

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        long start = System.currentTimeMillis();
        var filename = args.length == 0 ? FILE : args[0];
        var file = new File(filename);

        var resultsMap = getFileSegments(file)
                .stream()
                .map(segment -> readSegment(segment, filename))
                .parallel()
                .flatMap(partition -> partition.getAll().stream())
                .collect(Collectors.toMap(Tools.Entry::key, Tools.Entry::value, CalculateAverage_jmiettinen::merge, TreeMap::new));

        System.out.println(resultsMap);
    }

    private static List<Tools.FileSegment> getFileSegments(File file) throws IOException {
        int numberOfSegments = Runtime.getRuntime().availableProcessors();
        long fileSize = file.length();
        long segmentSize = fileSize / numberOfSegments;
        List<Tools.FileSegment> segments = new ArrayList<>(numberOfSegments);
        // Pointless to split small files
        if (segmentSize < MIN_SEGMENT_SIZE) {
            segments.add(new Tools.FileSegment(0, fileSize));
            return segments;
        }
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
            for (int i = 0; i < numberOfSegments; i++) {
                long segStart = i * segmentSize;
                long segEnd = (i == numberOfSegments - 1) ? fileSize : segStart + segmentSize;
                segStart = findSegment(i, 0, randomAccessFile, segStart, segEnd);
                segEnd = findSegment(i, numberOfSegments - 1, randomAccessFile, segEnd, fileSize);

                segments.add(new Tools.FileSegment(segStart, segEnd));
            }
        }
        return segments;
    }

    private static Tools.Result merge(Tools.Result v, Tools.Result value) {
        return merge(v, value.min, value.max, value.sum, value.count);
    }

    private static Tools.Result merge(Tools.Result v, short min, short max, int sum, int count) {
        v.min = (short) Math.min(v.min, min);
        v.max = (short) Math.max(v.max, max);
        v.sum += sum;
        v.count += count;
        return v;
    }

    private static long findSegment(int i, int skipSegment, RandomAccessFile raf, long location, long fileSize) throws IOException {
        if (i != skipSegment) {
            raf.seek(location);
            while (location < fileSize) {
                location++;
                if (raf.read() == '\n')
                    break;
            }
        }
        return location;
    }
}

class Tools {

    static class Result {
        short min, max;
        int sum;
        int count;

        Result() {
        }

        @Override
        public String toString() {
            return round(min) + "/" + round((double) sum / (double) count) + "/" + round(max);
        }

        double round(double v) {
            var asDouble = v / 10.0;
            return Math.round(asDouble * 10.0) / 10.0;
        }

    }

    record Entry(String key, Tools.Result value) {
    }

    record FileSegment(long start, long end) {
    }

    static class ByteArrayToResultMap {
        public static final int MAP_SIZE = 1024 * 64;

        private static final int KEY_INDEX_OFFSET = 0;
        private static final int HASH_CODE_OFFSET = KEY_INDEX_OFFSET + Integer.BYTES;
        private static final int KEY_SIZE_OFFSET = HASH_CODE_OFFSET + Integer.BYTES;
        private static final int SUM_OFFSET = KEY_SIZE_OFFSET + Integer.BYTES;
        private static final int COUNT_OFFSET = SUM_OFFSET + Integer.BYTES;
        private static final int MIN_OFFSET = COUNT_OFFSET + Integer.BYTES;
        private static final int MAX_OFFSET = MIN_OFFSET + Short.BYTES;

        private static final int ENTRY_SIZE = MAX_OFFSET + Short.BYTES;
        final ByteBuffer buf = ByteBuffer.allocate(MAP_SIZE * ENTRY_SIZE);
        byte[] keysAsBytes = new byte[256];

        private int freeIndex = 0;
        private int entries = 0;

        ByteArrayToResultMap() {
            buf.order(ByteOrder.nativeOrder());
            buf.limit(buf.capacity());
        }

        private int getKeyIndexFrom(int baseOffset) {
            return ~buf.getInt(KEY_INDEX_OFFSET + baseOffset);
        }

        private void setKeyIndexTo(int baseOffset, int index) {
            buf.putInt(KEY_INDEX_OFFSET + baseOffset, ~index);
        }

        private int addKeyBytes(byte[] key, int offset, int size) {
            if (freeIndex + size >= keysAsBytes.length) {
                var addedSize = Math.max(MAX_KEY_SIZE, keysAsBytes.length);
                var newBytes = new byte[keysAsBytes.length + addedSize];
                System.arraycopy(keysAsBytes, 0, newBytes, 0, freeIndex);
                keysAsBytes = newBytes;
            }
            var startIndex = freeIndex;
            System.arraycopy(key, offset, keysAsBytes, freeIndex, size);
            freeIndex += size;
            return startIndex;
        }

        public void putOrMerge(byte[] key, int offset, int size, short temp, int hash) {
            int indexUnscaled = hash & (MAP_SIZE - 1);
            int index = ENTRY_SIZE * indexUnscaled;
            while (getKeyIndexFrom(index) >= 0) {
                var slotKeyIndex = getKeyIndexFrom(index);
                var slotHash = buf.getInt(index + HASH_CODE_OFFSET);
                var slotKeySize = buf.getInt(index + KEY_SIZE_OFFSET);
                if (slotHash != hash || slotKeySize != size
                        || !Arrays.equals(keysAsBytes, slotKeyIndex, slotKeyIndex + size, key, offset, offset + size)) {
                    index += ENTRY_SIZE;
                    if (index >= keysAsBytes.length) {
                        index = 0;
                    }
                }
                else {
                    break;
                }
            }
            if (getKeyIndexFrom(index) >= 0) {
                buf.putInt(index + SUM_OFFSET, buf.getInt(index + SUM_OFFSET) + temp);
                buf.putInt(index + COUNT_OFFSET, buf.getInt(index + COUNT_OFFSET) + 1);
                buf.putShort(index + MIN_OFFSET, (short) Math.min(buf.getShort(index + MIN_OFFSET), temp));
                buf.putShort(index + MAX_OFFSET, (short) Math.max(buf.getShort(index + MAX_OFFSET), temp));
            }
            else {
                var keyIndex = addKeyBytes(key, offset, size);
                setKeyIndexTo(index, keyIndex);
                buf.putInt(index + KEY_SIZE_OFFSET, size);
                buf.putInt(index + HASH_CODE_OFFSET, hash);
                buf.putInt(index + SUM_OFFSET, temp);
                buf.putInt(index + COUNT_OFFSET, 1);
                buf.putShort(index + MIN_OFFSET, temp);
                buf.putShort(index + MAX_OFFSET, temp);
                entries++;
            }
        }

        // Get all pairs
        public List<Entry> getAll() {
            List<Tools.Entry> result = new ArrayList<>();
            for (int index = 0; index < buf.capacity(); index += ENTRY_SIZE) {
                var keyIndex = getKeyIndexFrom(index);
                if (keyIndex >= 0) {
                    var keySize = buf.getInt(index + KEY_SIZE_OFFSET);

                    var keyAsString = new String(keysAsBytes, keyIndex, keySize);

                    var sum = buf.getInt(index + SUM_OFFSET);
                    var count = buf.getInt(index + COUNT_OFFSET);
                    var min = buf.getShort(index + MIN_OFFSET);
                    var max = buf.getShort(index + MAX_OFFSET);
                    var materializedEntry = new Tools.Result();
                    materializedEntry.max = max;
                    materializedEntry.min = min;
                    materializedEntry.sum = sum;
                    materializedEntry.count = count;

                    result.add(new Entry(keyAsString, materializedEntry));
                }
            }
            return result;
        }
    }

}
