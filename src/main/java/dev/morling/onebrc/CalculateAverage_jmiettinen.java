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
import static dev.morling.onebrc.Tools.FlyweightResult.ENTRY_SIZE;

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

    static class FlyweightResult {

        static final int KEY_INDEX_OFFSET = 0;
        static final int HASH_CODE_OFFSET = KEY_INDEX_OFFSET + Integer.BYTES;
        static final int KEY_SIZE_OFFSET = HASH_CODE_OFFSET + Integer.BYTES;
        static final int SUM_OFFSET = KEY_SIZE_OFFSET + Integer.BYTES;
        static final int COUNT_OFFSET = SUM_OFFSET + Integer.BYTES;
        static final int MIN_OFFSET = COUNT_OFFSET + Integer.BYTES;
        static final int MAX_OFFSET = MIN_OFFSET + Short.BYTES;

        static final int ENTRY_SIZE = MAX_OFFSET + Short.BYTES;

        private final ByteBuffer buf;
        private final byte[] bytes;
        int offset;

        FlyweightResult(byte[] buffer, int offset) {
            this.offset = offset;
            this.buf = ByteBuffer.wrap(buffer);
            this.bytes = buffer;
            buf.order(ByteOrder.nativeOrder());
            buf.limit(buf.capacity());
        }

        public boolean isSet() {
            return getKeyIndex() >= 0;
        }

        public void setIndex(int newIndex) {
            offset = ENTRY_SIZE * newIndex;
        }

        private int getRelativeInt(int valueOffset) {
            return buf.getInt(offset + valueOffset);
        }

        private void setRelativeInt(int valueOffset, int value) {
            buf.putInt(offset + valueOffset, value);
        }

        private void setRelativeShort(int valueOffset, short value) {
            buf.putShort(offset + valueOffset, value);
        }

        private short getRelativeShort(int valueOffset) {
            return buf.getShort(offset + valueOffset);
        }

        private int getKeyIndex() {
            return ~getRelativeInt(KEY_INDEX_OFFSET);
        }

        private void setKeyIndex(int index) {
            setRelativeInt(KEY_INDEX_OFFSET, ~index);
        }

        private void setKeySize(int size) {
            setRelativeInt(KEY_SIZE_OFFSET, size);
        }

        private int getKeySize() {
            return getRelativeInt(KEY_SIZE_OFFSET);
        }

        private void setHash(int hash) {
            setRelativeInt(HASH_CODE_OFFSET, hash);
        }

        private int getHash() {
            return getRelativeInt(HASH_CODE_OFFSET);
        }

        private void setSum(int sum) {
            setRelativeInt(SUM_OFFSET, sum);
        }

        private int getSum() {
            return getRelativeInt(SUM_OFFSET);
        }

        private void setCount(int count) {
            setRelativeInt(COUNT_OFFSET, count);
        }

        private int getCount() {
            return getRelativeInt(COUNT_OFFSET);
        }

        private void setMin(short min) {
            setRelativeShort(MIN_OFFSET, min);
        }

        private short getMin() {
            return getRelativeShort(MIN_OFFSET);
        }

        private void setMax(short max) {
            setRelativeShort(MAX_OFFSET, max);
        }

        private short getMax() {
            return getRelativeShort(MAX_OFFSET);
        }

        public String toString() {
            return String.format("""
                    key_index: %d
                    key_size: %d
                    hash: %d
                    sum: %d
                    count: %d
                    min: %d
                    max: %d
                    """,
                    getKeyIndex(), getKeySize(), getHash(), getSum(), getCount(), getMin(), getMax());
        }

    }

    static class ByteArrayToResultMap {
        public static final int MAP_SIZE = 1024 * 64;

        final byte[] valuesAsBytes = new byte[MAP_SIZE * ENTRY_SIZE];
        byte[] keysAsBytes = new byte[256];

        private int freeIndex = 0;
        private int entries = 0;

        private final FlyweightResult view;

        ByteArrayToResultMap() {
            view = new FlyweightResult(valuesAsBytes, 0);
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
            view.setIndex(indexUnscaled);
            while (view.isSet()) {
                var slotKeyIndex = view.getKeyIndex();
                var slotHash = view.getHash();
                var slotKeySize = view.getKeySize();
                if (slotHash != hash || slotKeySize != size
                        || !Arrays.equals(keysAsBytes, slotKeyIndex, slotKeyIndex + size, key, offset, offset + size)) {
                    view.offset += ENTRY_SIZE;
                    if (view.offset >= keysAsBytes.length) {
                        view.offset = 0;
                    }
                }
                else {
                    break;
                }
            }
            if (view.isSet()) {
                view.setSum(view.getSum() + temp);
                view.setCount(view.getCount() + 1);
                view.setMin((short) Math.min(view.getMin(), temp));
                view.setMax((short) Math.max(view.getMax(), temp));
            }
            else {
                var keyIndex = addKeyBytes(key, offset, size);
                view.setKeyIndex(keyIndex);
                view.setKeySize(size);
                view.setHash(hash);
                view.setSum(temp);
                view.setCount(1);
                view.setMin(temp);
                view.setMax(temp);
                entries++;
            }
        }

        // Get all pairs
        public List<Entry> getAll() {
            List<Tools.Entry> result = new ArrayList<>();
            for (int i = 0; i < MAP_SIZE; i++) {
                view.setIndex(i);
                if (view.isSet()) {
                    var keyAsString = new String(keysAsBytes, view.getKeyIndex(), view.getKeySize());
                    var materializedEntry = new Tools.Result();
                    materializedEntry.max = view.getMax();
                    materializedEntry.min = view.getMin();
                    materializedEntry.sum = view.getSum();
                    materializedEntry.count = view.getCount();

                    result.add(new Entry(keyAsString, materializedEntry));
                }
            }
            return result;
        }
    }

}
