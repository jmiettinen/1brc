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
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class CalculateAverage_jmiettinen {
    private static final String FILE = "./measurements.txt";

    private static final int MIN_SEGMENT_SIZE = 1024 * 1024;

    /*
     * My results on this computer:
     *
     * CalculateAverage: 2m37.788s
     * CalculateAverage_royvanrijn: 0m29.639s
     * CalculateAverage_spullara: 0m2.013s
     *
     */

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        long start = System.currentTimeMillis();
        var filename = args.length == 0 ? FILE : args[0];
        var file = new File(filename);

        var resultsMap = getFileSegments(file).stream().map(segment -> {
            var resultMap = new Tools.ByteArrayToResultMap();
            long segmentEnd = segment.end();
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
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).parallel().flatMap(partition -> partition.getAll().stream())
                .collect(
                        Collectors.toMap(
                                e -> new String(e.key()),
                                Tools.Entry::value,
                                CalculateAverage_jmiettinen::merge,
                                TreeMap::new));

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

    private static Tools.Result merge(Tools.Result v, short min, short max, int sum, long count) {
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

        Result(short value) {
            min = max = value;
            sum = value;
            count = 1;
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

    record Entry(byte[] key, Tools.Result value) {
    }

    record FileSegment(long start, long end) {
    }

    static class ByteArrayToResultMap {
        public static final int MAP_SIZE = 1024 * 128;
        final Tools.Result[] slots = new Tools.Result[MAP_SIZE];

        final int[] hashCodes = new int[MAP_SIZE];
        final byte[][] keys = new byte[MAP_SIZE][];

        public void putOrMerge(byte[] key, int offset, int size, short temp, int hash) {
            int slot = hash & (slots.length - 1);
            var slotValue = slots[slot];
            // Linear probe for open slot
            while (slotValue != null && (hashCodes[slot] != hash || keys[slot].length != size || !Arrays.equals(keys[slot], 0, size, key, offset, size))) {
                slot = (slot + 1) & (slots.length - 1);
                slotValue = slots[slot];
            }
            Tools.Result value = slotValue;
            if (value == null) {
                slots[slot] = new Tools.Result(temp);
                byte[] bytes = new byte[size];
                System.arraycopy(key, offset, bytes, 0, size);
                keys[slot] = bytes;
                hashCodes[slot] = hash;
            }
            else {
                value.min = (short) Math.min(value.min, temp);
                value.max = (short) Math.max(value.max, temp);
                value.sum += temp;
                value.count += 1;
            }
        }

        // Get all pairs
        public List<Tools.Entry> getAll() {
            List<Tools.Entry> result = new ArrayList<>(slots.length);
            for (int i = 0; i < slots.length; i++) {
                Tools.Result slotValue = slots[i];
                if (slotValue != null) {
                    result.add(new Tools.Entry(keys[i], slotValue));
                }
            }
            return result;
        }
    }

}
