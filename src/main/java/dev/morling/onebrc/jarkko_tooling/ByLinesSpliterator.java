package dev.morling.onebrc.jarkko_tooling;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Spliterator;
import java.util.function.Consumer;

public class ByLinesSpliterator implements Spliterator<ByteBuffer> {

    private ByteBuffer bb;
    private boolean advanced;

    public ByLinesSpliterator(FileChannel channel) throws IOException {
        this(channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size()));
    }

    private ByLinesSpliterator(ByteBuffer bb) {
        this.bb = bb;
        advanced = false;
    }


    @Override
    public boolean tryAdvance(Consumer<? super ByteBuffer> action) {
        if (advanced) {
            return false;
        } else {
            advanced = true;
            action.accept(this.bb);
            return true;
        }
    }

    @Override
    public ByLinesSpliterator trySplit() {
        var middle = bb.limit() / 2;
        int midL = middle;
        int midR = middle;
        while (midL > 0) {
            if (bb.get(midL) == '\n') {
                break;
            }
            midL--;
        }
        if (midL < 0) {
            while (midR < bb.limit()) {
                if (bb.get(midR) == '\n') {
                    break;
                }
                midR++;
            }
        }
        if (midL > 0 || midR < bb.limit()) {
            // [1,2,3,4,5,6,7] -> [1,2,3,4], [5,6,7]
            int leftLength = midL > 0 ? midL + 1 : midR;
            int rightStart = midL > 0 ? midL : midR;
            int rightLength = bb.limit() - leftLength;
            ByteBuffer left = bb.slice(0, leftLength);
            ByteBuffer right = bb.slice(rightStart, rightLength);
            this.bb = left;
            return new ByLinesSpliterator(right);
        } else {
            return null;
        }
    }

    @Override
    public long estimateSize() {
        return Long.MAX_VALUE;
    }

    @Override
    public int characteristics() {
        return NONNULL |IMMUTABLE;
    }

    public static void main(String[] args) throws IOException {
        var fc = FileChannel.open(Path.of("src/test/resources/samples/measurements-10000-unique-keys.txt"), StandardOpenOption.READ);
        var firstIter = new ByLinesSpliterator(fc);
        var iters = new ArrayList<ByLinesSpliterator>();
        iters.add(firstIter);
        boolean oneDidSplit = false;
        do {
            var size = iters.size();
            for (int i = 0; i < size; i++) {
                var maybeNewIter = iters.get(i).trySplit();
                if (maybeNewIter != null) {
                    oneDidSplit = true;
                    iters.add(maybeNewIter);
                }
            }
        } while(oneDidSplit);
        System.out.println(iters.size());
    }
}

