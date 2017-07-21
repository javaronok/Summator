package sum;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.StreamSupport;

/**
 * User: Gorchakov Dmitriy
 * Date: 01.07.2017.
 */
public class BinaryFileSummator {
  public static final int DEFAULT_BUFFER_LENGTH = 8192;

  private final Path file;

  public BinaryFileSummator(Path file) {
    this.file = file;
  }

  public long calcSum() throws InterruptedException, ExecutionException, IOException {
    return calcSum(Runtime.getRuntime().availableProcessors(), DEFAULT_BUFFER_LENGTH);
  }

  public long calcSum(int threads, int bufferLength) throws IOException, ExecutionException, InterruptedException {
    long size = Files.size(file);
    if (size % 4 != 0) {
      throw new IllegalArgumentException("Not supported file format");
    }
    if (bufferLength == 0 || bufferLength % 4 != 0) {
      throw new IllegalArgumentException("Invalid buffer length");
    }

    if (threads == 0) {
      throw new IllegalArgumentException("Invalid threads number");
    }

    long chunkSize = calcChuckSize(size, threads, bufferLength);

    Iterator<Long> it = new SeekPositionIterator(size, chunkSize);
    long itSize = (long) Math.ceil((double) size / chunkSize);
    int characteristics = Spliterator.DISTINCT | Spliterator.ORDERED | Spliterator.NONNULL;
    return StreamSupport.stream(Spliterators.spliterator(it, itSize, characteristics), false).parallel()
            .mapToLong(pos -> calcChunk(file, pos, chunkSize, bufferLength)).sum();
  }

  private class SeekPositionIterator implements Iterator<Long> {
    final long size;
    final long chunkSize;

    private long pos = 0;

    public SeekPositionIterator(long size, long chunkSize) {
      this.size = size;
      this.chunkSize = chunkSize;
    }

    @Override
    public boolean hasNext() {
      return pos < size;
    }

    @Override
    public Long next() {
      long next = pos;
      pos += chunkSize;
      return next;
    }
  }

  private long calcChuckSize(long size, int threads, int bufferLength) {
    return (long)Math.ceil((double)size/(threads * bufferLength)) * bufferLength;
  }

  private long calcChunk(Path file, long seekPosition, long chunkSize, int bufferLength) {
    long accumulator = 0;
    long readBytes = 0;
    try {
      try (SeekableByteChannel channel = Files.newByteChannel(file, StandardOpenOption.READ)) {
        ByteBuffer buffer = ByteBuffer.allocate(bufferLength);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        channel.position(seekPosition);

        int count = channel.read(buffer);
        while (readBytes < chunkSize && count != -1) {
          buffer.rewind();

          int position = 0;
          while (position < count) {
            accumulator += buffer.getInt();
            position = buffer.position();
          }
          buffer.clear();
          readBytes += count;
          count = channel.read(buffer);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("IO error:", e);
    }
    return accumulator;
  }
}
