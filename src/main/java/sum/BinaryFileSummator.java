package sum;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;

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

    long accumulator = 0;
    long chunkSize = calcChuckSize(size, threads, bufferLength);

    ForkJoinPool pool = new ForkJoinPool(threads);
    List<ForkJoinTask<Long>> tasks = new ArrayList<>();
    for (long pos = 0; pos < size; pos += chunkSize) {
      tasks.add(pool.submit(new ChunkReaderTask(file, pos, chunkSize, bufferLength)));
    }

    for (ForkJoinTask<Long> t : tasks) {
      accumulator += t.join();
    }

    return accumulator;
  }

  private long calcChuckSize(long size, int threads, int bufferLength) {
    return (long)Math.ceil((double)size/(threads * bufferLength)) * bufferLength;
  }

  static class ChunkReaderTask extends RecursiveTask<Long> {
    final Path file;
    final long seekPosition;
    final long chunkSize;
    final int bufferLength;

    public ChunkReaderTask(Path file, long seekPosition, long chunkSize, int bufferLength) {
      this.file = file;
      this.seekPosition = seekPosition;
      this.chunkSize = chunkSize;
      this.bufferLength = bufferLength;
    }

    @Override
    public Long compute() {
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
}
