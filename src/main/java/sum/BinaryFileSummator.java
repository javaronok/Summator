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
 * Class calculate the sum of values stored in a user-specified binary file.
 * Path to file there is required argument for program
 * 
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

  /**
   * Calculate sum integer values of the file with specified parameters. 
   * 
   * @param threads - number of threads 
   * @param bufferLength - file buffer length
   * @return calculation result
   * @throws IOException
   * @throws ExecutionException
   * @throws InterruptedException
   */
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

  /**
   * Iterator returns sequence of seek positions in file for reading chunks.  
   */
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

  /**
   * Calculation chunk size for effective parallel read 
   * 
   * @param size - amount file size
   * @param threads - number of threads
   * @param bufferLength - file buffer length
   * @return chunk size
   */
  private long calcChuckSize(long size, int threads, int bufferLength) {
    return (long)Math.ceil((double)size/(threads * bufferLength)) * bufferLength;
  }

  /**
   * Calculation integer values containing in chunk. 
   * Method can be called from different threads in parallel mode.
   * File is reading from specified seek position for size bytes 
   * 
   * @param file - source file
   * @param seekPosition - start seek position
   * @param chunkSize - size of reading bytes
   * @param bufferLength - file buffer length 
   * @return calculation result for chunk
   */
  private long calcChunk(Path file, long seekPosition, long chunkSize, int bufferLength) {
    long accumulator = 0;
    long readBytes = 0;
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
    } catch (IOException e) {
      throw new RuntimeException("IO error:", e);
    }
    return accumulator;
  }
}
