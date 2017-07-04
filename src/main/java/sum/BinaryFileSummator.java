package sum;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Date;
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

  public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
    if (args.length != 1) {
      printUsage();
      System.exit(1);
    }

    Path file = Paths.get(args[0]);
    long size = Files.size(file);

    if (size % 4 != 0) {
      throw new IllegalArgumentException("Not supported file format");
    }

    long time = new Date().getTime();

    long accumulator = 0;
    int threads = Runtime.getRuntime().availableProcessors();
    long chunkSize = size/threads;

    System.out.println("Available processors: " + threads);

    if (chunkSize % ChunkReaderTask.BUFFER_LENGTH != 0) {
      chunkSize = (chunkSize / ChunkReaderTask.BUFFER_LENGTH) * ChunkReaderTask.BUFFER_LENGTH + ChunkReaderTask.BUFFER_LENGTH;
    }

    ForkJoinPool pool = new ForkJoinPool(threads);
    List<ForkJoinTask<Long>> tasks = new ArrayList<>();
    for (long pos = 0; pos < size; pos += chunkSize) {
      tasks.add(pool.submit(new ChunkReaderTask(file, pos, chunkSize)));
    }

    for (ForkJoinTask<Long> t : tasks) {
      accumulator += t.join();
    }

    System.out.println(accumulator);
    System.out.println("Time: " + (new Date().getTime() - time) + " ms");
  }

  public static void printUsage() {
    System.out.println("Enter file path");
  }
  
  static class ChunkReaderTask extends RecursiveTask<Long> {
    private static final int BUFFER_LENGTH = 8192;
    
    final Path file;
    final long seekPosition;
    final long chunkSize;

    public ChunkReaderTask(Path file, long seekPosition, long chunkSize) {
      this.file = file;
      this.seekPosition = seekPosition;
      this.chunkSize = chunkSize;
    }

    @Override
    public Long compute() {
      long accumulator = 0;
      long readBytes = 0;
      try {
        try (SeekableByteChannel channel = Files.newByteChannel(file, StandardOpenOption.READ)) {
          ByteBuffer buffer = ByteBuffer.allocate(BUFFER_LENGTH);
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
