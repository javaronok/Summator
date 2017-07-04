package sum;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

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

    if (chunkSize % ChunkReader.BUFFER_LENGTH != 0)
      chunkSize = (chunkSize/ChunkReader.BUFFER_LENGTH)*ChunkReader.BUFFER_LENGTH + ChunkReader.BUFFER_LENGTH; 

    ExecutorService executor = Executors.newFixedThreadPool(4);
    List<Future<Long>> futureResults = new LinkedList<>();
  
    for (long pos = 0; pos < size; pos += chunkSize) {
      futureResults.add(executor.submit(new ChunkReader(file, pos, chunkSize)));
    }

    executor.shutdown();    

    for (Future<Long> f : futureResults) {
      accumulator += f.get();
    }

    System.out.println(accumulator);
    System.out.println("Time: " + (new Date().getTime() - time) + " ms");
  }

  public static void printUsage() {
    System.out.println("Enter file path");
  }
  
  static class ChunkReader implements Callable<Long> {
    private static final int BUFFER_LENGTH = 8192;
    
    final Path file;
    final long seekPosition;
    final long chunkSize;

    public ChunkReader(Path file, long seekPosition, long chunkSize) {
      this.file = file;
      this.seekPosition = seekPosition;
      this.chunkSize = chunkSize;
    }

    @Override
    public Long call() throws Exception {
      long accumulator = 0;
      long readBytes = 0;
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
      return accumulator;
    }
  } 
}
