package sum;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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

    long accumulator = 0;
    int chunkSize = 16;

    ExecutorService executor = Executors.newFixedThreadPool(4);
    List<Future<Long>> futureResults = new LinkedList<>();

    for (long pos = 0; pos < size; pos += chunkSize) {
      futureResults.add(executor.submit(new ChunkReader(file, pos, chunkSize)));
    }

    for (Future<Long> f : futureResults) {
      accumulator += f.get();
    }

    executor.shutdown();

    System.out.println(accumulator);
  }

  public static void printUsage() {
    System.out.println("Enter file path");
  }

  static class ChunkReader implements Callable<Long> {
    final Path file;
    final long seekPosition;
    final int chunkSize;

    public ChunkReader(Path file, long seekPosition, int chunkSize) {
      this.file = file;
      this.seekPosition = seekPosition;
      this.chunkSize = chunkSize;
    }

    @Override
    public Long call() throws Exception {
      long accumulator = 0;
      try (SeekableByteChannel channel = Files.newByteChannel(file)) {
        ByteBuffer buffer = ByteBuffer.allocate(chunkSize);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        channel.position(seekPosition);

        int count = channel.read(buffer);

        buffer.rewind();

        int position = 0;
        while (position < count) {
          accumulator += buffer.getInt();
          position = buffer.position();
        }
      }
      return accumulator;
    }
  } 
}
