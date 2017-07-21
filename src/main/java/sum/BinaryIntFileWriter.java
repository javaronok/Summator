package sum;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import java.util.Date;

/**
 * User: Gorchakov Dmitriy
 * Date: 02.07.2017.
 */
public class BinaryIntFileWriter {
  private final Path file;

  public BinaryIntFileWriter(Path file) {
    this.file = file;
  }

  public long write(long packageCount) throws IOException {
    try (SeekableByteChannel channel = Files.newByteChannel(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
      for (int it = 0; it < packageCount; it++) {
        ByteBuffer buffer = ByteBuffer.allocate(20);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        for (int i = 1; i <= 5; i++) {
          buffer.putInt(i);
        }
        buffer.flip();
        channel.write(buffer);
      }
    }

    return Files.size(file);
  }

  public static void main(String[] args) throws IOException {
    if (args.length != 2) {
      printUsage();
      System.exit(1);
    }

    Path file = Paths.get(args[0]);
    Long count = Long.valueOf(args[1]);

    long time = new Date().getTime();    

    long size = new BinaryIntFileWriter(file).write(count);
    System.out.println(size);
    System.out.println("Time: " + (new Date().getTime() - time) + " ms");
  }

  public static void printUsage() {
    System.out.println("Enter file path and packages count");
  }  
}
