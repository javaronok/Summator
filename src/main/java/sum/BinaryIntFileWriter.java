package sum;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;

/**
 * User: Gorchakov Dmitriy
 * Date: 02.07.2017.
 */
public class BinaryIntFileWriter {
  public static void main(String[] args) throws IOException {
    if (args.length != 1) {
      printUsage();
      System.exit(1);
    }

    Path file = Paths.get(args[0]);

    try (SeekableByteChannel channel = Files.newByteChannel(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
      for (int it = 0; it < 1000000; it++) {
        ByteBuffer buffer = ByteBuffer.allocate(20);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        for (int i = 1; i <= 5; i++) {
          buffer.putInt(i);
        }
        buffer.flip();
        channel.write(buffer);
      }  
    }

    long size = Files.size(file);
    System.out.println(size);
  }

  public static void printUsage() {
    System.out.println("Enter file path");
  }  
}
