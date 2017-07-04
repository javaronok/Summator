package sum;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * User: Gorchakov Dmitriy
 * Date: 01.07.2017.
 */
public class BinaryFileSummator {

  public static void main(String[] args) throws IOException {
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

    try (SeekableByteChannel channel = Files.newByteChannel(file)) {
      ByteBuffer buffer = ByteBuffer.allocate(8);
      buffer.order(ByteOrder.LITTLE_ENDIAN);

      int count = channel.read(buffer);
      while (count != -1) {
        buffer.rewind();

        int position = 0;
        while (position < count) {
          accumulator += buffer.getInt();
          position = buffer.position();
        }
        buffer.clear();
        count = channel.read(buffer);
      }
    }

    System.out.println(accumulator);
  }

  public static void printUsage() {
    System.out.println("Enter file path");
  }
}
