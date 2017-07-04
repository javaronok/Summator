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
    long accumulator = 0;

    String path = "data/simple.txt";

    Path file = Paths.get(path);

    long size = Files.size(file);

    System.out.println(size);

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
}
