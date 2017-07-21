import org.junit.Assert;
import org.junit.Test;
import sum.BinaryFileSummator;
import sum.BinaryIntFileWriter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutionException;

public class BinaryFileSummatorTest {
    @Test
    public void testSimple() throws IOException, ExecutionException, InterruptedException {
        Path testFile = Paths.get("data/simple.txt");
        long sum = new BinaryFileSummator(testFile).calcSum(2, 16);
        Assert.assertEquals(15, sum);
    }

    @Test
    public void testTempRandomFile() throws IOException, ExecutionException, InterruptedException {
        Path tempFile = Files.createTempFile("tempfiles", ".tmp");
        int packagesCount = 1 + (int)(Math.random() * 1000);
        new BinaryIntFileWriter(tempFile).write(packagesCount);
        long sum = new BinaryFileSummator(tempFile).calcSum(2, 16);
        tempFile.toFile().deleteOnExit();
        Assert.assertEquals(packagesCount*15, sum);
    }
}
