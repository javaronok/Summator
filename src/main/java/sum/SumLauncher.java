package sum;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.concurrent.ExecutionException;

public class SumLauncher {

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        if (args.length != 1) {
            printUsage();
            System.exit(1);
        }

        Path file = Paths.get(args[0]);
        int threads = Runtime.getRuntime().availableProcessors();
        System.out.println("Available processors: " + threads);

        long time = new Date().getTime();
        long result = new BinaryFileSummator(file).calcSum();

        System.out.println("Result: " + result);
        System.out.println("Execute time: " + (new Date().getTime() - time) + " ms");
    }

    public static void printUsage() {
        System.out.println("Enter file path");
    }
}
