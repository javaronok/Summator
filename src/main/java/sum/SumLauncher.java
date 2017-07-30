package sum;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.concurrent.ExecutionException;

/**
 * This class contains entry point for launch file sum operation from command line interface (CLI)  
 */
public class SumLauncher {

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        int argsLength = args.length;

        if (argsLength == 0) {
            printUsage();
            System.exit(1);
        }

        if ("-help".equals(args[0])) {
          printUsage();
          System.exit(0);
        }

        Path file = Paths.get(args[0]);

        int threads = Runtime.getRuntime().availableProcessors();
        int bufferLength = BinaryFileSummator.DEFAULT_BUFFER_LENGTH;

        boolean isDebug = false;

        for (int i = 1; i < argsLength; i++) {
            final String arg = args[i];
            if ("-threads".equals(arg) && ++i < argsLength) {
                threads = Integer.valueOf(args[i]);
            } if ("-bufferLength".equals(arg) && ++i < argsLength) {
                bufferLength = Integer.valueOf(args[i]);
            } if ("-debug".equals(arg) && ++i <= argsLength) {
                isDebug = true;
            }
        }

        if (isDebug)
          System.out.println("Available processors: " + threads);

        long time = new Date().getTime();
        long result = new BinaryFileSummator(file).calcSum(threads, bufferLength);

        if (isDebug) {
          System.out.print("Result: ");
        }
        System.out.println(result);

        if (isDebug)  
          System.out.println("Execute time: " + (new Date().getTime() - time) + " ms");
    }

    public static void printUsage() {
        System.out.printf("Summator usage:" +
                "\n" +
                "<file path> (required, example data/simple.txt)\n" +
                "-threads <number of processors> (optional)\n" +
                "-bufferLength <length of buffer> (optional, must be multiple of 4), default: 8192\n" +
                "-debug debug mode, default: false\n" +
                "-help\n" +
                "");
    }
}
