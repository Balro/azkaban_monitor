import org.apache.zookeeper.CreateMode;

import java.io.PrintStream;
import java.util.Arrays;

public class Test {
    private static PrintStream OUT = System.out;
    public static void main(String[] args) {
        String s = String.format("create %s, data %s, mode %s", "/root", "123", CreateMode.PERSISTENT);
        System.out.println(s);
    }
}
