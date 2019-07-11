package baluo.monitor.azkaban;

public class AzkabanMonitor2 {
    public static boolean RUN = true;

    public static void main(String[] args) {
        switch (args[0]) {
            case "start":
                start();
                break;
            case "stop":
                stop();
                break;
            case "status":
                status();
                break;
            default:
                System.err.println("Unknown command " + args[0]);
        }
    }

    private static void start() {
        System.out.println("start");
    }

    private static void stop() {
        System.out.println("stop");
    }

    private static void status() {
        System.out.println("status");
    }
}
