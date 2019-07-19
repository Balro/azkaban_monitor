package balro.monitor.azkaban.checker;

public interface ShouldCheckable {
    boolean shouldCheck(long start, long end);
}
