package demo.baluo.monitor.sender;

import java.io.PrintStream;
import java.util.LinkedList;
import java.util.List;

public class SenderTest {
    private static PrintStream out = System.out;

    public static void main(String[] args) throws Exception {
        SenderTest at = new SenderTest();
        at.test();
    }

    private void test() throws Exception {
        SenderOnealert so = new SenderOnealert("fe6171f2-6c64-a89a-ddc0-8392eee550db");

        SendEvent se = new SendEvent();
        se.id = "12345";
        se.title = "测试告警";
        se.msg = "发生测试告警";
        se.detail = "详细内容";

        SendEvent se2 = new SendEvent();
        se2.id = "123123";
        se2.title = "测试告警";
        se2.msg = "发生测试告警";
        se2.detail = "详细内容";

        List<SendEvent> ses = new LinkedList<>();
        ses.add(se);
        ses.add(se2);
        so.send(ses);
    }
}
