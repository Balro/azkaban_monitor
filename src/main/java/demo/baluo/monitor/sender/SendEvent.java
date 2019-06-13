package demo.baluo.monitor.sender;

public class SendEvent {
    public String id;
    public String title;
    public String msg;
    public String detail;

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("SendEvent:id[");
        sb.append(id);
        sb.append("],title[");
        sb.append(title);
        sb.append("],msg[");
        sb.append(msg);
        sb.append("],detail[");
        sb.append(detail);
        sb.append("]");
        return sb.toString();
    }
}
