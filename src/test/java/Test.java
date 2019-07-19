import org.apache.commons.configuration.XMLConfiguration;

import static java.lang.System.out;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.*;

public class Test {

    public static void main(String[] args) throws Exception {
        out.println((1563508800000L + 8 * 60 * 60 * 1000) % (24 * 60 * 60 * 1000));
        out.println((1563508800000L) % (24 * 60 * 60 * 1000));
        out.println(new SimpleDateFormat("HH:mm").parse("12:00").getTime() + 8 * 60 * 60 * 1000);
    }
}
