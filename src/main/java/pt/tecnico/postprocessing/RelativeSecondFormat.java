package pt.tecnico.postprocessing;

import java.text.DateFormat;
import java.text.FieldPosition;
import java.text.ParsePosition;
import java.util.Date;

/**
 * Relative format in second
 */
public class RelativeSecondFormat extends DateFormat {

    private final long baseMillis;

    public RelativeSecondFormat(long baseMillis) {
        this.baseMillis = baseMillis;
    }

    @Override
    public StringBuffer format(Date date, StringBuffer stringBuffer, FieldPosition fieldPosition) {
        long elapse = (date.getTime() - baseMillis) / 1000;
        stringBuffer.append(elapse);
        return stringBuffer;
    }

    @Override
    public Date parse(String s, ParsePosition parsePosition) {
        return null;
    }
}
