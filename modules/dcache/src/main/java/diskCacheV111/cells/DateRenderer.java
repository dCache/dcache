package diskCacheV111.cells;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.antlr.stringtemplate.AttributeRenderer;

public class DateRenderer implements AttributeRenderer {

    private Map<String, DateFormat> formatMap = new ConcurrentHashMap<String, DateFormat>();
    private static final String DEFAULT_FORMAT_PATTERN = "MM.dd HH:mm:ss";

    /**
     * Default constructor
     */
    public DateRenderer() {
        formatMap.put(DEFAULT_FORMAT_PATTERN, new SimpleDateFormat(DEFAULT_FORMAT_PATTERN));
    }

    /**
     * The DEFAULT pattern is used, if no format provided
     */
    @Override
    public String toString(Object object) {
        return toString(object, DEFAULT_FORMAT_PATTERN);
    }

    /**
     * Formats a date according to the given pattern.
     */
    @Override
    public String toString(Object object, String format) {
        DateFormat dateFormat = formatMap.get(format);
        if (dateFormat == null) {
            dateFormat = new SimpleDateFormat(format);
            formatMap.put(format, dateFormat);
        }
        return dateFormat.format((Date) object);
    }
}
