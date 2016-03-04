package diskCacheV111.cells;

import com.google.common.collect.Maps;
import org.stringtemplate.v4.AttributeRenderer;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Map;

public class DateRenderer implements AttributeRenderer
{
    private final Map<String, DateFormat> formatMap = Maps.newHashMap();
    private static final String DEFAULT_FORMAT_PATTERN = "MM.dd HH:mm:ss";

    /**
     * Default constructor
     */
    public DateRenderer() {
        formatMap.put(DEFAULT_FORMAT_PATTERN, new SimpleDateFormat(DEFAULT_FORMAT_PATTERN));
    }

    /**
     * Formats a date according to the given pattern.
     */
    @Override
    public synchronized String toString(Object object, String format, Locale locale) {
        if (format == null) {
            format = DEFAULT_FORMAT_PATTERN;
        }
        DateFormat dateFormat = formatMap.get(format);
        if (dateFormat == null) {
            dateFormat = new SimpleDateFormat(format);
            formatMap.put(format, dateFormat);
        }
        return dateFormat.format((Date) object);
    }
}
