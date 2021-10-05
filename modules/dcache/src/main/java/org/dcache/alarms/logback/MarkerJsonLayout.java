package org.dcache.alarms.logback;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.contrib.json.classic.JsonLayout;
import java.util.Map;
import org.dcache.alarms.LogEntry;


public class MarkerJsonLayout extends JsonLayout {


    public static final String CONTEXT_MARKER = "marker";


    public MarkerJsonLayout() {
        super();
    }


    @Override
    protected void addCustomDataToJsonMap(Map<String, Object> map, ILoggingEvent event) {

        LoggingEventConverter logginEventConv = new LoggingEventConverter();

        LogEntry entry = logginEventConv.createEntryFromEvent(event);

        map.put(CONTEXT_MARKER, entry);


    }


}
