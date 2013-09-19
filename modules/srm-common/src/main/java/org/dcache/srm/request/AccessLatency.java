package org.dcache.srm.request;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

import org.dcache.srm.v2_2.TAccessLatency;

public enum AccessLatency {
    ONLINE            (TAccessLatency.ONLINE),
    NEARLINE            (TAccessLatency.NEARLINE);

    private final TAccessLatency _latency;
    private static final ImmutableMap<TAccessLatency,AccessLatency> MAP;
    private static final String ERROR_MESSAGE;

    private AccessLatency(TAccessLatency latency) {
        _latency = latency;
    }

    static {
        StringBuilder sb = new StringBuilder();
        sb.append("Unknown AccessLatency: \"%s\".");
        sb.append(" Supported values :");

        Builder<TAccessLatency,AccessLatency> builder =
            new Builder<>();
        for (AccessLatency value : values()) {
                builder.put(value._latency,value);
                sb.append(" \"").append(value._latency).append("\"");
        }
        MAP = builder.build();
        ERROR_MESSAGE = sb.toString();
    }

    public TAccessLatency toTAccessLatency() {
        return _latency;
    }

    public static AccessLatency fromTAccessLatency(TAccessLatency latency) {
       if ( latency == null ) {
            return null;
        }
        else {
            return MAP.get(latency);
        }
    }

    /**
     *  this function provides wrapper of TAccessLatency.fromString
     *  so that user gets better error handling
     */
    public static AccessLatency fromString(String txt)
            throws IllegalArgumentException  {
            try {
                TAccessLatency latency = TAccessLatency.fromString(txt);
                return fromTAccessLatency(latency);
            }
            catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(String.format(ERROR_MESSAGE,
                                                                 txt));
            }
    }
}
