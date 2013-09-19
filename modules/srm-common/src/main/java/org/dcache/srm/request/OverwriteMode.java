package org.dcache.srm.request;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

import org.dcache.srm.v2_2.TOverwriteMode;

public enum OverwriteMode {
    NEVER            (TOverwriteMode.NEVER),
    ALWAYS            (TOverwriteMode.ALWAYS),
    WHEN_FILES_ARE_DIFFERENT           (TOverwriteMode.WHEN_FILES_ARE_DIFFERENT);

    private final TOverwriteMode _mode;
    private static final ImmutableMap<TOverwriteMode,OverwriteMode> MAP;
    private static final String ERROR_MESSAGE;

    private OverwriteMode(TOverwriteMode mode) {
        _mode = mode;
    }

    static {
        StringBuilder sb = new StringBuilder();
        sb.append("Unknown OverwriteMode: \"%s\".");
        sb.append(" Supported values :");

        Builder<TOverwriteMode,OverwriteMode> builder =
            new Builder<>();
        for (OverwriteMode value : values()) {
                builder.put(value._mode,value);
                sb.append(" \"").append(value._mode).append("\"");
        }
        MAP = builder.build();
        ERROR_MESSAGE = sb.toString();
    }


    public TOverwriteMode toTOverwriteMode() {
        return _mode;
    }

    public static OverwriteMode fromTOverwriteMode(TOverwriteMode mode) {
        if ( mode == null ) {
            return null;
        }
        else {
            return MAP.get(mode);
        }
    }

    /**
     * this function provides wrapper of TOverwriteMode.fromString
     *  so that user gets better error handling
     */
    public static OverwriteMode fromString(String txt)
        throws IllegalArgumentException  {
        try {
            TOverwriteMode mode = TOverwriteMode.fromString(txt);
            return fromTOverwriteMode(mode);
        }
        catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(String.format(ERROR_MESSAGE,
                                                             txt));
        }
    }
}
