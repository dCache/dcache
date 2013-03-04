package org.dcache.srm.request;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

import org.dcache.srm.v2_2.TFileStorageType;

public enum FileStorageType {
    VOLATILE             (TFileStorageType.VOLATILE),
    DURABLE           (TFileStorageType.DURABLE),
    PERMANENT           (TFileStorageType.PERMANENT);

    private final TFileStorageType _type;
    private static final ImmutableMap<TFileStorageType,FileStorageType> MAP;
    private static final String ERROR_MESSAGE;

    private FileStorageType(TFileStorageType type) {
        _type = type;
    }

    static {
        StringBuilder sb = new StringBuilder();
        sb.append("Unknown FileStorageType: \"%s\".");
        sb.append(" Supported values :");

        Builder<TFileStorageType,FileStorageType> builder =
            new Builder<>();
        for (FileStorageType value : values()) {
                builder.put(value._type,value);
                sb.append(" \"").append(value._type).append("\"");
        }
        MAP = builder.build();
        ERROR_MESSAGE = sb.toString();
    }

    public TFileStorageType toTFileStorageType() {
        return _type;
    }

    public static FileStorageType fromTFileStorageType(TFileStorageType type) {
        if ( type == null ) {
            return null;
        }
        else {
            return MAP.get(type);
        }
    }

    /**
     * this function provides wrapper of TFileStorageType.fromString
     *  so that user gets better error handling
     */
    public static FileStorageType fromString(String txt)
            throws IllegalArgumentException  {
            try {
                TFileStorageType type = TFileStorageType.fromString(txt);
                return fromTFileStorageType(type);
            }
            catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(String.format(ERROR_MESSAGE,
                                                                 txt));
            }
        }
}
