package org.dcache.srm.request;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

import org.dcache.srm.v2_2.TRetentionPolicy;

public enum RetentionPolicy {
    REPLICA             (TRetentionPolicy.REPLICA),
    OUTPUT              (TRetentionPolicy.OUTPUT),
    CUSTODIAL           (TRetentionPolicy.CUSTODIAL);

    private final TRetentionPolicy _policy;
    private static final ImmutableMap<TRetentionPolicy,RetentionPolicy> MAP;
    private static final String ERROR_MESSAGE;

    private RetentionPolicy(TRetentionPolicy policy) {
        _policy = policy;
    }

    public TRetentionPolicy toTRetentionPolicy() {
        return _policy;
    }

    static {
        StringBuilder sb = new StringBuilder();
        sb.append("Unknown RetentionPolicy: \"%s\".");
        sb.append(" Supported values :");

        Builder<TRetentionPolicy,RetentionPolicy> builder =
            new Builder<>();
        for (RetentionPolicy value : values()) {
                builder.put(value._policy,value);
                sb.append(" \"").append(value._policy).append("\"");
        }
        MAP = builder.build();
        ERROR_MESSAGE = sb.toString();
    }


    public static RetentionPolicy fromTRetentionPolicy(TRetentionPolicy policy) {
        if ( policy == null ) {
            return null;
        }
        else {
            return MAP.get(policy);
        }
    }

    /**
     *  this function provides wrapper of TRetentionPolicy.fromString
     *  so that user gets better error handling
     */
    public static RetentionPolicy fromString(String txt)
            throws IllegalArgumentException  {
            try {
                TRetentionPolicy policy = TRetentionPolicy.fromString(txt);
                return fromTRetentionPolicy(policy);
            }
            catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(String.format(ERROR_MESSAGE,
                                                                 txt));
            }
    }
}
