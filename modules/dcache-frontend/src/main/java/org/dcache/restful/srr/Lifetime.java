package org.dcache.restful.srr;

import java.util.HashMap;
import java.util.Map;

public class Lifetime {

    private String _default;
    private String maximum;
    private Expiration expiration;

    public String getDefault() {
        return _default;
    }

    public void setDefault(String _default) {
        this._default = _default;
    }

    public Lifetime withDefault(String _default) {
        this._default = _default;
        return this;
    }

    public String getMaximum() {
        return maximum;
    }

    public void setMaximum(String maximum) {
        this.maximum = maximum;
    }

    public Lifetime withMaximum(String maximum) {
        this.maximum = maximum;
        return this;
    }

    public Expiration getExpiration() {
        return expiration;
    }

    public void setExpiration(Expiration expiration) {
        this.expiration = expiration;
    }

    public Lifetime withExpiration(Expiration expiration) {
        this.expiration = expiration;
        return this;
    }

    public enum Expiration {

        RELEASE("release"),
        WARN("warn"),
        NEVER("never");
        private final String value;
        private final static Map<String, Expiration> CONSTANTS = new HashMap<String, Expiration>();

        static {
            for (Expiration c : values()) {
                CONSTANTS.put(c.value, c);
            }
        }

        Expiration(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return this.value;
        }

        public String value() {
            return this.value;
        }

        public static Expiration fromValue(String value) {
            Expiration constant = CONSTANTS.get(value);
            if (constant == null) {
                throw new IllegalArgumentException(value);
            } else {
                return constant;
            }
        }

    }

}
