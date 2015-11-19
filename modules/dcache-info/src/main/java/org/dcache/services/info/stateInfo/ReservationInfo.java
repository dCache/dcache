package org.dcache.services.info.stateInfo;

/**
 * Objects of this Class contain information about an SRM reservation as
 * obtained from dCache state.
 */
public class ReservationInfo
{
    public enum AccessLatency
    {
        ONLINE("ONLINE"), NEARLINE("NEARLINE"), OFFLINE("OFFLINE");

        private final String _metricValue;

        AccessLatency(String metricValue)
        {
            _metricValue = metricValue;
        }

        public String getMetricValue()
        {
            return _metricValue;
        }

        /**
         * Look up the AccessLatency value that matches the given metric
         * value.
         *
         * @param metricValue
         * @return the corresponding AccessLatency, if valid, null otherwise.
         */
        public static AccessLatency parseMetricValue(String metricValue)
        {
            for (AccessLatency al : AccessLatency.values()) {
                if (al.getMetricValue().equals(metricValue)) {
                    return al;
                }
            }
            return null;
        }
    }

    public enum RetentionPolicy
    {
        REPLICA("REPLICA"), OUTPUT("OUTPUT"), CUSTODIAL("CUSTODIAL");

        private final String _metricValue;

        RetentionPolicy(String metricValue)
        {
            _metricValue = metricValue;
        }

        public String getMetricValue()
        {
            return _metricValue;
        }

        /**
         * Look up the RetentionPolicy value that matches the given metric
         * value.
         *
         * @param metricValue
         * @return the corresponding RetentionPolicy, if valid, null
         *         otherwise.
         */
        public static RetentionPolicy parseMetricValue(String metricValue) {
            for (RetentionPolicy rp : RetentionPolicy.values()) {
                if (rp.getMetricValue().equals(metricValue)) {
                    return rp;
                }
            }
            return null;
        }
    }

    public enum State {
        RESERVED("RESERVED", false), RELEASED("RELEASED", true),
        EXPIRED("EXPIRED", true);

        private final String _metricValue;
        private final boolean _isFinalState;

        State(String metricValue, boolean isFinalState) {
            _metricValue = metricValue;
            _isFinalState = isFinalState;
        }

        public String getMetricValue() {
            return _metricValue;
        }

        public boolean isFinalState()
        {
            return _isFinalState;
        }

        /**
         * Look up the State value that matches the given metric value.
         *
         * @param metricValue
         * @return the corresponding State, if valid, null otherwise.
         */
        public static State parseMetricValue(String metricValue)
        {
            for (State state : State.values()) {
                if (state.getMetricValue().equals(metricValue)) {
                    return state;
                }
            }
            return null;
        }
    }

    private RetentionPolicy _rp;
    private AccessLatency _al;
    private State _state;
    private final String _id;
    private long _lifetime;
    private boolean _haveLifetime;
    private String _description;

    private long _total;
    private boolean _haveTotal;

    private long _free;
    private boolean _haveFree;

    private long _allocated;
    private boolean _haveAllocated;

    private long _used;
    private boolean _haveUsed;

    private String _vo;

    public ReservationInfo(String id)
    {
        _id = id;
    }

    public void setRetentionPolicy(RetentionPolicy rp)
    {
        if (hasRetentionPolicy()) {
            throw new IllegalStateException("attempt to set RetentionPolicy twice");
        }

        _rp = rp;
    }

    public RetentionPolicy getRetentionPolicy()
    {
        return _rp;
    }

    public boolean hasRetentionPolicy()
    {
        return _rp != null;
    }

    public void setAccessLatency(AccessLatency al)
    {
        if (hasAccessLatency()) {
            throw new IllegalStateException("attempt to set AccessLatency twice");
        }

        _al = al;
    }

    public AccessLatency getAccessLatency()
    {
        return _al;
    }

    public boolean hasAccessLatency()
    {
        return _al != null;
    }

    public String getId()
    {
        return _id;
    }

    public long getLifetime()
    {
        return _lifetime;
    }

    public void setLifetime(long lifetime)
    {
        if (_haveLifetime) {
            throw new IllegalStateException("attempt to set lifetime twice");
        }

        _lifetime = lifetime;
        _haveLifetime = true;
    }

    public boolean hasLifetime()
    {
        return _haveLifetime;
    }

    public void setDescription(String description)
    {
        if (hasDescription()) {
            throw new IllegalStateException("attempt to set description of reservation " +
                    _id + " twice.");
        }

        _description = description;
    }

    public String getDescription()
    {
        return _description;
    }

    public boolean hasDescription()
    {
        return _description != null;
    }

    public void setVo(String vo)
    {
        if (hasVo()) {
            throw new IllegalStateException("attempt to set VO name of reservation " +
                    _id + " twice.");
        }

        _vo = vo;
    }

    public String getVo()
    {
        return _vo;
    }

    public boolean hasVo()
    {
        return _vo != null;
    }

    public void setState(State state)
    {
        if (hasState()) {
            throw new IllegalStateException("attempt to set state of reservation " +
                    _id + " twice.");
        }

        _state = state;
    }

    public State getState()
    {
        return _state;
    }

    public boolean hasState()
    {
        return _state != null;
    }

    public long getTotal()
    {
        return _total;
    }

    public boolean hasTotal()
    {
        return _haveTotal;
    }

    public void setTotal(long value)
    {
        if (hasTotal()) {
            throw new IllegalStateException("attempt to set total size of reservation " +
                    _id + " twice.");
        }

        _total = value;
        _haveTotal = true;
    }

    public long getFree()
    {
        return _free;
    }

    public boolean hasFree()
    {
        return _haveFree;
    }

    public void setFree(long value)
    {
        if (hasFree()) {
            throw new IllegalStateException("attempt to set free size of reservation " +
                    _id + " twice.");
        }

        _free = value;
        _haveFree = true;
    }

    public long getAllocated()
    {
        return _allocated;
    }

    public boolean hasAllocated()
    {
        return _haveAllocated;
    }

    public void setAllocated(long value)
    {
        if (hasAllocated()) {
            throw new IllegalStateException("attempt to set allocated size of reservation " +
                    _id + " twice.");
        }

        _allocated = value;
        _haveAllocated = true;
    }

    public long getUsed()
    {
        return _used;
    }

    public boolean hasUsed()
    {
        return _haveUsed;
    }

    public void setUsed(long value)
    {
        if (hasUsed()) {
            throw new IllegalStateException("attempt to set used size of reservation " +
                    _id + " twice.");
        }

        _used = value;
        _haveUsed = true;
    }
}
