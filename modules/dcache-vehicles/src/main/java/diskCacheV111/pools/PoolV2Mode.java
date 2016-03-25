package diskCacheV111.pools;


import java.io.Serializable;

public class PoolV2Mode implements Serializable {

    private static final long serialVersionUID = -3620447515380724292L;

    public static final int ENABLED                 = 0x00 ;
    public static final int DISABLED                = 0x01 ;
    public static final int DISABLED_FETCH          = 0x02 ;
    public static final int DISABLED_STORE          = 0x04 ;
    public static final int DISABLED_STAGE          = 0x08 ;
    public static final int DISABLED_P2P_CLIENT     = 0x10 ;
    public static final int DISABLED_P2P_SERVER     = 0x20 ;
    public static final int DISABLED_DEAD           = 0x40 ;
    public static final int DISABLED_STRICT         =
                                     DISABLED |
                                     DISABLED_FETCH |
                                     DISABLED_STORE |
                                     DISABLED_STAGE |
                                     DISABLED_P2P_CLIENT |
                                     DISABLED_P2P_SERVER;
    public static final int DISABLED_RDONLY         =
                                     DISABLED |
                                     DISABLED_STORE |
                                     DISABLED_STAGE |
                                     DISABLED_P2P_CLIENT ;

    private static final int RESILIENCE_ENABLED = 0x7F ;
    private static final int RESILIENCE_DISABLED = 0x80 ;

    private static final String [] __modeString = {
       "fetch" , "store" , "stage" , "p2p-client" , "p2p-server"  , "dead"
    } ;

    private int _mode = ENABLED ;

    /*
     *  For convenience.
     */
    public synchronized boolean isResilienceEnabled() {
        return !((_mode & RESILIENCE_DISABLED) == RESILIENCE_DISABLED);
    }

    public synchronized void setResilienceEnabled(boolean isResilienceEnabled) {
        if (isResilienceEnabled) {
            _mode &= RESILIENCE_ENABLED;
        } else {
            _mode |= RESILIENCE_DISABLED;
        }
    }

    @Override
    public String toString() {
        int mode = getMode();

        if (isEnabled(mode)) {
            if (isResilienceEnabled()) {
                return "enabled";
            }
            return "enabled,noresilience";
        }

        StringBuilder sb = new StringBuilder();
        sb.append("disabled(");
        boolean first = true;
        for (String modeString: __modeString) {
            mode >>= 1;
            if ((mode & 1) != 0) {
                if (first) {
                    first = false;
                } else {
                    sb.append(',');
                }
                sb.append(modeString);
            }
        }
        if (!isResilienceEnabled()) {
            sb.append(",noresilience");
        }
        sb.append(")");
        return sb.toString();
    }

    public PoolV2Mode() {
        this(ENABLED);
    }

    public PoolV2Mode(int mode) {
        _mode = mode;
    }

    public synchronized void setMode(int mode) {
        if(isEnabled(mode)) {
            _mode = mode;
        } else {
            _mode = mode | DISABLED;
        }
    }

    public synchronized int getMode() {
        return _mode;
    }

    public synchronized boolean isDisabled(int mask) {
        return (_mode & mask) == mask;
    }

    public synchronized boolean isDisabled() {
        return !isEnabled(_mode);
    }

    public synchronized boolean isEnabled() {
        return isEnabled(_mode);
    }

    @Override
    public synchronized boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof PoolV2Mode)) {
            return false;
        }

        return ((PoolV2Mode) obj)._mode == this._mode;
    }

    @Override
    public synchronized int hashCode() {
        return _mode;
    }

    private static boolean isEnabled(int mode) {
        return mode == ENABLED || mode == (ENABLED| RESILIENCE_DISABLED);
    }
}
