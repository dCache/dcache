/*
 * $Id$
 */


package diskCacheV111.pools;


public class PoolV2Mode implements java.io.Serializable {

    private static final long serialVersionUID = -3620447515380724292L;

    public static final int ENABLED             = 0x00 ;
    public static final int DISABLED            = 0x01 ;
    public static final int DISABLED_FETCH      = 0x02 ;
    public static final int DISABLED_STORE      = 0x04 ;
    public static final int DISABLED_STAGE      = 0x08 ;
    public static final int DISABLED_P2P_CLIENT = 0x10 ;
    public static final int DISABLED_P2P_SERVER = 0x20 ;
    public static final int DISABLED_DEAD       = 0x40 ;
    public static final int DISABLED_STRICT     =
                                     DISABLED |
                                     DISABLED_FETCH |
                                     DISABLED_STORE |
                                     DISABLED_STAGE |
                                     DISABLED_P2P_CLIENT |
                                     DISABLED_P2P_SERVER ;
    public static final int DISABLED_RDONLY     =
                                     DISABLED |
                                     DISABLED_STORE |
                                     DISABLED_STAGE |
                                     DISABLED_P2P_CLIENT ;

    private static final String [] __modeString = {
       "fetch" , "store" , "stage" , "p2p-client" , "p2p-server"  , "dead"
    } ;
    private int _mode = ENABLED ;

    public String toString() {
        if (_mode == ENABLED)
            return "enabled";

        StringBuilder sb = new StringBuilder();
        sb.append("disabled(");
        boolean first = true;
        for (int i = 0, mode = _mode; i < __modeString.length; i++) {
            mode >>= 1;
            if ((mode & 1) != 0) {
                if (first) {
                    first = false;
                } else {
                    sb.append(',');
                }
                sb.append(__modeString[i]);
            }
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
        if( mode == ENABLED ) {
            _mode = ENABLED;
        }else{
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
        return _mode != ENABLED;
    }

    public synchronized boolean isEnabled() {
        return _mode == ENABLED;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (!(obj instanceof PoolV2Mode))
            return false;

        return ((PoolV2Mode) obj)._mode == this._mode;
    }

    @Override
    public int hashCode() {
        return _mode;
    }
}
