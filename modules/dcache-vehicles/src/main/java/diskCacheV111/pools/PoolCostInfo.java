package diskCacheV111.pools;

import javax.annotation.Nonnull;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class PoolCostInfo implements Serializable {

    private static final long serialVersionUID = 5181562551679185500L;

    private PoolQueueInfo _store;
    private PoolQueueInfo _restore;
    private PoolQueueInfo _p2p;
    private PoolQueueInfo _p2pClient;
    private final Map<String, NamedPoolQueueInfo> _extendedMoverHash = new HashMap<>();
    private final String  _defaultQueueName;
    private PoolSpaceInfo _space;
    private final String  _poolName ;
    private double _moverCostFactor;

    public PoolCostInfo(String poolName, String defaultQueue)
    {
        _poolName = poolName;
        _defaultQueueName = checkNotNull(defaultQueue);
        _space = new PoolSpaceInfo(0, 0, 0, 0);
    }

    public String getPoolName(){ return _poolName ; }
    public static class NamedPoolQueueInfo extends PoolQueueInfo {

        private static final long serialVersionUID = -7097362707394583875L;

        private final String _name;
        private NamedPoolQueueInfo(String name,
                                   int active, int maxActive, int queued,
                                   int readers, int writers)
        {
            super(active, maxActive, queued, readers, writers);
            _name = name;
        }

        public String getName(){ return _name ; }
        @Override
        public String toString(){
           return _name + "={" + super.toString() + '}';
        }
    }
    public static class PoolQueueInfo implements Serializable {

        private static final long serialVersionUID = 1304697767284208011L;

        private int _active;
        private final int _maxActive;
        private int _queued;
        private final int _readers;
        private final int _writers;

        private PoolQueueInfo(int active, int maxActive, int queued, int readers, int writers)
        {
           _active = active;
           _maxActive = maxActive;
           _queued = queued;
           _readers = readers;
           _writers = writers;
        }

        @Override
        public String toString(){ return "a="+_active+";m="+_maxActive+";q="+_queued + ";r=" + _readers + ";w=" + _writers; }
        public int getActive(){ return _active ; }
        public int getMaxActive(){ return _maxActive ; }
        public int getQueued(){ return _queued ; }

        public int getReaders()
        {
            return _readers;
        }

        public int getWriters()
        {
            return _writers;
        }

        public void modifyQueue(int diff)
        {
            int total = Math.max(0, _active + _queued + diff);

           _active = Math.min( total , _maxActive ) ;

           _queued = Math.max( 0 , total - _maxActive ) ;
        }
    }
    public PoolQueueInfo getStoreQueue(){ return _store ; }
    public PoolQueueInfo getRestoreQueue(){ return _restore ; }
    public PoolQueueInfo getP2pQueue(){ return _p2p ; }
    public PoolQueueInfo getP2pClientQueue(){ return _p2pClient ; }

    public PoolQueueInfo getMoverQueue()
    {
        int moverActive = 0, moverMaxActive = 0, queued = 0, readers = 0, writers = 0;
        for (NamedPoolQueueInfo queue: _extendedMoverHash.values()) {
            moverActive += queue.getActive();
            moverMaxActive += queue.getMaxActive();
            queued += queue.getQueued();
            readers += queue.getReaders();
            writers += queue.getWriters();
        }
        return new PoolQueueInfo(moverActive, moverMaxActive, queued, readers, writers);
    }

    public PoolSpaceInfo getSpaceInfo(){ return _space ; }

    public static class PoolSpaceInfo implements Serializable {

    	private static final long serialVersionUID = -8966065301943351970L;

        private final long _total;

        private long _free;

        private final long _precious;

        private final long _removable;

        private final long _lru;
        private long _gap;
        private double _breakEven;

        private PoolSpaceInfo( long total , long free , long precious , long removable ){
            this(total, free, precious, removable, 0);
        }

        private PoolSpaceInfo( long total , long free , long precious , long removable , long lru ){

            if( total < free ) {
                throw new IllegalArgumentException("total >= free");
            }

            if( total < precious ) {
                throw new IllegalArgumentException("total >= precious");
            }

            if( total < removable ) {
                throw new IllegalArgumentException("total >= removable");
            }

           _total     = total ;
           _free      = free ;
           _precious  = precious ;
           _removable = removable ;
           _lru       = lru ;
        }
        public void setParameter( double breakEven , long gap ){
           _breakEven = breakEven ;
           _gap       = gap ;
        }
        @Override
        public String toString(){
           return "t=" + _total +
                  ";f=" + _free +
                  ";p=" + _precious +
                  ";r=" + _removable +
                  ";lru=" + _lru +
                  ";{g=" + _gap + ";b=" + _breakEven + '}';
        }
        public long getFreeSpace(){ return _free ; }
        public long getTotalSpace(){ return _total ; }
        public long getPreciousSpace(){ return _precious ; }
        public long getRemovableSpace(){ return _removable ; }
        public long getUsedSpace(){ return _total - _free ; }
        public long getGap(){ return _gap ; }
        public double getBreakEven(){ return _breakEven ; }
        public long getLRUSeconds(){ return _lru ; }

        public void modifyPinnedSpace(long diff)
        {
            _free = Math.max(0, Math.min(_free - diff, _total - _removable - _precious));
        }
    }

    //
    /// the setters
    //
    public void setSpaceUsage( long total , long free , long precious , long removable ){
        _space = new PoolSpaceInfo( total , free , precious , removable ) ;
    }
    public void setSpaceUsage( long total , long free , long precious , long removable , long lru ){
        _space = new PoolSpaceInfo( total , free , precious , removable , lru ) ;
    }

    public void
        setQueueSizes(int restoreActive, int restoreMaxActive, int restoreQueued,
                      int storeActive, int storeMaxActive, int storeQueued)
    {
        _restore = new PoolQueueInfo(restoreActive, restoreMaxActive,
                                     restoreQueued, 0,
                                     restoreActive + restoreQueued);
        _store   = new PoolQueueInfo(storeActive, storeMaxActive, storeQueued,
                                     storeActive + storeQueued, 0);
    }

    public void addExtendedMoverQueueSizes(String name, int moverActive,
                                           int moverMaxActive, int moverQueued,
                                           int moverReaders, int moverWriters)
    {
        NamedPoolQueueInfo info =
            new NamedPoolQueueInfo(name, moverActive, moverMaxActive,
                                   moverQueued, moverReaders, moverWriters);
        _extendedMoverHash.put(name, info);
    }

    @Nonnull
    public Map<String, NamedPoolQueueInfo> getExtendedMoverHash()
    {
        return _extendedMoverHash;
    }

    @Nonnull
    public String getDefaultQueueName()
    {
        return _defaultQueueName;
    }

    public void setP2pServerQueueSizes(int p2pActive, int p2pMaxActive, int p2pQueued)
    {
        _p2p = new PoolQueueInfo(p2pActive, p2pMaxActive, p2pQueued,
                                 p2pActive + p2pQueued, 0);
    }

    public void setP2pClientQueueSizes(int p2pClientActive, int p2pClientMaxActive, int p2pClientQueued)
    {
        _p2pClient = new PoolQueueInfo(p2pClientActive, p2pClientMaxActive,
                                       p2pClientQueued, 0,
                                       p2pClientActive + p2pClientQueued);
    }

    public void setMoverCostFactor(double moverCostFactor)
    {
        _moverCostFactor = moverCostFactor;
    }

    public double getMoverCostFactor()
    {
        return _moverCostFactor;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(_poolName).append("={");
        if (_restore != null) {
            sb.append("R={").append(_restore).append("};");
        }
        if (_store != null) {
            sb.append("S={").append(_store).append("};");
        }
        if (_p2p != null) {
           sb.append("PS={").append(_p2p).append("};");
        }
        if (_p2pClient != null) {
           sb.append("PC={").append(_p2pClient).append("};");
        }
        sb.append("SP={").append(_space.toString()).append("};");
        sb.append("XM={");
        for( PoolQueueInfo namedPoolQueueInfo : _extendedMoverHash.values() ){
            sb.append( namedPoolQueueInfo.toString() ).append(';');
        }
        sb.append("};");
        sb.append('}');
        return sb.toString();
    }
}
