// $Id: PoolCostInfo.java,v 1.7 2007-07-26 13:43:32 tigran Exp $

package diskCacheV111.pools ;

import com.google.common.collect.Maps;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.dcache.pool.classic.IoQueueManager;

public class PoolCostInfo implements Serializable {

    private static final long serialVersionUID = 5181562551679185500L;

    private PoolQueueInfo _store, _restore,
                          _mover, _p2p,
                          _p2pClient;
    private Map<String, NamedPoolQueueInfo>           _extendedMoverHash;
    private final String  _defaultQueueName;
    private PoolSpaceInfo _space;
    private final String  _poolName ;
    private double _moverCostFactor;

    public PoolCostInfo(String poolName)
    {
        _poolName = poolName;
        _defaultQueueName = IoQueueManager.DEFAULT_QUEUE;
        _space = new PoolSpaceInfo(0, 0, 0, 0);
    }

    public String getPoolName(){ return _poolName ; }
    public class NamedPoolQueueInfo extends PoolQueueInfo {

        private static final long serialVersionUID = -7097362707394583875L;

        private String _name;
        private NamedPoolQueueInfo(String name,
                                   int active, int maxActive, int queued,
                                   int readers, int writers)
        {
            super(active, maxActive, queued, readers, writers);
            _name = name;
        }

        private NamedPoolQueueInfo(String name, PoolQueueInfo queue) {
            this(name, queue.getActive(), queue.getMaxActive(), queue.getQueued(), queue.getReaders(), queue.getWriters());
        }

        public String getName(){ return _name ; }
        @Override
        public String toString(){
           return _name+"={"+super.toString()+"}" ;
        }
    }
    public class PoolQueueInfo implements Serializable {

        private static final long serialVersionUID = 1304697767284208011L;

        private int _active;
        private int _maxActive;
        private int _queued;
        private int _readers;
        private int _writers;

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
            /* For pre 1.9.14 pools we estimate the number of readers.
             */
            if (_readers + _writers > 0 || _active + _queued == 0) {
                return _readers;
            } else if (this == _p2pClient) {
                return 0;
            } else if (this == _p2p) {
                return _active + _queued;
            } else if (this == _store) {
                return _active + _queued;
            } else if (this == _restore) {
                return 0;
            } else {
                return (_active + _queued + 1) / 2;
            }
        }

        public int getWriters()
        {
            /* For pre 1.9.14 pools we estimate the number of writers.
             */
            if (_readers + _writers > 0 || _active + _queued == 0) {
                return _writers;
            } else if (this == _p2pClient) {
                return _active + _queued;
            } else if (this == _p2p) {
                return 0;
            } else if (this == _store) {
                return 0;
            } else if (this == _restore) {
                return _active + _queued;
            } else {
                return (_active + _queued) / 2;
            }
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
    public PoolQueueInfo getMoverQueue(){ return _mover ; }
    public PoolQueueInfo getP2pQueue(){ return _p2p ; }
    public PoolQueueInfo getP2pClientQueue(){ return _p2pClient ; }
    public PoolSpaceInfo getSpaceInfo(){ return _space ; }

    public Map<String, NamedPoolQueueInfo> getMoverQueues() {
        Map<String, NamedPoolQueueInfo> moverQueues;
        if (_extendedMoverHash != null) {
            moverQueues = Maps.newHashMap(_extendedMoverHash);
        } else {
            moverQueues = Maps.newHashMap();
        }

        if (_store != null) {
            moverQueues.put("Stores", new NamedPoolQueueInfo("Stores", _store));
        }
        if (_restore != null) {
            moverQueues.put("Restores", new NamedPoolQueueInfo("Restores", _restore));
        }
        if (_mover != null) {
            moverQueues.put("Movers", new NamedPoolQueueInfo("Movers", _mover));
        }
        if (_p2p != null) {
            moverQueues.put("P2P-Server", new NamedPoolQueueInfo("P2P-Server", _p2p));
        }
        if (_p2pClient != null) {
            moverQueues.put("P2P-Client", new NamedPoolQueueInfo("P2P-Client", _p2pClient));
        }
        return moverQueues;
    }

    public class PoolSpaceInfo implements Serializable {

    	private static final long serialVersionUID = -8966065301943351970L;



        private long _total, _free, _precious, _removable, _lru;
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
           return "t="+_total+
                  ";f="+_free+
                  ";p="+_precious+
                  ";r="+_removable+
                  ";lru="+_lru+
                  ";{g="+_gap+";b="+_breakEven+"}" ;
        }
        public long getFreeSpace(){ return _free ; }
        public long getTotalSpace(){ return _total ; }
        public long getPreciousSpace(){ return _precious ; }
        public long getRemovableSpace(){ return _removable ; }
        public long getUsedSpace(){ return _total - _free ; }
        public long getPinnedSpace(){ return _total - _free - _precious - _removable ; }
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
        setQueueSizes(int moverActive, int moverMaxActive, int moverQueued,
                      int restoreActive, int restoreMaxActive, int restoreQueued,
                      int storeActive, int storeMaxActive, int storeQueued)
    {
        _mover = new PoolQueueInfo(moverActive, moverMaxActive, moverQueued, 0, 0);
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
        if (_extendedMoverHash == null) {
            _extendedMoverHash = new HashMap<>();
        }
        _extendedMoverHash.put(name, info);
    }

    public Map<String,NamedPoolQueueInfo> getExtendedMoverHash()
    {
        return _extendedMoverHash;
    }

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

    public int getWriters()
    {
        int writers = 0;
        if (_store != null) {
            writers += _store.getWriters();
        }
        if (_restore != null) {
            writers += _restore.getWriters();
        }
        if (_p2p != null) {
            writers += _p2p.getWriters();
        }
        if (_p2pClient != null) {
            writers += _p2pClient.getWriters();
        }
        if (_extendedMoverHash != null) {
            for (PoolQueueInfo info: _extendedMoverHash.values()) {
                writers += info.getWriters();
            }
        } else {
            if (_mover != null) {
                writers += _mover.getWriters();
            }
        }
        return writers;
    }

    @Override
    public String toString() {
       StringBuilder sb = new StringBuilder() ;

       sb.append(_poolName).append("={R={").append(_restore.toString()).
          append("};S={").append(_store.toString()).
          append("};M={").append(_mover.toString()) ;
       if( _p2p != null ) {
           sb.append("};PS={").append(_p2p.toString());
       }
       if( _p2pClient != null ) {
           sb.append("};PC={").append(_p2pClient.toString());
       }
       sb.append("};SP={").append(_space.toString()).append("};");
       if( _extendedMoverHash != null ){
           sb.append("XM={");
           for( PoolQueueInfo namedPoolQueueInfo : _extendedMoverHash.values() ){
               sb.append( namedPoolQueueInfo.toString() ).append(";");
           }
           sb.append("};");
       }
       sb.append("}");

       return sb.toString();
    }

    private void readObject(ObjectInputStream stream)
        throws IOException, ClassNotFoundException
    {
        /* For compatibility with pre 1.9.14 pools we inject a default
         * value for the mover cost factor. REVISIT: Can be removed
         * after the third golden release.
         */
        _moverCostFactor = 0.5;
        stream.defaultReadObject();
    }
}
