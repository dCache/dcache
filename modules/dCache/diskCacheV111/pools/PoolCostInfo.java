// $Id: PoolCostInfo.java,v 1.7 2007-07-26 13:43:32 tigran Exp $

package diskCacheV111.pools ;
import com.google.common.collect.Maps;
import java.util.* ;
import org.dcache.pool.classic.IoQueueManager;

public class PoolCostInfo implements java.io.Serializable {

    static final long serialVersionUID = 5181562551679185500L;

    private PoolQueueInfo _store = null  , _restore = null  ,
                          _mover = null  , _p2p     = null ,
                          _p2pClient = null ;
    private Map<String, NamedPoolQueueInfo>           _extendedMoverHash = null ;
    private final String  _defaultQueueName;
    private PoolSpaceInfo _space  = null ;
    private final String  _poolName ;

    public PoolCostInfo(String poolName)
    {
        _poolName = poolName;
        _defaultQueueName = IoQueueManager.DEFAULT_QUEUE;
    }

    public String getPoolName(){ return _poolName ; }
    public class NamedPoolQueueInfo extends PoolQueueInfo {

        static final long serialVersionUID = -7097362707394583875L;

        private String _name = null ;
        private NamedPoolQueueInfo(String name, int active, int maxActive, int queued) {
            super(active, maxActive, queued);
            _name = name;
        }

        private NamedPoolQueueInfo(String name, PoolQueueInfo queue) {
            this(name, queue.getActive(), queue.getMaxActive(), queue.getQueued());
        }

        public String getName(){ return _name ; }
        @Override
        public String toString(){
           return _name+"={"+super.toString()+"}" ;
        }
    }
    public class PoolQueueInfo implements java.io.Serializable {

        static final long serialVersionUID = 1304697767284208011L;

        private int _active    = 0;
        private int _maxActive =0 ;
        private int _queued    = 0 ;

        private PoolQueueInfo( int active , int maxActive , int queued ){
           _active    = active ;
           _maxActive = maxActive ;
           _queued    = queued ;

        }
        @Override
        public String toString(){ return "a="+_active+";m="+_maxActive+";q="+_queued ; }
        public int getActive(){ return _active ; }
        public int getMaxActive(){ return _maxActive ; }
        public int getQueued(){ return _queued ; }
        public void modifyQueue( int diff ){

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

    public class PoolSpaceInfo implements java.io.Serializable {

    	static final long serialVersionUID = -8966065301943351970L;



        private long _total = 0 , _free = 0 , _precious = 0 , _removable = 0 , _lru = 0 ;
        private long _gap   = 0 ;
        private double _breakEven = 0;

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
    public void setQueueSizes( int moverActive   , int moverMaxActive   , int moverQueued ,
                               int restoreActive , int restoreMaxActive , int restoreQueued ,
                               int storeActive   , int storeMaxActive   , int storeQueued        ){

       _mover   = new PoolQueueInfo( moverActive , moverMaxActive , moverQueued ) ;
       _restore = new PoolQueueInfo( restoreActive , restoreMaxActive , restoreQueued ) ;
       _store   = new PoolQueueInfo( storeActive , storeMaxActive , storeQueued ) ;

    }
    public void addExtendedMoverQueueSizes( String name , int moverActive   , int moverMaxActive   , int moverQueued ){
       if( _extendedMoverHash == null ){
           _extendedMoverHash = new HashMap<String, NamedPoolQueueInfo>() ;
       }
       _extendedMoverHash.put( name, new NamedPoolQueueInfo( name, moverActive, moverMaxActive, moverQueued ));
    }
    public Map<String, NamedPoolQueueInfo> getExtendedMoverHash(){ return _extendedMoverHash ; }
    public String getDefaultQueueName(){ return _defaultQueueName ; }
    public void setP2pServerQueueSizes( int p2pActive     , int p2pMaxActive     , int p2pQueued  ){
       _p2p = new PoolQueueInfo( p2pActive   , p2pMaxActive   , p2pQueued ) ;
    }
    public void setP2pClientQueueSizes( int p2pClientActive     , int p2pClientMaxActive     , int p2pClientQueued ){
       _p2pClient = new PoolQueueInfo( p2pClientActive   , p2pClientMaxActive   , p2pClientQueued ) ;
    }
    @Override
    public String toString() {
       StringBuffer sb = new StringBuffer() ;

       sb.append(_poolName).append("={R={").append(_restore.toString()).
          append("};S={").append(_store.toString()).
          append("};M={").append(_mover.toString()) ;
       if( _p2p != null )sb.append("};PS={").append(_p2p.toString()) ;
       if( _p2pClient != null )sb.append("};PC={").append(_p2pClient.toString()) ;
       sb.append("};SP={").append(_space.toString()).append("};");
       if( _extendedMoverHash != null ){
           sb.append("XM={");
           for( NamedPoolQueueInfo namedPoolQueueInfo : _extendedMoverHash.values() ){
               sb.append( namedPoolQueueInfo.toString() ).append(";");
           }
           sb.append("};");
       }
       sb.append("}");

       return sb.toString();
    }



}
