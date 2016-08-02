package diskCacheV111.vehicles;

import java.util.StringTokenizer;
//
//  Flush Control
//

public class PoolFlushDoFlushMessage extends PoolFlushControlInfoMessage {

    private static final long serialVersionUID = 2092239799703855551L;

    private final String  _hsmName;
    private final String  _storageClassName;
    private int     _maxFlushCount;
    private long    _flushId;
    private int     _totalFlushed;
    private int     _totalFailed;
    private boolean _finished;

    public PoolFlushDoFlushMessage( String poolName , String composedStorageClass ){
        super(poolName);
        StringTokenizer st = new StringTokenizer( composedStorageClass , "@" ) ;
        _storageClassName = st.nextToken() ;
        _hsmName = st.nextToken();
        setReplyRequired(true);
    }
    public PoolFlushDoFlushMessage(String poolName , String hsmName , String storageClassName ){
         super(poolName);
         _hsmName = hsmName ;
         _storageClassName = storageClassName ;
         setReplyRequired(true);
    }
    public String getHsmName(){ return _hsmName ; }
    public String getStorageClassName(){ return _storageClassName ; }
    public void setMaxFlushCount( int count ){ _maxFlushCount = count ; }
    public int  getMaxFlushCount(){ return _maxFlushCount ; }
    public void setFlushId( long flushId ){ _flushId = flushId ; }
    public long getFlushId(){ return _flushId ; }
    public void setResult( int totalFlushed , int totalFailed ){
       _totalFlushed = totalFlushed ;
       _totalFailed  = totalFailed ;
       _finished     = true ;
    }
    public int totalFailed(){ return _totalFailed ; }
    public int totalFlushed(){ return _totalFlushed ; }
    public String toString(){
       StringBuilder sb = new StringBuilder() ;
       sb.append(super.toString()).append(";SC=").
          append(_storageClassName).append('@').append(_hsmName).
          append(";MAX=").append(_maxFlushCount).append(";id=").
          append(_flushId) ;
       if( _finished ) {
           sb.append(";total=").append(_totalFlushed).
                   append(";failed=").append(_totalFailed);
       }
       return sb.toString();
    }
    public boolean isFinished(){ return _finished ; }
}

