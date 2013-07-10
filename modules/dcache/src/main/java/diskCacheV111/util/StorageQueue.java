// $Id: StorageQueue.java,v 1.10 2002-02-19 20:19:38 cvs Exp $

package diskCacheV111.util ;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import diskCacheV111.vehicles.PoolClassAttraction;
import diskCacheV111.vehicles.StorageInfo;

public class StorageQueue {

    private String    _poolName;
    private HashMap<String, StorageClassInfo> _storageQueue      = new HashMap<>() ;
    private long      _defaultExpiration = 4 * 3600 * 1000 ;
    private int       _defaultPending    = 3 ;
    private int       _defaultPreference = -1 ;

    public static final int  DEFAULT_EXPIRATION  = -1 ;
    public static final long DEFAULT_PENDING     = -1 ;
    public static final int  DEFAULT_PREFERENCES = -1 ;

    public class StorageRequestInfo {
       private String      _pnfsId ;
       private StorageInfo _info ;
       private long        _time;
       private StorageRequestInfo( String pnfsId , StorageInfo info ){
          this( pnfsId , info , -1 ) ;
       }
       private StorageRequestInfo( String pnfsId , StorageInfo info , long time ){
          _pnfsId = pnfsId ;
          _info   = info ;
          _time   = time < 0 ? ( new Date().getTime() ) : time ;
       }
       public long getTime(){ return _time ; }
       public String getPnfsId(){ return _pnfsId ; }
       public StorageInfo getStorageInfo(){ return _info ; }
       public String toString(){
           return "pnfsid="+_pnfsId+";info="+_info ;
       }
    }
    public class StorageClassInfo {

       private long      _time;
       private Hashtable<String, StorageRequestInfo> _requests = new Hashtable<>() ;
       private String    _name;
       private long      _expiration = _defaultExpiration ;
       private int       _pending    = _defaultPending ;
       private boolean   _defined;
       private int       _writePreference = _defaultPreference ;
       private int       _readPreference  = _defaultPreference ;
       private String    _hsmName         = "cache" ;
       private StorageClassInfo( String hsmName , String storageClass ){
          _name = storageClass ;
          _hsmName = hsmName.toLowerCase() ;
       }
       public String getHsm(){ return _hsmName ; }
       public String getName(){ return _name ; }
       public Enumeration<StorageRequestInfo> requests(){ return _requests.elements() ; }
       public String toString(){
         StringBuilder sb = new StringBuilder() ;
         sb.append("SCI=").append(_name).
            append("@").append(_hsmName).
            append(";def=").append(_defined).
            append(";exp=").append(_expiration/1000).
            append(";pend=").append(_pending).
            append(";readpref=").append(_readPreference).
            append(";writepref=").append(_writePreference).
            append(";waiting=").append(_requests.size()) ;
         return sb.toString() ;
       }
       public boolean hasExpired(){
           return ( _time + _expiration ) > new Date().getTime() ;
       }
       public long expiresIn(){ return  (new Date().getTime() - ( _time + _expiration ) ) / 1000 ; }
       public boolean isFull(){
           return _requests.size() >= _pending ;
       }
       public boolean isTriggered(){
           return hasExpired() || isFull() ;
       }
       public String getStorageClass(){ return _name ; }
       private void setTime( long time ){ _time = time ; }
       public long getTime(){ return _time ; }
       private void setDefined( boolean d ){ _defined = d ; }
       public boolean isDefined(){ return _defined ; }
       public Enumeration<StorageRequestInfo> getRequests(){ return _requests.elements() ; }
       private void addRequest( StorageRequestInfo request ){
           _requests.put( request.getPnfsId() , request ) ;
           if( _time == 0L || ( request.getTime() < _time ) ){
              _time = request.getTime() ;
           }
       }
       public Iterator<String> classes(){ return _storageQueue.keySet().iterator() ; }
       public int  size(){ return _requests.size() ; }
       private void removeRequest( String pnfsId ){
          _requests.remove( pnfsId ) ;
       }
       public void setExpiration( int expiration){
          _expiration = expiration * 1000  ;
       }
       public void setPending( int pending ){
          _pending = pending ;
       }
       public void setWritePreference( int preference ){
          _writePreference = preference ;
       }
       public void setReadPreference( int preference ){
          _readPreference = preference ;
       }
       public int  getWritePreference(){ return _writePreference ; }
       public int  getReadPreference(){ return _readPreference ; }
       public int  getPending(){ return _pending ; }
       public int  getExpiration(){ return (int)(_expiration / 1000); }
    }
    public StorageQueue( String poolName ){
       _poolName = poolName ;
    }
    public void printSetup( PrintWriter pw ){
        for (Object o : _storageQueue.values()) {
            StorageClassInfo classInfo = (StorageClassInfo) o;
            if (classInfo.isDefined()) {
                pw.println("define class " + classInfo
                        .getHsm() + " " + classInfo.getStorageClass() +
                        " -pending=" + classInfo.getPending() +
                        " -expire=" + classInfo.getExpiration() +
                        " -writepref=" + classInfo.getWritePreference() +
                        " -readpref=" + classInfo.getReadPreference());
            }
        }
    }
    public int size(){ return _storageQueue.size() ; }
    public String toString(){
      StringBuilder sb = new StringBuilder() ;

        for (Object o : _storageQueue.values()) {
            StorageClassInfo classInfo = (StorageClassInfo) o;
            sb.append(classInfo.toString()).append("\n");
            Enumeration<StorageRequestInfo> reqs = classInfo.requests();
            while (reqs.hasMoreElements()) {
                sb.append("   ").append(reqs.nextElement().toString())
                        .append("\n");
            }
        }

      return sb.toString() ;
    }
    public Iterator<StorageClassInfo> storageClassInfos(){ return _storageQueue.values().iterator() ; }
    public void setDefaultExpiration( long secs ){
       _defaultExpiration = secs*1000 ;
    }
    public void setDefaultPending( int pending ){
       _defaultPending = pending ;
    }
    public synchronized StorageClassInfo
                          defineStorageClass( String hsmName , String storageClass ){
        String composedName = storageClass+"@"+hsmName.toLowerCase() ;
        StorageClassInfo info =
                _storageQueue.get( composedName );

        if( info == null ) {
            info = new StorageClassInfo(hsmName, storageClass);
        }

        info.setDefined(true) ;
        _storageQueue.put( composedName , info ) ;
        return info ;

    }
    /*
    public synchronized StorageClassInfo
                          defineStorageClass(
                                 String storageClass ,
                                 int    expiration ,
                                 int    pending  ,
                                 int    preference    ){

        StorageClassInfo info = defineStorageClass( storageClass ) ;
        if( expiration >= 0 )info.setExpiration( expiration ) ;
        if( pending >= 0 )info.setPending( pending ) ;
        if( preference >  0 )info.setPreference( preference ) ;
      return info ;
    }
    */
    public synchronized Enumeration<String> getPnfsIdsOfClass(
            String storageClass){

        Vector<String> v = new Vector<>() ;
        StorageClassInfo info = _storageQueue.get(storageClass);
        if( info == null ) {
            return v.elements();
        }

        Enumeration<StorageRequestInfo> e = info.getRequests() ;
        while( e.hasMoreElements() ){
           v.addElement(
              (e.nextElement()).getPnfsId()
                       ) ;
        }
        return v.elements() ;
    }
    public synchronized Enumeration<StorageRequestInfo> getRequestsOfClass(
            String storageClass){

        Vector<StorageRequestInfo> v = new Vector<>() ;
        StorageClassInfo info = _storageQueue.get(storageClass);
        if( info == null ) {
            return v.elements();
        }

        return info.getRequests() ;
    }
    public synchronized List<PoolClassAttraction> getPreferenceHash(){
        List<PoolClassAttraction> list = new ArrayList<>() ;
        Iterator<StorageClassInfo> e    = _storageQueue.values().iterator() ;
        StorageClassInfo    info;
        PoolClassAttraction attr;
        while( e.hasNext() ){
           info = e.next();
           if( info.isDefined() ){
              attr = new PoolClassAttraction(
                             _poolName ,
                             info.getHsm() ,
                             info.getStorageClass()  ) ;
              attr.setPreferences(
                             info.getReadPreference() ,
                             info.getWritePreference()  ) ;

              list.add( attr) ;
           }
        }
        return list ;
    }
    public synchronized StorageClassInfo get( String hsmName , String storageClass ){
         return _storageQueue.get(
                     hsmName.toLowerCase()+"@"+storageClass);
    }
    public synchronized Enumeration<String> getTriggeredClasses(){
        Vector<String> v   = new Vector<>() ;
        Iterator<StorageClassInfo> e   = _storageQueue.values().iterator() ;
        StorageClassInfo info ;
        while( e.hasNext() ){
            info = e.next();
            if( info.isTriggered() ) {
                v.addElement(info.getStorageClass());
            }
        }
        return v.elements() ;
    }
    public synchronized boolean add( StorageInfo info ,
                                     String pnfsId       ){

        return add( info , pnfsId , -1 ) ;
    }
    public synchronized boolean add( StorageInfo info ,
                                     String pnfsId   ,
                                     long   time        ){

        String storageClass = info.getStorageClass() ;
        String hsmName      = info.getHsm().toLowerCase() ;

        String composedName = storageClass+"@"+hsmName ;

	StorageClassInfo classInfo =
                _storageQueue.get(composedName);

        if( classInfo == null ){
           classInfo =  new StorageClassInfo(hsmName,storageClass) ;
           _storageQueue.put( composedName , classInfo ) ;
        }

	classInfo.addRequest( new StorageRequestInfo( pnfsId , info , time ) );

        return classInfo.size() >= classInfo.getPending() ;
    }
    public synchronized void remove( String pnfsId ){

        for (Object o : _storageQueue.values()) {
            StorageClassInfo classInfo = (StorageClassInfo) o;
            classInfo.removeRequest(pnfsId);
            if ((classInfo.size() == 0) &&
                    !classInfo.isDefined()) {
                _storageQueue
                        .remove(classInfo.getName() + "@" + classInfo.getHsm());
            }
        }
    }
    public synchronized void remove( StorageInfo info , String pnfsId ){

        String storageClass = info.getStorageClass() ;
        String hsmName      = info.getHsm().toLowerCase() ;

        String composedName = storageClass+"@"+hsmName ;

	StorageClassInfo classInfo =
                _storageQueue.get(composedName);

        if( classInfo == null ) {
            return;
        }

	classInfo.removeRequest( pnfsId );

	if( ( classInfo.size() == 0 ) && ! classInfo.isDefined() ){
	    _storageQueue.remove( composedName );
	}
    }

    private synchronized void reschedule(String storageClass , long time){
        StorageClassInfo info =
                _storageQueue.get( storageClass );
        if( info == null ) {
            return;
        }

	info.setTime( time ) ;
    }
 }
