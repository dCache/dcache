package diskCacheV111.util.event ;

public class CacheComponent {

    CacheRepositoryListener eventListener = null ;
    
    public synchronized void 
           addCacheRepositoryListener( CacheRepositoryListener listener ){
        eventListener = CacheEventMulticaster.add(eventListener,listener);
    }
    public synchronized void 
           removeCacheRepositoryListener( CacheRepositoryListener listener ){
        eventListener = CacheEventMulticaster.remove(eventListener,listener);
    }
    public void  processActionEvent( CacheRepositoryEvent event ){
       if( eventListener != null )
          eventListener.actionPerformed( event ) ;
    }
    public void  processCreateEvent( CacheRepositoryEvent event ){
       if( eventListener != null )
          eventListener.created( event ) ;
    }
    public static void main( String [] args ){
        CacheComponent component  = new CacheComponent() ;
        component.addCacheRepositoryListener( new XXX("x1") ) ;
        component.addCacheRepositoryListener( new XXX("x2") ) ;
        component.addCacheRepositoryListener( new XXX("x3") ) ;
        component.processActionEvent( new CacheRepositoryEvent(component,null) ) ;
        component.processCreateEvent( new CacheRepositoryEvent(component,null) ) ;
    }
}
