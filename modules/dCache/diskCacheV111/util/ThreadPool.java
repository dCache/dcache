// $Id: ThreadPool.java,v 1.2 2004-06-07 12:53:51 patrick Exp $ 

package diskCacheV111.util ;


public interface ThreadPool {


    //
    // if 'runner imlements dmg.util.ExtendRunnabe',
    // runFailed is called whenever the runner couldn't
    // be and will not be called for whatever reason.
    //
    public void invokeLater( Runnable runner , String name ) 
       throws IllegalArgumentException ;
       
    public int getCurrentThreadCount() ;
    public int getMaxThreadCount() ;
    public int getWaitingThreadCount() ;
    
    public void setMaxThreadCount( int maxThreadCount )
        throws IllegalArgumentException ;
}
