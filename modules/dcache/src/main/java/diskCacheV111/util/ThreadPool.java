// $Id: ThreadPool.java,v 1.4 2007-07-23 13:21:42 tigran Exp $

package diskCacheV111.util ;

/*
 * consider to use java.util.concurrent.ThreadPoolExecutor
 */
@Deprecated
public interface ThreadPool {


    //
    // if 'runner implements dmg.util.ExtendRunnabe',
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
