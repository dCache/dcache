// $Id: MessageTimerEvent.java,v 1.1 2002-01-21 09:00:06 cvs Exp $
package diskCacheV111.util ;

public interface MessageTimerEvent {

    public void event( MessageEventTimer timer ,
                       Object eventObject ,
		       int eventType ) ;
}
