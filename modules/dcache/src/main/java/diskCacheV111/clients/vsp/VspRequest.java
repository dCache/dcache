// $Id: VspRequest.java,v 1.1 2000-02-20 21:16:36 cvs Exp $
//
package diskCacheV111.clients.vsp ;

public interface VspRequest {
   public static final int IN_PROCESS = -1 ;
   public int getResultCode() ;
   public String getResultMessage() ;
}
