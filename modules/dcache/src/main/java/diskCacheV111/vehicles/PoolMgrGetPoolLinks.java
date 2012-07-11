// $Id: PoolMgrGetPoolLinks.java,v 1.2 2006-08-07 21:21:50 timur Exp $

package diskCacheV111.vehicles ;
import  diskCacheV111.util.* ;

public class PoolMgrGetPoolLinks extends PoolManagerMessage {

    private PoolLinkInfo[] poolLinkInfos;

  // need to add once finalized
   // private static final long serialVersionUID = ;

    public PoolMgrGetPoolLinks( ){
	setReplyRequired(true);
    }
    public String toString(){
       if( getReturnCode() == 0 ) {
           StringBuffer sb  = new StringBuffer();
           for(int i=0; i<getPoolLinkInfos().length; ++i ) {
               sb.append('[').append(getPoolLinkInfos()[i]).append(']');
           }
             return sb.toString();
       }
       else {
           return super.toString();
       }
    }

    public PoolLinkInfo[] getPoolLinkInfos() {
        return poolLinkInfos;
    }

    public void setPoolLinkInfos(PoolLinkInfo[] poolLinkInfos) {
        this.poolLinkInfos = poolLinkInfos;
    }


}
