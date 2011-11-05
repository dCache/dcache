// $Id: PoolCheckFileCostMessage.java,v 1.4 2004-11-05 12:07:19 tigran Exp $
package diskCacheV111.vehicles;
import  diskCacheV111.util.* ;
import java.util.*;


/**
  *   Pool check cost message sended by Pool Manager to collect information
  *   for Decision Unit.
  */

public class   PoolCheckFileCostMessage
       extends PoolCheckFileMessage
       implements PoolFileCheckable,
                  PoolCostCheckable{


    private double _spaceCost      = 0.0  ;
    private double _performanceCost= 0.0  ;
    private long   _filesize       = 0 ;

    private static final long serialVersionUID = 1230547500651961362L;

    public PoolCheckFileCostMessage(String poolName,
                                    PnfsId pnfsId ,
                                    long filesize){
	super(poolName,pnfsId);
        _filesize = filesize ;
	setReplyRequired(true);

    }
    public double getSpaceCost() {
       return _spaceCost;
    }

    public void setSpaceCost(double spaceCost) {
       _spaceCost = spaceCost;
    }

    public double getPerformanceCost() {
       return _performanceCost;
    }

    public void setPerformanceCost(double performanceCost) {
       _performanceCost = performanceCost;
    }
    public long getFilesize(){ return _filesize ; }

    public String toString(){
       StringBuffer sb = new StringBuffer() ;
       sb.append(super.toString()).
          append(";Space=").append((float)_spaceCost).
          append(";Load=").append((float)_performanceCost) ;
       if( getReturnCode() != 0 )sb.append(";").append(super.toString());

       return sb.toString() ;
    }
}
