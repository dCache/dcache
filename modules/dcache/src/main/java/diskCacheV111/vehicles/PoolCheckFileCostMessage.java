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
    @Override
    public double getSpaceCost() {
       return _spaceCost;
    }

    @Override
    public void setSpaceCost(double spaceCost) {
       _spaceCost = spaceCost;
    }

    @Override
    public double getPerformanceCost() {
       return _performanceCost;
    }

    @Override
    public void setPerformanceCost(double performanceCost) {
       _performanceCost = performanceCost;
    }
    @Override
    public long getFilesize(){ return _filesize ; }

    public String toString(){
       StringBuilder sb = new StringBuilder() ;
       sb.append(super.toString()).
          append(";Space=").append((float)_spaceCost).
          append(";Load=").append((float)_performanceCost) ;
       if( getReturnCode() != 0 ) {
           sb.append(";").append(super.toString());
       }

       return sb.toString() ;
    }
}
