// $Id: PoolCheckFileCostMessage.java,v 1.4 2004-11-05 12:07:19 tigran Exp $
package diskCacheV111.vehicles;
import  diskCacheV111.util.* ;


/**
  *   Pool check cost message sended by Pool Manager to collect information
  *   for Decision Unit.
  */

public class   PoolCheckFileCostMessage
       extends PoolCheckFileMessage
       implements PoolFileCheckable,
                  PoolCostCheckable{


    private double _spaceCost;
    private double _performanceCost;
    private long   _filesize;

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
