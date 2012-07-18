// $Id: PoolCheckCostMessage.java,v 1.8 2004-11-05 12:07:19 tigran Exp $
package diskCacheV111.vehicles;

import java.util.*;


/**
  *   Pool check cost message sended by Pool Manager to collect information
  *   for Decision Unit.
  */

public class PoolCheckCostMessage
       extends PoolCheckMessage
       implements PoolCostCheckable {

    private double _spaceCost      = 0.0  ;
    private double _performanceCost= 0.0  ;
    private long   _filesize       = 0 ;

    private static final long serialVersionUID = 4310317407646107895L;

    public PoolCheckCostMessage( String poolName , long filesize ){
	super(poolName);
        _filesize = filesize ;
	setReplyRequired(true);
    }
    public PoolCheckCostMessage( String poolName ){
        this( poolName , 0 ) ;
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
          append(";Filesize=").append(_filesize).
          append(";Space=").append((float)_spaceCost).
          append(";Load=").append((float)_performanceCost);
       return sb.toString();
    }
}
