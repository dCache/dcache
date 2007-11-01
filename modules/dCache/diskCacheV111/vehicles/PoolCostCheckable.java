// $Id: PoolCostCheckable.java,v 1.2 2002-03-11 09:21:27 cvs Exp $

package diskCacheV111.vehicles;
import  diskCacheV111.util.* ;
import java.util.*; 


public interface PoolCostCheckable extends PoolCheckable {

    public double getSpaceCost();
    public void setSpaceCost(double spaceCost) ;
    public double getPerformanceCost() ;
    public void setPerformanceCost(double performanceCost);
    public long getFilesize() ;

}
