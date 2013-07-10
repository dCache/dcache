// $Id: CostCalculatable.java,v 1.1 2003-08-03 21:16:51 cvs Exp $
//
package diskCacheV111.pools ;

public interface CostCalculatable {

    public double getSpaceCost() ;
    public double getPerformanceCost() ;
    public void   recalculate();
}
