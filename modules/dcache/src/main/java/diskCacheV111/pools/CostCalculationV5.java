package diskCacheV111.pools ;

import java.io.Serializable;

public class      CostCalculationV5
       implements CostCalculatable,
                  Serializable  {

    private final PoolCostInfo      _info ;
    private double _performanceCost;

    private static final long serialVersionUID = 1466064905628901498L;

    public CostCalculationV5( PoolCostInfo info ){
       _info  = info ;
    }
    @Override
    public double getPerformanceCost(){
       return _performanceCost ;
    }

    @Override
    public void recalculate()
    {
       double cost = 0.0 ;
       double div  = 0.0 ;

       PoolCostInfo.PoolQueueInfo [] q1 = {
          _info.getP2pQueue() ,
          _info.getP2pClientQueue()
       };
        for (PoolCostInfo.PoolQueueInfo queue : q1) {
            if (queue != null) {
                if (queue.getMaxActive() > 0) {
                    cost += ((double) queue.getQueued() +
                            (double) queue.getActive()) /
                            (double) queue.getMaxActive();
                } else if (queue.getQueued() > 0) {
                    cost += 1.0;
                }
                div += 1.0;
            }
        }
       PoolCostInfo.PoolQueueInfo [] q2 = {
          _info.getStoreQueue() ,
          _info.getRestoreQueue()

       };
        for (PoolCostInfo.PoolQueueInfo queue : q2) {
            if (queue != null) {
                if (queue.getQueued() > 0) {
                    cost += 1.0;
                } else {
                    cost += (1.0 - Math.pow(0.75, queue.getActive()));
                }
                div += 1.0;
            }
        }
        for (PoolCostInfo.NamedPoolQueueInfo queue : _info.getExtendedMoverHash().values()) {
            if (queue.getMaxActive() > 0) {
                cost += ((double) queue.getQueued() +
                        (double) queue.getActive()) /
                        (double) queue.getMaxActive();
            } else if (queue.getQueued() > 0) {
                cost += 1.0;
            }
            div += 1.0;
        }
       _performanceCost = div > 0.0 ? cost / div : 1000000.0;
//       System.out.println("Calculation : "+_info+" -> cpu="+_performanceCost);

       /*
       if( ( queue = _info.getMoverQueue() ).getMaxActive() > 0 ){
         cost += ( (double)queue.getQueued() +
                   (double)queue.getActive()  ) /
                   (double)queue.getMaxActive() ;
         div += 1.0 ;
       }
       if( ( queue = _info.getStoreQueue() ).getMaxActive() > 0 ){
         cost += ( (double)queue.getQueued() +
                   (double)queue.getActive()  ) /
                   (double)queue.getMaxActive() ;
         div += 1.0 ;
       }
       if( ( queue = _info.getRestoreQueue() ).getMaxActive() > 0 ){
         cost += ( (double)queue.getQueued() +
                   (double)queue.getActive()  ) /
                   (double)queue.getMaxActive() ;
         div += 1.0 ;
       }
       */

    }

}
