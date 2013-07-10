// $Id: CostCalculationV5.java,v 1.11 2007-07-26 13:44:03 tigran Exp $
//
package diskCacheV111.pools ;

import java.io.Serializable;
import java.util.Map;

public class      CostCalculationV5
       implements CostCalculatable,
                  Serializable  {

    private final PoolCostInfo      _info ;
    private double _performanceCost;
    private double _spaceCost;
    private long   _spaceCut        = 60 ;
    private final PoolCostInfo.PoolSpaceInfo _space ;

    private static final long serialVersionUID = 1466064905628901498L;

    public CostCalculationV5( PoolCostInfo info ){
       _info  = info ;
       _space = _info.getSpaceInfo() ;
    }
    @Override
    public double getSpaceCost(){
       return _spaceCost ;
    }
    @Override
    public double getPerformanceCost(){
       return _performanceCost ;
    }
    private double recalculateV4( long filesize ){
//       System.out.println("calling recalculate V4");
       if( filesize < _space.getFreeSpace() ){

          return  ((double)filesize) /
                  ((double)_space.getFreeSpace() ) /
                           _space.getBreakEven()  ;

       }else if( _space.getRemovableSpace() < _space.getGap() ){

          return Double.POSITIVE_INFINITY;

       }else{

	  return  ((double)filesize) /
                  ((double)(
                    _space.getRemovableSpace()+_space.getFreeSpace() )) ;

       }
    }
    private double recalculateV5( long filesize , long lru ){

       double SPACECOST_AFTER_ONE_WEEK = _space.getBreakEven();
       double spaceFactor = SPACECOST_AFTER_ONE_WEEK * (double)(24*7*3600) ;
//       System.out.println("calling recalculate V5 "+SPACECOST_AFTER_ONE_WEEK+" "+filesize+" "+lru);

//       if( filesize < _space.getFreeSpace() ){
       if(  _space.getFreeSpace() > _space.getGap() ){

          return  ((double)filesize) /
                  ((double)_space.getFreeSpace()) ;


       }else if( _space.getRemovableSpace() < _space.getGap() ){

           return Double.POSITIVE_INFINITY;

       }else{

          return 1.0 +
                      spaceFactor /
                       (double) Math.max( lru , _spaceCut ) ;

       }

    }
    @Override
    public void recalculate()
    {
       long filesize = 3 * 50 * 1000 * 1000;

       long lru = _space.getLRUSeconds() ;

       _spaceCost = _space.getBreakEven() >= 1.0 ?
                    recalculateV4( filesize ) :
                    recalculateV5( filesize , lru ) ;

//       System.out.println("Calculated space cost : "+_spaceCost);

       double cost = 0.0 ;
       double div  = 0.0 ;

       PoolCostInfo.PoolQueueInfo queue;

       Map<String, PoolCostInfo.NamedPoolQueueInfo> map = _info.getExtendedMoverHash() ;

       PoolCostInfo.PoolQueueInfo [] q = {

          map == null ? _info.getMoverQueue() : null ,
          _info.getP2pQueue() ,
          _info.getP2pClientQueue() ,
          _info.getStoreQueue() ,
          _info.getRestoreQueue()

       };
        for (PoolCostInfo.PoolQueueInfo info : q) {

            queue = info;

            if ((queue != null) && (queue.getMaxActive() > 0)) {
                cost += ((double) queue.getQueued() +
                        (double) queue.getActive()) /
                        (double) queue.getMaxActive();
                div += 1.0;
//            System.out.println("DEBUG : top "+cost+" "+div);
            }

        }
       if( map != null ) {
           for (Object o : map.values()) {
               queue = (PoolCostInfo.PoolQueueInfo) o;
               if ((queue != null) && (queue.getMaxActive() > 0)) {
                   cost += ((double) queue.getQueued() +
                           (double) queue.getActive()) /
                           (double) queue.getMaxActive();
                   div += 1.0;
//            System.out.println("DEBUG : top "+cost+" "+div);
               }
           }
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
