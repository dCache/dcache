// $Id: CostCalculationEngine.java,v 1.3 2003-08-23 16:53:47 cvs Exp $
//
package diskCacheV111.pools ;

import java.lang.reflect.* ;
import java.util.* ;

public class CostCalculationEngine {
    private Class _class = null ;
    private Constructor _constructor = null ;
    private final static Class [] _classArgs = { diskCacheV111.pools.PoolCostInfo.class } ;
    public CostCalculationEngine( String algorithmClass )
           throws 
               ClassNotFoundException ,
               NoSuchMethodException   {
    
       _class = Class.forName(algorithmClass) ;
       _constructor = _class.getConstructor( _classArgs ) ;
       
    }
    public CostCalculatable getCostCalculatable( PoolCostInfo info )
           throws MissingResourceException {
                  
       Object [] args = { info } ;
       long started = System.currentTimeMillis() ;
       
       try{
       
         return (CostCalculatable)_constructor.newInstance( args ) ;
        
       }catch(Exception ee ){
          throw new
          MissingResourceException( "Can't create cost calculator : "+ee ,
                                    _class.getName() , info.getPoolName() ) ; 
       }finally{
//          System.out.println("newInstance(CostCalculatable) : "+
//                             (System.currentTimeMillis()-started));
       }
    }
}
