// $Id: CostCalculationEngine.java,v 1.3 2003-08-23 16:53:47 cvs Exp $
//
package diskCacheV111.pools ;

import java.lang.reflect.* ;
import java.util.* ;
import java.io.IOException;
import java.io.Serializable;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;

public class CostCalculationEngine implements Serializable
{
    static final long serialVersionUID = -5879481746924995686L;

    private final Class _class;
    private transient Constructor _constructor;
    private final static Class [] _classArgs = { diskCacheV111.pools.PoolCostInfo.class } ;

    public CostCalculationEngine(String algorithmClass)
        throws ClassNotFoundException,
               NoSuchMethodException
    {
       _class = Class.forName(algorithmClass) ;
       _constructor = _class.getConstructor( _classArgs ) ;
    }

    public CostCalculatable getCostCalculatable( PoolCostInfo info )
           throws MissingResourceException
    {
       Object [] args = { info } ;

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

    private void readObject(ObjectInputStream in)
        throws IOException, ClassNotFoundException
    {
        try {
            in.defaultReadObject();
            _constructor = _class.getConstructor(_classArgs);
        } catch (NoSuchMethodException e) {
            throw new IOException("Failed to read CostCalculationEngine", e);
        }
    }
}
