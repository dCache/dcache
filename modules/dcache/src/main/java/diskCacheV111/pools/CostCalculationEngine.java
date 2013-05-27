// $Id: CostCalculationEngine.java,v 1.3 2003-08-23 16:53:47 cvs Exp $
//
package diskCacheV111.pools ;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.MissingResourceException;

public class CostCalculationEngine implements Serializable
{
    private static final long serialVersionUID = -5879481746924995686L;

    private final Class<? extends CostCalculatable> _class;
    private transient Constructor<? extends CostCalculatable> _constructor;
    private final static Class<?> [] _classArgs = { PoolCostInfo.class } ;

    public CostCalculationEngine(String algorithmClass)
        throws ClassNotFoundException,
               NoSuchMethodException
    {
       _class = Class.forName(algorithmClass).asSubclass(CostCalculatable.class);
       _constructor = _class.getConstructor( _classArgs ) ;
    }

    public CostCalculatable getCostCalculatable( PoolCostInfo info )
           throws MissingResourceException
    {
       Object [] args = { info } ;

       try{

         return _constructor.newInstance( args ) ;

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
