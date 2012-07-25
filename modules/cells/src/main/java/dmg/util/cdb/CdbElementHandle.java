package dmg.util.cdb ;

import java.lang.reflect.* ;
import java.util.* ;
import java.io.* ;

public class CdbElementHandle implements CdbLockable {

   private String         _name;
   private CdbContainable _container;
   private CdbElementable _element;
   
   public CdbElementHandle( String name ,
                            CdbContainable container ,
                            CdbElementable element      ){
      _name      = name ;
      _container = container ;
      _element   = element ;

   }
   public String getName(){ return _name ; }
   public CdbContainable getContainer(){ return _container ; }
   public CdbElementable getElement(){ return _element ; }
   @Override
   public void open( int flags ) throws CdbLockException,
                                        InterruptedException {
       _element.open( flags ) ;
   }
   @Override
   public void close( int flags ) throws CdbLockException {
       _element.close( flags ) ;
   }
   @Override
   protected void finalize() throws Throwable {
//       System.out.println( "Decrementing "+_name ) ;
       _container.unlinkElement( _name ) ;
   }

}
