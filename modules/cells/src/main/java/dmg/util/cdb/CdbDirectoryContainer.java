package dmg.util.cdb ;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Hashtable;

import dmg.cells.services.login.UserHandle;

public class      CdbDirectoryContainer
       extends    CdbGLock
       implements CdbContainable, CdbElementable {

   private Class<CdbFileRecord> _elementClass;
   private Class<UserHandle> _handlerClass;
   private File   _containerDirectory;

   private Constructor<UserHandle> _handlerConstructor;
   private Constructor<CdbFileRecord> _elementConstructor;
   private Method      _elementRemoveMethod;

   private boolean     _sticky;
   private boolean     _exists = true ;


   private static final Class<?> []
            __elementConstructorArguments = {
       CdbLockable.class ,
       File.class ,
       Boolean.TYPE
   } ;
   private static final Class<?> []
            __handlerConstructorArguments = {
       String.class ,
       CdbContainable.class ,
       CdbElementable.class
   } ;
   private final Hashtable<String, ElementEntry> _table      = new Hashtable<>() ;
   private class  ElementEntry {

       private CdbLockable   _lockable;
       private int           _refCounter;

       private ElementEntry( CdbLockable lockable ){
          _lockable   = lockable ;
          _refCounter = 1 ;
       }
       private CdbLockable getLockable(){ return _lockable ; }
       private synchronized  void incrementRefCounter(){ _refCounter ++ ; }
       private synchronized  void decrementRefCounter(){ _refCounter -- ; }
       private int  getRefCounter(){ return _refCounter ; }

   }
   public CdbDirectoryContainer( CdbLockable superLock ,
                                 Class<CdbFileRecord> elementClass ,
                                 Class<UserHandle> handlerClass ,
                                 File        directory   ,
                                 boolean     create           )
          throws CdbException                                   {

       _elementClass       = elementClass ;
       _handlerClass       = handlerClass ;
       _containerDirectory = directory ;
       //
       // do the File stuff
       //
//       try{
          if( create ){
             if( _containerDirectory.exists() ) {
                 throw new
                         IllegalArgumentException("DataSource already exists");
             }
             if( ! _containerDirectory.mkdir() ) {
                 throw new
                         IllegalArgumentException(
                         "Failed to create : " + _containerDirectory);
             }
          }else{
             if( ! _containerDirectory.isDirectory() ) {
                 throw new
                         IllegalArgumentException("DataSource does not exist");
             }
          }
//       }catch( IOException ioe ){
//          throw new CdbException( "IO Problem : "+ioe ) ;
//       }
       //
       // check for the existence of the neccessary
       // container constructors and remove method.
       //
       try{
           _elementConstructor =
               elementClass.getConstructor(
                     __elementConstructorArguments ) ;
       }catch( NoSuchMethodException nsme ){
          throw new
          CdbException( "No matching container constructor found for : "+
                        elementClass.getName() ) ;
       }
       //
       //
       //
       try{
          _elementRemoveMethod =
              _elementClass.getMethod("remove");
       }catch( NoSuchMethodException nsmei ){
           throw new CdbException( "No matching remove method found" ) ;
       }
       //
       // check for the existence of the handler constructor
       //
       try{
           _handlerConstructor =
               _handlerClass.getConstructor(
                     __handlerConstructorArguments ) ;
       }catch( NoSuchMethodException nsme ){
           throw new
           CdbException( "No matching handler constructor found" ) ;
       }
   }
   public void setSticky( boolean sticky ){ _sticky = sticky ; }

   public CdbElementHandle createElement( String name )
          throws CdbException, InterruptedException {
       //
       // make sure we are holding the mutex.
       //
       CdbElementHandle handle;
       //
       //
       //
       //
       open( CdbLockable.WRITE ) ;
       //
       // check for the existence of the resource ...
       //
       File file = new File( _containerDirectory , name ) ;
       if( file.exists() ){
         close(CdbLockable.ABORT) ;
         throw new CdbException("Record already exists "+file ) ;
       }
       //
       //
       try{
          handle = createMirrorEntry( name , true ) ;
       }catch( CdbException edbe ){
          close(CdbLockable.ABORT) ;
          throw edbe ;
       }
       close(CdbLockable.COMMIT) ;
       return handle ;
   }
   public CdbElementHandle getElementByName( String name )
          throws CdbException, InterruptedException {
       //
       // check for an entry in the cache
       //
       ElementEntry    entry;
       CdbElementHandle handle;
       //
       // get the read mutex
       //
       open( CdbLockable.READ ) ;
       //
       // try to find the entry in the cache
       //
       if( ( entry = _table.get( name )) != null ){
          //
          //  it was still in the cache, so we only have to increment
          //  the reference counter and that's it.
          //
          entry.incrementRefCounter() ;
          close(CdbLockable.COMMIT) ;
          return newHandlerInstance( name , this , entry.getLockable() ) ;
       }
       //
       // not in cache , look for it in the database itself ( filesystem )
       // check for the existence of the resource ...
       //
       File file = new File( _containerDirectory , name ) ;
       if( ! file.exists() ){
          //
          // doens't exists, so what says zap
          //
         close(CdbLockable.ABORT) ;
         throw new CdbException("Record does not exists "+file ) ;
       }
       //
       //
       try{
          handle = createMirrorEntry( name , false ) ;
       }catch( CdbException edbe ){
          close(CdbLockable.ABORT) ;
          throw edbe ;
       }
       close(CdbLockable.COMMIT) ;
       return handle ;
   }
   private CdbElementHandle createMirrorEntry( String name , boolean create  )
           throws CdbException
   {

       CdbElementHandle handle;
       CdbLockable      element = null ;
       //
       // and create our 'mirror object'
       //
       try{
           if( _elementConstructor != null ){
              Object [] args = { this ,
                                 new File(_containerDirectory , name ) ,
                      create
                               } ;
              element  = _elementConstructor.newInstance( args );
           }
       }catch( InvocationTargetException ite ){
          throw new
          CdbException( "Invocation Failed : "+ite.getTargetException() ) ;
       }catch( Exception  e ){
          throw new
          CdbException( "Problem : "+e ) ;
       }
       //
       // create the first handle
       //
       handle = newHandlerInstance( name , this , element ) ;
       //
       // and add it  to our list. ( no need to increment ref counter,
       // this is done automatically on creation of entry )
       //
       synchronized( _table ){
          _table.put( name , new ElementEntry( element ) ) ;
       }
       //
       // return the write access
       //
       return handle ;
   }
   private CdbElementHandle newHandlerInstance( String name ,
                                                 CdbContainable container ,
                                                 CdbLockable    element    )
           throws CdbException {

       CdbElementHandle handle;
       try{
           Object [] args = { name , container , element } ;
           handle  = _handlerConstructor.newInstance( args );

       }catch( InvocationTargetException ite ){
          throw new
          CdbException( "Invocation Failed : "+ite.getTargetException() ) ;
       }catch( Exception  e ){
          throw new
          CdbException( "Problem : "+e ) ;
       }
       return handle ;
   }
   public void removeElement( String name )
          throws CdbException, InterruptedException  {

       open( CdbLockable.WRITE ) ;
       CdbElementHandle handle = getElementByName( name ) ;
       try{
           handle.open( CdbLockable.WRITE ) ;
           try{
               ElementEntry entry = _table.get( name );
               CdbLockable element = entry.getLockable() ;
               _elementRemoveMethod.invoke( element) ;
           }catch( InvocationTargetException ive ){
              handle.close(CdbLockable.ABORT) ;
              Throwable t = ive.getTargetException() ;
              if( t instanceof CdbException ) {
                  throw (CdbException) t;
              }
              throw new
              CdbException( "Problem in remove method : "+t );
           }catch( Exception ee ){
              handle.close(CdbLockable.ABORT) ;
              throw new
              CdbException( "Problem in remove method : "+ee);
           }
           handle.close(CdbLockable.COMMIT ) ;
       }catch( CdbException | InterruptedException edbe ){
          close( CdbLockable.ABORT ) ;
          throw edbe ;
       }
       close( CdbLockable.COMMIT ) ;
   }
   @Override
   public synchronized void unlinkElement( String name ) {
      ElementEntry entry = _table.get( name );
      if( entry == null ) {
          return;
      }
      entry.decrementRefCounter() ;
      if( ( ! _sticky ) && ( entry.getRefCounter() <= 0 ) ){
//          System.out.println( name + " removed from hashtable" ) ;
          _table.remove( name ) ;
      }
   }
   public String [] getElementNames(){
       return _containerDirectory.list() ;
   }
   //
   // this removes the container resource itself
   //
   @Override
   public synchronized void open( int mode )
          throws CdbLockException, InterruptedException {
       if( ! _exists ) {
           throw new CdbLockException("Object removed");
       }
       super.open( mode ) ;

   }
   @Override
   public void remove() throws CdbException {
       _exists = false ;
       if( ! _containerDirectory.delete() ) {
           throw new
                   CdbException("Couldn't remove : " + _containerDirectory);
       }
   }
   @Override
   public void readLockGranted() {
//     System.out.println( "readLockGranted "+_containerDirectory ) ;
   }
   @Override
   public void writeLockGranted(){
//     System.out.println( "writeLockGranted "+_containerDirectory ) ;
   }
   @Override
   public void readLockReleased(){
//     System.out.println( "readLockReleased "+_containerDirectory ) ;
   }
   @Override
   public void writeLockReleased(){
//     System.out.println( "writeLockReleased "+_containerDirectory ) ;
   }
   @Override
   public void writeLockAborted(){
//      System.out.println( "writeLockAborted "+_containerDirectory ) ;
   }

}
