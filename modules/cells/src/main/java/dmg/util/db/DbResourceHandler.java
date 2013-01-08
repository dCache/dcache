package dmg.util.db ;

import java.util.* ;
import java.io.* ;

public class DbResourceHandler extends DbGLock {
    private final Hashtable<String, ResourceEntry> _table      = new Hashtable<>() ;
    private File      _dataSource;
    private class  ResourceEntry {
    
        private DbRecordable _recordable;
        private int          _refCounter;
        
        private ResourceEntry( DbRecordable recordable ){
           _recordable = recordable ;
           _refCounter = 1 ;
        }
        private DbRecordable getRecordable(){ return _recordable ; }
        private synchronized void incrementRefCounter(){ _refCounter ++ ; }
        private synchronized void decrementRefCounter(){ _refCounter -- ; }
        private int  getRefCounter(){ return _refCounter ; }
    
    }
    public DbResourceHandler( File source  , boolean create ){
       _dataSource = source ;
       if( create && _dataSource.isDirectory() ) {
           throw new IllegalArgumentException("DataSource already exists");
       }
       if( create  ) {
           throw new
                   IllegalArgumentException("create not yet implemented");
       }
    }
    public DbResourceHandler( DbLockable creator , File source  , boolean create ){
       super( creator ) ;
       _dataSource = source ;
       if( create && _dataSource.isDirectory() ) {
           throw new IllegalArgumentException("DataSource already exists");
       }
       if( create  ) {
           throw new
                   IllegalArgumentException("create not yet implemented");
       }
    }
    public String [] getResourceNames()
    {
       return _dataSource.list() ;
    }
    public DbResourceHandle createResource( String name )
           throws DbException, InterruptedException  {
       
       //
       // make sure we are holding the mutex.
       //   
       open( DbGLock.WRITE_LOCK ) ;
       //
       // check for the existence of the resource ...
       //
       File file = new File( _dataSource , name ) ;
       if( file.exists() ){
         close() ;
         throw new DbException("Record already exists "+file ) ;
       }
       //
       // and create it
       //
       DbRecordable rec;
       try{
          rec = new DbFileRecord( this , file , true ) ;
       }catch(IOException ioe ){
          close() ;
          throw new DbException( "Can't create "+name+" : "+ioe.toString() ) ;
       }
       //
       // create the first handle
       //
       DbResourceHandle handle = new DbResourceHandle( name , this , rec ) ;
       //
       // and add it  to our list. ( no need to increment ref counter,
       // this is done automatically on creation of entry )
       //
       synchronized( _table ){
          _table.put( name , new ResourceEntry( rec ) ) ;
       }
       //
       // return the write access
       //
       close() ;
       return handle ;
    }
    public int getCacheSize(){ return _table.size() ; }
    public DbResourceHandle getResourceByName( String name )
           throws DbException, InterruptedException   {   
           
       //
       // check for an entry in the cache
       //            
       ResourceEntry   entry;
       DbRecordable    rec;
       //
       // get the read mutex
       //      
       open( DbGLock.READ_LOCK  ) ;
       //
       // try to find the entry in the cache
       //
       if( ( entry = _table.get( name )) != null ){
          //
          //  it was still in the cache, so we only have to increment
          //  the reference counter and that's it.
          //
          entry.incrementRefCounter() ;
          close() ;
          return new DbResourceHandle( name , this , entry.getRecordable() ) ;
       }
       //
       // not in cache , look for it in the database itself ( filesystem )
       //
       File file = new File( _dataSource , name ) ;
       if( ! file.exists() ){
          //
          // doens't exists, so what says zap
          //
          close() ;
          throw new DbException("Record not found "+file ) ;
       }
       try{
          //
          // create the internal representation
          //
          rec = new DbFileRecord( this , file , false ) ;
       }catch(IOException ioe ){
          close() ;
          throw new DbException( "Can't create "+name+" : "+ioe.toString() ) ;
       }
       DbResourceHandle handle = new DbResourceHandle( name , this , rec ) ;
       synchronized( _table ){
          _table.put( name , new ResourceEntry( rec ) ) ;
       }
       close() ;
       return handle ;
    }
    public void removeResource( DbResourceHandle handle )
           throws DbLockException , InterruptedException {
       
        open( DbGLock.WRITE_LOCK ) ;
        handle.open( DbGLock.WRITE_LOCK ) ;
        handle.remove() ;
        handle.close() ;
        close();
    }
    /**
      *  unlink deletes the specified reference to the resource,
      *  checks if the remainding ref counter is zero
      *  and removes the Resource representation if so.
      *  The resource itself is NOT removed.
      */
    void unlinkResource( DbResourceHandle handle )
           throws DbLockException
    {
           
       //
       // because this method will be called by the
       // finalizer, we can't use the .open().
       //
       synchronized( _table ){
          ResourceEntry entry = _table.get( handle.getName() );
          if( entry == null ) {
              throw new
                      DbLockException(
                      "PANIC : entry to be removed not found : " + handle
                              .getName());
          }
            
          entry.decrementRefCounter() ;  
          if( entry.getRefCounter() <= 0 ){
              //
              //
              _table.remove( handle.getName() ) ;
              System.out.println( "Removed from cache : " + handle.getName() ) ;
          }
       }
    }
}
