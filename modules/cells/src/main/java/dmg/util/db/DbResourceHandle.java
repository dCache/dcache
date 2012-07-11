package dmg.util.db ;

import java.util.* ;

public class DbResourceHandle implements DbLockable, DbRecordable {
   private int               _refCounter = 0 ;
   private DbResourceHandler _creator    = null ;
   private DbRecordable      _resource   = null ;
   private String            _name       = null ;
   private boolean           _isValid    = true ;
   public DbResourceHandle( String name ,
                            DbResourceHandler creator ,
                            DbRecordable resource ){
       _name     = name ;
       _creator  = creator ;
       _resource = resource ;
       
   
   }
   public String  getName(){ return _name ; }
   public void    isValid( boolean isValid ){ _isValid  = isValid ; }
   public boolean isValid(){ return _isValid ; }
   
   public void open( int flags ) 
          throws DbLockException, 
                 InterruptedException {
       if( ! _isValid ) {
           throw new DbLockException("Object no longer exists");
       }
       _resource.open( flags ) ;
   }
   public void close() throws DbLockException {
       _resource.close() ;
   }
   public void setAttribute( String name , String attribute ) {
       _resource.setAttribute( name , attribute ) ;
   }
 
   public void setAttribute( String name , String [] attribute ){
       _resource.setAttribute( name , attribute ) ;
   }
 
   public Object getAttribute( String name ) {
     return _resource.getAttribute( name ) ;
   }
  
   public Enumeration getAttributes(){ 
     return _resource.getAttributes() ;
   } 
   
   public void remove(){
     _resource.remove() ;
   }
   protected void finalize() throws Throwable {
       System.out.println( "Decrementing "+_name ) ;
       _creator.unlinkResource( this ) ;
   }
   public String toString(){
      StringBuffer sb = new StringBuffer() ;
      sb.append( "Name : "+getName()+"\n" ) ;
      sb.append( _resource.toString() ) ;
      return sb.toString() ;
   }

}
