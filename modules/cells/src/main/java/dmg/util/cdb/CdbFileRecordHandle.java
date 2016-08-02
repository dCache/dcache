package dmg.util.cdb ;


public class CdbFileRecordHandle extends CdbElementHandle  {
   private CdbFileRecord _record;
   public CdbFileRecordHandle( String  name ,
                               CdbContainable container  ,
                               CdbElementable element ){
    
        super( name , container , element ) ;
        
        if( ! ( element instanceof CdbFileRecord ) ) {
            throw new IllegalArgumentException("Not a CdbFileRecord");
        }
          
        _record = (CdbFileRecord)element ;
   }
   public Object getAttribute( String name ){
      return _record.getAttribute( name ) ;
   }
   public int getIntAttribute( String name ){
      return _record.getIntAttribute( name ) ;
   }
   public void setAttribute( String key , String name ){
      _record.setAttribute( key , name ) ;
   }
   public void setAttribute( String key , String [] name ){
      _record.setAttribute( key , name ) ;
   }
   public void setAttribute( String key , int value ){
      _record.setAttribute(key , String.valueOf(value)) ;
   }
   public void addListItem( String attributeName , String itemName ){
      _record.addListItem( attributeName , itemName , false ) ;
   }
   public void addListItem( String attributeName , String itemName , boolean unique ){
      _record.addListItem( attributeName , itemName , unique ) ;
   }
   public void removeListItem( String attributeName , String itemName ){
      _record.removeListItem( attributeName , itemName ) ;
   }
   public String toString(){
      return _record.toString() ;
   }
   public String toLine(){
      return _record.toLine() ;
   }
   
                                

} 
