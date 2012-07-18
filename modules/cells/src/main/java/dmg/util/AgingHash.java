package dmg.util  ;

import java.util.* ;

public class AgingHash {

   private int       _maxSize = 0 ;
   private Node      _first   = null ;
   private Node      _last    = null ;
   private Hashtable _hash    = new Hashtable() ;
   
   public synchronized void clear(){
      _first = null ;
      _last  = null ;
      _hash.clear() ;
   }
   private class Node {
   
      private Object _value  = null ;
      private Object _key    = null ;
      private Node   _next     = null ; 
      private Node   _previous = null ;
      
      private Node( Object key , Object value ){ 
         _value = value ; 
         _key   = key ;
      }
      public String toString(){
         return "("+_key+":"+_value+")" ;
      }
      private void link(){
          _next     = _first ;
          _previous = null ;
          if( _first != null ) {
              _first._previous = this;
          }
          _first = this ;

          if( _last == null ) {
              _last = this;
          }
          return ;

      }
      private void unlink(){
         if( _next != null ) {
             _next._previous = _previous;
         } else {
             _last = _previous;
         }

         if( _previous != null ) {
             _previous._next = _next;
         } else {
             _first = _next;
         }

         return ;     
      }
   }
   public AgingHash( int maxSize ){
      _maxSize = maxSize ;
   }
   public synchronized Object get( Object key ){
       if( key == null ) {
           throw new
                   NullPointerException("Key == null");
       }
           
       Node node = (Node)_hash.get( key ) ;
       if( node == null ) {
           return null;
       }
       
       node.unlink() ;
       node.link() ;
       
       return node._value ;
   }
   public synchronized void put( Object key , Object value ){
       if( ( key == null ) || ( value == null ) ) {
           throw new
                   NullPointerException("Key/Value == null");
       }
       
       Node node = (Node)_hash.get( key ) ;
       if( node == null ){
       
           node = new Node( key , value ) ;
           _hash.put( key , node ) ;
           
       }else{
       
           node._value = value ;
           //
           // we have to relink to become top of stack.
           //
           node.unlink() ;
           
       }
       node.link() ;
       
       if( _hash.size() > _maxSize ){
          _hash.remove( _last._key ) ;
          _last.unlink() ;

       }
       return ;
   }
   public int size(){ return _hash.size() ; }
   public synchronized Object remove( Object key ){
      Node node = (Node)_hash.remove( key ) ;
      if( node == null ) {
          return null;
      }
            
      node.unlink() ; 
          
      return node._value ;     
   }
   public synchronized Iterator valuesIterator(){
       return new ArrayList(_hash.values()).iterator() ;
   }
   public synchronized Iterator keysIterator(){
       return new ArrayList(_hash.keySet()).iterator() ;
   }
   public synchronized String toString(){
      Node node = _first ;
      StringBuilder sb = new StringBuilder() ;
      for( node = _first ; node != null ; node = node._next ){
         sb.append(node.toString()).append(";") ;
      }
      sb.append("[").append(_first).append(";").append(_last).append("]");
      return sb.toString() ;
   }
   public static void main( String [] args ){
      AgingHash hash = new AgingHash(3) ;
      System.out.println( hash.toString() ) ;
      hash.put("1","1");
      System.out.println( hash.toString() ) ;
      hash.put("2","2");
      System.out.println( hash.toString() ) ;
      hash.put("3","3");
      System.out.println( hash.toString() ) ;
      hash.put("4","4");
      System.out.println( hash.toString() ) ;
      hash.put("5","5");
      System.out.println( hash.toString() ) ;
      hash.remove("4");
      System.out.println( hash.toString() ) ;
      hash.remove("3");
      System.out.println( hash.toString() ) ;
      hash.remove("5");
      System.out.println( hash.toString() ) ;
      
   }  

}
