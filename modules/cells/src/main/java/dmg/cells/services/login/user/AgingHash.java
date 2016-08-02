// $Id: AgingHash.java,v 1.1 2001-05-02 06:14:15 cvs Exp $
package dmg.cells.services.login.user  ;

import java.util.Hashtable;

public class AgingHash {

   private int _maxSize;
   private Node _first;
   private Node _last;
   private Hashtable<Object, Node> _hash = new Hashtable<>() ;
   private static class Node {
      private Node( Object key , Object value ){
         this.value = value ;
         this.key   = key ;
      }
      private Object value;
      private Object key;
      private Node next;
      private Node previous;
      public String toString(){
         return "(" + key + ':' + value + ')';
//         return "("+key+":"+value+":"+previous+":"+next+")" ;
      }
   }
   public AgingHash( int maxSize ){
      _maxSize = maxSize ;
   }
   public synchronized Object get( Object key ){
       if( key == null ) {
           throw new
                   IllegalArgumentException("Key == null");
       }

       Node node = _hash.get( key );
       if( node == null ) {
           return null;
       }

       unlink( node ) ;
       link( node ) ;
       return node.value ;
   }
   public synchronized void put( Object key , Object value ){
       if( ( key == null ) || ( value == null ) ) {
           throw new
                   IllegalArgumentException("Key/Value == null");
       }
       //
       Node node = _hash.get( key );
       if( node == null ){

           node = new Node(key, value);
           _hash.put( key , node ) ;

       }else{

           node.value = value ;
           //
           // we have to relink to become top of stack.
           //
           unlink( node ) ;

       }
       link( node ) ;

       if( _hash.size() > _maxSize ){
          _hash.remove( _last.key ) ;
          unlink( _last ) ;

       }
   }
   public synchronized Object remove( Object key ){
      Node node = _hash.remove( key );
      if( node == null ) {
          return null;
      }

      unlink( node ) ;

      return node.value ;
   }
   private void link( Node node ){
       node.next     = _first ;
       node.previous = null ;
       if( _first != null ) {
           _first.previous = node;
       }
       _first = node ;

       if( _last == null ) {
           _last = node;
       }

   }
   private void unlink( Node node ){
      if( node.next != null ) {
          node.next.previous = node.previous;
      } else {
          _last = node.previous;
      }

      if( node.previous != null ) {
          node.previous.next = node.next;
      } else {
          _first = node.next;
      }

   }
   public String display(){
      Node node;
      StringBuilder sb = new StringBuilder() ;
      for( node = _first ; node != null ; node = node.next ){
         sb.append(node).append(';') ;
      }
      sb.append('[').append(_first).append(';').append(_last).append(']');
      return sb.toString() ;
   }
   public static void main( String [] args ){
      AgingHash hash = new AgingHash(3) ;
      System.out.println( hash.display() ) ;
      hash.put("1","1");
      System.out.println( hash.display() ) ;
      hash.put("2","2");
      System.out.println( hash.display() ) ;
      hash.put("3","3");
      System.out.println( hash.display() ) ;
      hash.put("4","4");
      System.out.println( hash.display() ) ;
      hash.put("5","5");
      System.out.println( hash.display() ) ;
      hash.remove("4");
      System.out.println( hash.display() ) ;
      hash.remove("3");
      System.out.println( hash.display() ) ;
      hash.remove("5");
      System.out.println( hash.display() ) ;

   }

}
