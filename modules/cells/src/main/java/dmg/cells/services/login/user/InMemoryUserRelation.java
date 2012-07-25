// $Id: InMemoryUserRelation.java,v 1.1 2001-05-02 06:14:15 cvs Exp $
package dmg.cells.services.login.user  ;

import java.util.* ;

public class InMemoryUserRelation implements UserRelationable {

   private class DEnumeration implements Enumeration {
       @Override
       public boolean hasMoreElements(){ return false ; }
       @Override
       public Object nextElement(){ return null ;}
   }
   
   private class ElementItem {
      private Hashtable _parents;
      private Hashtable _childs;
      private void addParent(String parent){
         if( _parents == null ) {
             _parents = new Hashtable();
         }
         _parents.put(parent,parent) ;
      }
      private void addChild( String child ){
         if( _childs == null ) {
             _childs = new Hashtable();
         }
         _childs.put(child,child) ;
      }
      private Enumeration parents(){ 
          return _parents == null ? new DEnumeration() : _parents.keys() ;
      }
      private Enumeration children(){ 
          return _childs == null ? new DEnumeration() : _childs.keys() ;
      }
      private boolean hasChildren(){
         return ( _childs != null ) && ( _childs.size() > 0 ) ;
      }
      private boolean isParent(String parent){
          return ( _parents != null ) && ( _parents.get(parent)!=null ) ;
      }
      private boolean isChild(String child){
          return ( _childs != null ) && ( _childs.get(child)!=null ) ;
      }
      private void removeChild( String child ){
         if( _childs == null ) {
             return;
         }
         _childs.remove(child);
      }
      private void removeParent( String parent ){
         if( _parents == null ) {
             return;
         }
         _parents.remove(parent);
      }
   }
   
   private TopDownUserRelationable _db;
   private Hashtable _elements;
   public InMemoryUserRelation( TopDownUserRelationable db )
   {
      _db = db ;
      _loadElements() ;
   }
   @Override
   public synchronized Enumeration getContainers() {
      //
      // anonymous class with 'instance initialization'
      // We copy the data first to reduct the probability 
      // of currupted information.
      // Warning : This interface doesn't distiguish between
      //           groups and users. As a result the only 
      //           way to identify a user is to make sure
      //           that is has no children which could be
      //           an empty group as well. ( So what ??? )
      //
      return new Enumeration(){
        private Enumeration _ee;
        { 
           Enumeration xx = _elements.keys() ;
           Vector      v  = new Vector() ;
           while( xx.hasMoreElements() ){
              String name = (String) xx.nextElement() ;
              ElementItem ee  = (ElementItem)_elements.get(name) ;
              if( ee == null ) {
                  continue;
              }
              if( ee.hasChildren() ) {
                  v.addElement(name);
              }
           }
           _ee = v.elements() ;
        }
        @Override
        public boolean hasMoreElements(){
          return _ee.hasMoreElements() ;
        }
        @Override
        public Object nextElement(){
          return _ee.nextElement() ;
        }
      } ;
   }
   @Override
   public synchronized Enumeration getParentsOf( String element )
          throws NoSuchElementException {
       
      ElementItem item = (ElementItem)_elements.get(element) ;
      if( item == null ) {
          throw new
                  NoSuchElementException(element);
      }
         
      
      return item.parents()  ;
   }
   @Override
   public boolean isParentOf( String element , String container )
          throws NoSuchElementException {
          
      ElementItem item = (ElementItem)_elements.get(element) ;
      if( item == null ) {
          throw new
                  NoSuchElementException(element);
      }
         
      
      return item.isParent(container)  ;
   }      
   @Override
   public void createContainer( String container )
       throws DatabaseException {
       _db.createContainer(container) ;
       _elements.put( container , new ElementItem() ) ;
   }
   @Override
   public Enumeration getElementsOf( String container )
       throws NoSuchElementException {
      ElementItem item = (ElementItem)_elements.get(container) ;
      if( item == null ) {
          throw new
                  NoSuchElementException(container);
      }
         
      
      return item.children()  ;
   }
   @Override
   public boolean isElementOf( String container , String element )
       throws NoSuchElementException {
      ElementItem item = (ElementItem)_elements.get(container) ;
      if( item == null ) {
          throw new
                  NoSuchElementException(container);
      }
         
      
      return item.isChild(element)  ;
   }
   @Override
   public void addElement( String container , String element )
       throws NoSuchElementException {
       
      _db.addElement( container , element ) ;
      
      ElementItem item = (ElementItem)_elements.get(container) ;
      if( item == null ) {
          throw new
                  NoSuchElementException(container);
      }
         
      item.addChild( element ) ;
      
      item = (ElementItem)_elements.get(element) ;
      if( item == null ) {
          _elements.put(element, item = new ElementItem());
      }
      
      item.addParent(container);
      return ;
   }
   @Override
   public void removeElement( String container , String element )
       throws NoSuchElementException {
       
      _db.removeElement( container , element ) ;
      ElementItem item = (ElementItem)_elements.get(container) ;
      if( item == null ) {
          throw new
                  NoSuchElementException(container);
      }
         
      item.removeChild(element) ;
      
      item = (ElementItem)_elements.get(element) ;
      if( item == null ) {
          return;
      }
      
      item.removeParent(container);
      
      return ;
   }
   @Override
   public void removeContainer( String container )
       throws NoSuchElementException ,
              DatabaseException {
      
      _db.removeContainer( container ) ;
      _elements.remove( container ) ;
      return ;
   } 
   private void _loadElements()
   {
         
        Hashtable hash = new Hashtable() ;
        Enumeration e = _db.getContainers() ;
        
        while( e.hasMoreElements() ){
           String container = (String)e.nextElement() ;
           ElementItem item  = null , x = null;
           if( ( item = (ElementItem)hash.get( container ) ) == null ){
              hash.put( container , item = new ElementItem() ) ;
           }
           try{
              Enumeration f = _db.getElementsOf(container) ;
              while( f.hasMoreElements() ){
                  String name = (String)f.nextElement() ; 
                  item.addChild(name) ;
                  if( ( x = (ElementItem)hash.get(name) ) == null ){
                      hash.put(name , x = new ElementItem() ) ;
                  }
                  x.addParent( container ) ;
               }
           }catch( Exception ie ){
           }
        }
        _elements = hash ;
    }

}
