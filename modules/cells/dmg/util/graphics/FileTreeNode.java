package dmg.util.graphics ;

import java.io.* ;

public class FileTreeNode implements TreeNodeable {
   private boolean _folded   = true ;
   private boolean _selected = false ;
   private File    _base     = null ;
   private FileTreeNode _sub = null , _next = null ;
   public FileTreeNode( File f ){
      _base = f ;
   }
   public String       getName(){ return _base.getName() ; }
   public TreeNodeable getNext(){return _next ; }
   public TreeNodeable getSub(){
      if( _sub != null )return _sub ;
      if( ! _base.isDirectory() )return null ;
      String [] list = _base.list() ;
      if( ( list == null ) || ( list.length == 0 ) )return null ;
      
      FileTreeNode [] nodes = new FileTreeNode[list.length] ;
      nodes[0] = new FileTreeNode( new File( _base , list[0] ) ) ;
      for( int i = 1 ; i < nodes.length ; i++ ){
         nodes[i] = new FileTreeNode( new File( _base , list[i] ) ) ;
         nodes[i-1].setNextNode( nodes[i] ) ;
      }
      
      return _sub = nodes[0] ;
   }
   public void setNextNode( FileTreeNode node ){ _next = node ; }
   public boolean isContainerNode(){
      return _base.isDirectory() ;
   }
   public boolean      isFolded(){ return _folded ; }
   public void         switchFold(){ 
      if( _folded ){
        _folded = false ;
        _sub    = null ;
      }else{
        _folded = true ;
      }  
   }
   public boolean      isSelected(){ return _selected ; }
   public void         setSelected( boolean sel ){ _selected = sel ; }
}
