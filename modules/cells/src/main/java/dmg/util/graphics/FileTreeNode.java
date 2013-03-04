package dmg.util.graphics ;

import java.io.File;

public class FileTreeNode implements TreeNodeable {
   private boolean _folded   = true ;
   private boolean _selected;
   private File    _base;
   private FileTreeNode _sub, _next;
   public FileTreeNode( File f ){
      _base = f ;
   }
   @Override
   public String       getName(){ return _base.getName() ; }
   @Override
   public TreeNodeable getNext(){return _next ; }
   @Override
   public TreeNodeable getSub(){
      if( _sub != null ) {
          return _sub;
      }
      if( ! _base.isDirectory() ) {
          return null;
      }
      String [] list = _base.list() ;
      if( ( list == null ) || ( list.length == 0 ) ) {
          return null;
      }

      FileTreeNode [] nodes = new FileTreeNode[list.length] ;
      nodes[0] = new FileTreeNode( new File( _base , list[0] ) ) ;
      for( int i = 1 ; i < nodes.length ; i++ ){
         nodes[i] = new FileTreeNode( new File( _base , list[i] ) ) ;
         nodes[i-1].setNextNode( nodes[i] ) ;
      }

      return _sub = nodes[0] ;
   }
   public void setNextNode( FileTreeNode node ){ _next = node ; }
   @Override
   public boolean isContainerNode(){
      return _base.isDirectory() ;
   }
   @Override
   public boolean      isFolded(){ return _folded ; }
   @Override
   public void         switchFold(){
      if( _folded ){
        _folded = false ;
        _sub    = null ;
      }else{
        _folded = true ;
      }
   }
   @Override
   public boolean      isSelected(){ return _selected ; }
   @Override
   public void         setSelected( boolean sel ){ _selected = sel ; }
}
