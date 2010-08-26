package dmg.util.graphics ;


public class TreeNodeImpl implements TreeNodeable {
   public TreeNodeable _next = null , _sub = null ;
   private String  _name     = null ;
   private boolean _folded   = true ;
   private boolean _selected = false ;
   public TreeNodeImpl( String name ){ _name = name ; }
   //
   // the TreeNodeable interface 
   //
    public String       getName(){  return _name ; } 
    public TreeNodeable getNext(){  return _next ; }
    public TreeNodeable getSub(){   return _sub ; }
    public boolean      isFolded(){ return _folded ; }
    public void         switchFold(){ _folded = ! _folded ; }
    public boolean      isSelected(){ return _selected ; }
    public void         setSelected( boolean sel ){ _selected = sel ; }
    public boolean      isContainerNode(){ return _sub != null  ; }

}
