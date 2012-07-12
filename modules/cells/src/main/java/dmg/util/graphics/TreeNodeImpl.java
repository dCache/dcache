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
    @Override
    public String       getName(){  return _name ; }
    @Override
    public TreeNodeable getNext(){  return _next ; }
    @Override
    public TreeNodeable getSub(){   return _sub ; }
    @Override
    public boolean      isFolded(){ return _folded ; }
    @Override
    public void         switchFold(){ _folded = ! _folded ; }
    @Override
    public boolean      isSelected(){ return _selected ; }
    @Override
    public void         setSelected( boolean sel ){ _selected = sel ; }
    @Override
    public boolean      isContainerNode(){ return _sub != null  ; }

}
