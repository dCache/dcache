package dmg.util.graphics ;


public interface TreeNodeable {

    public String       getName() ;
    public TreeNodeable getNext() ;
    public TreeNodeable getSub() ;
    public boolean      isFolded() ;
    public void         switchFold() ;
    public boolean      isSelected() ;
    public void         setSelected( boolean sel ) ;
    public boolean      isContainerNode() ;

}
