package dmg.cells.applets.alias ;

import dmg.cells.applets.login.CenterLayout ;
import java.lang.reflect.* ;
import java.applet.*;
import java.awt.* ;
import java.awt.event.* ;
import java.util.* ;
import java.io.* ;
import java.net.* ;

import dmg.util.* ;
import dmg.cells.services.* ;
import dmg.cells.nucleus.* ;

public class AliasEditorPanel 
       extends Panel 
       implements ActionListener,
                  ItemListener,
                  TextListener    {
                  
  private int _b = 5 ;
  public Insets getInsets(){ return new Insets(_b , _b ,_b , _b) ; }
  public void paint( Graphics g ){

     Dimension   d    = getSize() ;
     Color base = getBackground() ;
     g.setColor( Color.white ) ;
     g.drawRect( _b/2 , _b/2 , d.width-_b , d.height-_b ) ;
  }
   private Font   _bigFont = 
             new Font( "SansSerif" , Font.BOLD , 24 )  ; 
             
   private Button    _removeButton = new Button( "Remove Alias" ) ;
   private TextField _aliasField   = new TextField() ;
   private TextField _panelField   = new TextField() ;
   private java.awt.List _aliasList    = new java.awt.List(10) ;
   private Hashtable _aliasHash    = new Hashtable() ;
   private ActionListener _actionListener = null ;
   private final static String __defaultPanelClass = 
           "dmg.cells.applets.alias.MultiRequestPanel" ;
   public AliasEditorPanel( ){
      super( new CenterLayout() ) ;
      
      _removeButton.addActionListener( this ) ;
      _aliasField.addActionListener( this ) ;
      _aliasField.addTextListener( this ) ;
      _aliasList.addItemListener( this ) ;
      
      BorderLayout bl = new BorderLayout() ;
      bl.setHgap(10);
      bl.setVgap(10) ;
      
      Panel master    = new Panel( bl ) ;
      
      
      Label tmp = new Label("Active Aliases" , Label.CENTER ) ;
      tmp.setFont( _bigFont ) ;
      
      
      GridLayout gl = new GridLayout(0,1) ;
      gl.setHgap(3) ;
      gl.setVgap(3) ;
      
      Panel buttonPanel = new Panel( gl ) ;      
      buttonPanel.add( new Label( "Cell" , Label.CENTER ) ) ;
      buttonPanel.add( _aliasField ) ;
      buttonPanel.add( new Label( "PanelClass" , Label.CENTER ) ) ;
      _panelField.setText( __defaultPanelClass ) ;
      buttonPanel.add( _panelField ) ;
      buttonPanel.add( _removeButton ) ;
      
      
      master.setLayout( bl ) ;
      master.add( tmp , "North" ) ;
      master.add( _aliasList , "Center" ) ;
      master.add( buttonPanel , "South" ) ;

      add( master ) ;
//      doLayout() ;
//      validateTree() ;
      
   } 
  void addActionListener( ActionListener al ){
      _actionListener = al ;
  }    
  public String [] getAliases(){
       return _aliasList.getItems() ;
  }
  public String [][] getAliasInfos(){
      int size = _aliasHash.size() ;
      String [][] x = new String[size][] ;
      Enumeration e = _aliasHash.keys() ;
      for( int i = 0 ; e.hasMoreElements() ; i++ ){
         x[i] = new String[2] ;
         x[i][0] = (String)e.nextElement() ;
         x[i][1] = (String)_aliasHash.get( x[i][0] ) ;
      }
      return x ;
  }
  public void textValueChanged( TextEvent event ){
     Object source = event.getSource() ;
     if( source == _aliasField ){
        int pos = _aliasList.getSelectedIndex() ;
        if( pos >= 0 )_aliasList.deselect(pos) ;      
        String t = _aliasField.getText() ;
        if( _aliasHash.get(t) != null ){
           _removeButton.setEnabled(true) ;  
        }else{
           _removeButton.setEnabled(false) ;        
        }
           
     }
 
  }            
  public void itemStateChanged( ItemEvent event ){
      ItemSelectable sel = event.getItemSelectable() ;
      Object [] obj = sel.getSelectedObjects() ;
      if( ( obj == null ) || ( obj.length == 0 ) )return ;
      String newAlias = obj[0].toString() ;
      String panelClass = (String)_aliasHash.get(newAlias) ;
      _aliasField.setText( newAlias ) ;
      _panelField.setText( panelClass == null ? "???" : panelClass) ;
  
  }
  private int aliasIndexOf( String str ){
     String [] items =  _aliasList.getItems() ;
     for(int i = 0 ;i < items.length ; i++ )
        if( str.equals( items[i] ) )return i ;
     return -1 ;
  }
  public void actionPerformed( ActionEvent event ) {
    Object source = event.getSource() ;
    if( source == _aliasField ){
       String newAlias = _aliasField.getText() ;
       if( _aliasHash.get( newAlias ) == null ){
          _aliasList.add( newAlias ) ;
          String newPanel = _panelField.getText() ;
          if( newPanel == null )newPanel = __defaultPanelClass ;
          _aliasHash.put( newAlias , newPanel ) ;
          if( _actionListener != null )
            _actionListener.actionPerformed(
                   new ActionEvent(this,0,"a:"+newAlias+":"+newPanel) ) ;
       }
       _aliasField.setText("") ;
       _panelField.setText( __defaultPanelClass ) ;
    }else if( source == _removeButton ){
       String al = _aliasField.getText() ;
       int pos = aliasIndexOf( al ) ;
       if( pos < 0 )return ;
       _aliasHash.remove( al ) ;
       _aliasList.remove( al ) ;
       _aliasField.setText("");
       _panelField.setText( __defaultPanelClass ) ;
       if( _actionListener != null )
         _actionListener.actionPerformed(
                new ActionEvent(this,0,"r:"+al) ) ;
    }
    
  
  }
                 
}
