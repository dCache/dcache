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

public class CellSwitchPanel 
       extends Panel 
       implements ItemListener   {

   private Font   _bigFont = 
             new Font( "SansSerif" , Font.BOLD , 24 )  ; 
   private Font   _bigFont2 = 
             new Font( "SansSerif" , Font.BOLD , 16 )  ; 
  
  private Choice  _cellChoice = new Choice() ;
  private Label   _topLabel   = new Label( "Cell Choise" , Label.CENTER ) ;
  private CardLayout _cards = new CardLayout() ;
  private Panel      _switchPanel = new Panel( _cards ) ;
  private Hashtable  _panelHash   = new Hashtable() ;
  private int _b = 5 ;
  public Insets getInsets(){ return new Insets(_b , _b ,_b , _b) ; }
  public void paint( Graphics g ){

     Dimension   d    = getSize() ;
     Color base = getBackground() ;
     g.setColor( Color.white ) ;
     g.drawRect( _b/2 , _b/2 , d.width-_b , d.height-_b ) ;
  }
  private Panel _topRow = new Panel( new BorderLayout() ) ;
  private final static String __header = "Cell Choice" ;
  public CellSwitchPanel(){
     super( new BorderLayout() ) ;
 
     _cellChoice.setFont( _bigFont ) ;
     _cellChoice.addItemListener( this ) ;
     _cellChoice.add( __header ) ;
     _topLabel.setFont( _bigFont ) ;
     _topRow.add( _cellChoice , "West" ) ;
     _topRow.add( _topLabel , "Center" ) ;
//     _topRow.add( new Label("x") , "East" ) ;


     _switchPanel.add( new Dummy(20) , __header ) ; 
     
     add( _switchPanel , "Center" ) ;
     add( _topRow , "North" ) ;
  }
  
  public void removePanel( String name ){
     Component c = (Component)_panelHash.remove( name ) ;
     if( c == null )return ;
     _cellChoice.remove(name) ;
     _switchPanel.remove(c) ;
     _switchPanel.doLayout() ;
     _cellChoice.select( __header ) ;
     _cards.show( _switchPanel , __header ) ;
     doLayout() ;
  }
  public void addPanel( String name , Component panel ){
     if( _panelHash.get( name ) == null ){
        _panelHash.put( name , panel ) ;
        _cellChoice.add( name ) ;
        _cellChoice.doLayout() ;
        _switchPanel.add( panel , name ) ; 
        _switchPanel.doLayout() ; 
        _topRow.doLayout() ;
        _topRow.validate() ;
        validateTree() ;  
     }
  }
  public void itemStateChanged( ItemEvent event ){
      ItemSelectable sel = event.getItemSelectable() ;
      Object [] obj = sel.getSelectedObjects() ;
      if( ( obj == null ) || ( obj.length == 0 ) )return ;
      if( ( obj instanceof Object []   ) &&
          ( ((Object[])obj).length > 0 )     ){
         Object []a = (Object[])obj ;
         _cards.show( _switchPanel , a[0].toString() ) ;
      }
  }
   private class Dummy extends Canvas {
       int _count = 5 ;
       public Dummy( int count ){ _count = count ; }
       public Dummy(){
           _count = 5 ;
       }
       public void paint( Graphics g ){
           Dimension d = getSize() ;
           int dx = d.width / 2 ;
           int dy = d.height / 2 ;
           g.setColor( Color.red ) ;
           int off = 0 ;
           for( int i = 0 ;i < _count ; i++ ){
              g.drawRect( off  , off , 
                          d.width - 1 - 2*off , 
                          d.height - 1 - 2 * off ) ;
              off += 5 ;
           }
       }
   }
       
}
                  
 
