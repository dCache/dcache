package dmg.apps.osm  ;


import java.applet.*;
import java.awt.* ;
import java.awt.event.* ;
import java.util.* ;
import java.io.* ;
import java.net.* ;

import dmg.util.* ;
import dmg.cells.services.* ;
import dmg.cells.nucleus.* ;
import dmg.cells.applets.spy.* ;

public class      GraphicTest 
       extends    Frame 
       implements WindowListener, 
                  ActionListener  {

   private Label  _messageText      = null ;
   private SStatusCanvas _status    = null ;
   private class BorderPanel extends Panel {
      private int _border = 30 ;
     
      private String _title = "" ;
      private Font _basicFont = new Font( "SansSerif" ,
                                   0 , 
                                   _border-6 )  ;
      public BorderPanel( Component c , String title ){
         super( new BorderLayout() ) ;
         _title = title ;
         add( c , "Center" ) ;
      }
     
      public Insets getInsets(){ 
          return new Insets( _border,_border,_border,_border ) ;
      }
      public void paint( Graphics g ){
          Dimension d = getSize() ;
          g.setFont(_basicFont);
          FontMetrics fm = g.getFontMetrics() ;
          int offset = ( _border - (fm.getAscent()+fm.getDescent()))/2 ;
          offset = Math.max( 0 , offset ) ;
          g.drawString(_title,_border,offset+fm.getAscent());
          int o = _border/2 ;
          int w = fm.stringWidth(_title) ;
          g.drawLine(o,o,_border-3,o) ;
          g.drawLine(_border+w+3,o,d.width-1-o,o) ;
          g.drawLine(d.width-1-o,o,d.width-1-o,d.height-1-o) ;
          g.drawLine(d.width-1-o,d.height-1-o,o,d.height-1-o) ;
          g.drawLine(o,o,o,d.height-1-o) ;
      }
   
   }
   
  public GraphicTest( String [] args  ){
      super( "GraphicTest" ) ;
      
      addWindowListener( this ) ;
      
      Panel masterPanel = new Panel( new BorderLayout() ) ;      
      _messageText   = new Label("MessagePanel") ;
      _status        = new SStatusCanvas() ;
      BorderPanel bp = new BorderPanel(_status,"title") ;
      bp.setBackground(Color.orange) ;
      masterPanel.add( bp , "Center" ) ;
      masterPanel.add( _messageText , "South" ) ;
      
      add( masterPanel ) ;
      
      setLocation( 100  , 100 ) ;
      setSize( 400 , 200 ) ;
      pack() ;
      setSize( 400 , 200 ) ;
      setVisible( true ) ;
  
  }
  //
  // action interface
  //
  //
  // domain connection interfase
  //
  //
  // window interface
  //
  public void windowOpened( WindowEvent event ){}
  public void windowClosed( WindowEvent event ){
      System.exit(0);
  }
  public void windowClosing( WindowEvent event ){
      System.exit(0);
  }
  public void windowActivated( WindowEvent event ){}
  public void windowDeactivated( WindowEvent event ){}
  public void windowIconified( WindowEvent event ){}
  public void windowDeiconified( WindowEvent event ){}
  public void actionPerformed( ActionEvent event ) {
    Object source = event.getSource() ;
  
  }
   public static void main( String [] args ){
      try{
            
         new GraphicTest( args ) ;
      
      }catch( Exception e ){
         e.printStackTrace() ;
         System.exit(4);
      }
      
   }

}
