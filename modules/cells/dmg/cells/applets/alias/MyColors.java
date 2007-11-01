package dmg.cells.applets.alias ;

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

public class      MyColors 
       extends    Frame 
       implements WindowListener, 
                  AdjustmentListener {

   private class BorderPanel extends Panel {
      public BorderPanel( Component c ){
         super( new BorderLayout() ) ;
         add( c , "Center" ) ;
      }
     
      public Insets getInsets(){ return new Insets( 10 , 10 ,10 , 10 ) ; }
   
   }
   private class Dummy extends Canvas implements MouseMotionListener {
       public Dummy(){
           addMouseMotionListener( this ) ;
       }
       public void mouseMoved( MouseEvent event ){
           int x = event.getX() ;
           Dimension d = getSize() ;
           float w = (float)x / (float)d.width  ;
           int [] z = new int[3] ;
           for( int i = 0 ; i < 3 ; i++ ){
              z[i] = (int)(( w * ( ((float)_cValues[3+i])/256. - 
                           ((float)_cValues[i])/256.     ) 
                         + ((float)_cValues[i])/256. ) * 256. );
              _colorValue[i+6].setText( ""+z[i] ) ;
           }
           _bottomLabel.setBackground( new Color(z[0],z[1],z[2]) );
           _bottomLabel.setForeground( new Color(255-z[0],255-z[1],255-z[2]) ) ;
//           System.out.println( "w="+w+";z[0]="+z[0]+";z[1]="+z[1]+";z[2]="+z[2]);
       }
       public void mouseDragged( MouseEvent event ){
       
       }
       public void doIt(){ repaint() ; }
       public void paint( Graphics g ){
           int [] z = new int[3] ;
           Dimension d = getSize() ;
           for( int x = 0 ; x < d.width ; x++){
              float w = (float)x / (float)d.width  ;
              for( int i = 0 ; i < 3 ; i++ ){
                 z[i] = (int)(( w * ( ((float)_cValues[3+i])/256. - 
                              ((float)_cValues[i])/256.     ) 
                            + ((float)_cValues[i])/256. ) * 256. );
              }
              g.setColor(new Color( z[0]  , z[1] , z[2] )) ;
              g.drawLine( x , 0 , x , d.height-1 ) ;
           }
//           setBackground( new Color( z[0]  , z[1] , z[2] )  ) ;
       }
   }
   private Font   _bigFont = 
             new Font( "SansSerif" , Font.BOLD , 18 )  ; 
   private Font   _bigFont2 = 
             new Font( "SansSerif" , Font.BOLD , 24 )  ;
    
   private static final String [] __colors = { "Red" , "Green" , "Blue" } ; 
   private static final Color  [] __col = { Color.red , Color.green , Color.blue  } ;
   private Label []     _colorName  = new Label[3] ;
   private Label []     _colorValue = new Label[9] ;
   private Scrollbar [] _glider     = new Scrollbar[6] ;
   private int       [] _cValues    = new int[6] ;
   private Dummy        _canvas     = new Dummy() ;
   private Color        _ourChoice  = Color.black ;
   private Label        _bottomLabel = new Label("ThisColor",Label.CENTER) ;
  public MyColors( ){
      super( "Colors" ) ;
      setLayout( new BorderLayout() ) ;
      addWindowListener( this ) ;
      setLocation( 60 , 60) ;

      for( int i = 0 ; i < 3 ; i++ ){
         _colorName[i]  = new Label( __colors[i] , Label.CENTER ) ;
         _colorName[i].setFont( _bigFont ) ;
         _colorName[i].setForeground( __col[i] ) ;
         _colorValue[i] = new Label( "0" , Label.CENTER ) ;
         _colorValue[i].setFont( _bigFont ) ;
         _colorValue[i].setForeground( __col[i] ) ;
         _colorValue[3+i] = new Label( "0" , Label.CENTER ) ;
         _colorValue[3+i].setFont( _bigFont ) ;
         _colorValue[3+i].setForeground( __col[i] ) ;
         _colorValue[6+i] = new Label( "0" , Label.CENTER ) ;
         _colorValue[6+i].setFont( _bigFont ) ;
         _colorValue[6+i].setForeground( __col[i] ) ;
         _glider[i]     = new Scrollbar(Scrollbar.HORIZONTAL ,
                                     0, 1 , 0 , 256 ) ;
         _glider[i].addAdjustmentListener( this ) ;
         _glider[3+i]     = new Scrollbar(Scrollbar.HORIZONTAL ,
                                     0, 1 , 0 , 256 ) ;
         _glider[3+i].addAdjustmentListener( this ) ;
      }
      Panel gPanel = new Panel( new GridLayout(0,3) ) ;
      for(int i=0;i<3;i++)gPanel.add(_colorName[i]) ;
      for(int i=0;i<3;i++)gPanel.add(_colorValue[i]) ;
      for(int i=0;i<3;i++)gPanel.add( new BorderPanel(_glider[i])) ;
      for(int i=3;i<6;i++)gPanel.add(_colorValue[i]) ;
      for(int i=3;i<6;i++)gPanel.add( new BorderPanel(_glider[i])) ;
      for(int i=6;i<9;i++)gPanel.add(_colorValue[i]) ;
      add( gPanel , "North" ) ;
      add( _canvas , "Center" ) ;
      add( _bottomLabel , "South" ) ;
      _bottomLabel.setFont(_bigFont2) ;
      setSize( 750 , 400 ) ;
      pack() ;
      setSize( 750 , 400 ) ;
      setVisible( true ) ;
  
  }
  //
  // action interface
  //
  public void adjustmentValueChanged( AdjustmentEvent event ) {
      Scrollbar source = (Scrollbar)event.getSource() ;
      int i ; for( i = 0 ; ( i < 6 ) && ( _glider[i] != source ) ; i++ ) ;
      if( i == 6 )return ;
      int val = source.getValue() ;
      _cValues[i] = val ;
      _colorValue[i].setText( ""+val ) ;
      _ourChoice = new Color( _cValues[0] , _cValues[1] , _cValues[2] ) ;
      _canvas.doIt() ;
      
   }
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
   public static void main( String [] args ){
      try{
            
         new MyColors() ;
      
      }catch( Exception e ){
         e.printStackTrace() ;
         System.err.println( "Connection failed : "+e.toString() ) ;
         System.exit(4);
      }
      
   }

}
