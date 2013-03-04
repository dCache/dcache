package dmg.util.graphics ;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.Frame;
import java.awt.Graphics;
import java.awt.Insets;
import java.awt.Label;
import java.awt.Panel;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowEvent;
import java.awt.event.WindowListener;
import java.io.File;

import dmg.cells.applets.login.CenterLayout;

public class      AnyFrame
       extends    Frame
       implements WindowListener, ActionListener   {

   private static final long serialVersionUID = -7213311976976442647L;
   private Font   _bigFont =
             new Font( "SansSerif" , Font.BOLD , 18 )  ;
   private Font   _bigFont2 =
             new Font( "SansSerif" , Font.BOLD , 24 )  ;

  private class NicePanel extends Panel {
      private static final long serialVersionUID = 6676682889053190167L;

      private NicePanel( int columns ){
        TableLayout tl = new TableLayout( columns ) ;
        tl.setHgap(20) ;
        tl.setVgap(10) ;
        setLayout( tl ) ;
     }
     @Override
     public Insets getInsets(){ return new Insets( _b , _b ,_b , _b ) ; }
     private int _b = 7 ;
     @Override
     public void paint( Graphics g ){
        Dimension   d    = getSize() ;
        g.setColor( Color.blue ) ;
        g.drawRect( _b/2 , _b/2 , d.width-_b , d.height-_b ) ;
     }
  }
  public AnyFrame(  ){
      super( "Any Frame" ) ;
      setLayout( new CenterLayout() ) ;
      addWindowListener( this ) ;
      setLocation( 60 , 60) ;

      Panel p = new NicePanel( 3 ) ;
      p.setBackground( Color.yellow ) ;
      add( p , "South") ;
      Label x;
      p.add( x = new Label( "1" , Label.CENTER )  ) ;
      x.setBackground( Color.green ) ;
      p.add( x = new Label( "11" , Label.CENTER )  ) ;
      x.setFont( _bigFont ) ;
      x.setBackground( Color.green ) ;
      p.add( x = new Label( "111" , Label.CENTER )  ) ;
      x.setBackground( Color.green ) ;
      p.add( x = new Label( "1111" , Label.CENTER )  ) ;
      x.setBackground( Color.green ) ;
      p.add( new Label( "11111" , Label.CENTER )  ) ;
      p.add( new Label( "11111" , Label.CENTER )  ) ;
      p.add( x = new Label( "11111" , Label.CENTER )  ) ;
      x.setFont( _bigFont2 ) ;
      x.setBackground( Color.green ) ;

      TreeCanvas tc = new TreeCanvas() ;
      /*
      TreeNodeImpl otto = new TreeNodeImpl( "Otto" ) ;
      TreeNodeImpl karl = new TreeNodeImpl( "karl" ) ;
      TreeNodeImpl fritz = new TreeNodeImpl( "fritz-is-a-very-long-word" ) ;
      TreeNodeImpl karlNext = new TreeNodeImpl( "karlNext" ) ;
      TreeNodeImpl ottoNext = new TreeNodeImpl( "OttoNext" ) ;
      TreeNodeImpl ottoNextNext = new TreeNodeImpl( "OttoNextNext" ) ;
      otto._next = ottoNext ;
      ottoNext._next = ottoNextNext ;
      otto._sub = karl  ;
      karl._next = karlNext ;
      karl._sub = fritz ;
      ottoNextNext._sub = karl ;
      ottoNextNext._next = new TreeNodeImpl( "last" ) ;
      tc.setTree(otto) ;
      */


      FileTreeNode ftn = new FileTreeNode( new File("/etc") ) ;
      /*
      tc.setTree(ftn);
      &/
      ScrollPane sp = new ScrollPane() ;
      sp.add( tc ) ;
      sp.setSize( 200 , 300  ) ;
      p.add( sp ) ;
      */

      TreeNodePanel tnp = new TreeNodePanel() ;
      tnp.setTree( ftn ) ;
      tnp.setSize( 300 , 300 ) ;
      p.add( tnp ) ;

//      p.add( new Label( "Medium-3" )  ) ;
//      p.add( new Label( "Bottom-1" )  ) ;
//      p.add( new Label( "Bottom-2" )  ) ;

      setSize( 750 , 500 ) ;
      pack() ;
      setSize( 750 , 500 ) ;
      setVisible( true ) ;

  }
  private int _counter = 1000 ;
  @Override
  public void actionPerformed( ActionEvent event ){
  }
  //
  // window interface
  //
  @Override
  public void windowOpened( WindowEvent event ){}
  @Override
  public void windowClosed( WindowEvent event ){
      System.exit(0);
  }
  @Override
  public void windowClosing( WindowEvent event ){
      System.exit(0);
  }
  @Override
  public void windowActivated( WindowEvent event ){}
  @Override
  public void windowDeactivated( WindowEvent event ){}
  @Override
  public void windowIconified( WindowEvent event ){}
  @Override
  public void windowDeiconified( WindowEvent event ){}
   public static void main( String [] args ){
      try{

         new AnyFrame() ;

      }catch( Exception e ){
         e.printStackTrace() ;
         System.err.println( "Connection failed : "+e.toString() ) ;
         System.exit(4);
      }

   }

}
