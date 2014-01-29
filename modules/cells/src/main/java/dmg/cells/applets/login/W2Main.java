package dmg.cells.applets.login ;

import java.awt.Frame;
import java.awt.Label;
import java.awt.TextArea;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowEvent;
import java.awt.event.WindowListener;

import org.dcache.util.Args;

public class      W2Main
       extends    Frame
       implements WindowListener,
                  ActionListener {
  private static final long serialVersionUID = -6724257549082273714L;

  //
  // remember to change the following variable to your needs
  //


  public W2Main( Args args ){
      super( "WWW" ) ;
      RowColumnLayout rcl = new RowColumnLayout(3) ;
      rcl.setFitsAllSizes(1) ;
      setLayout( rcl ) ;
      addWindowListener( this ) ;
      setLocation( 60 , 60) ;

      add( new Label("ddddddddddddddddddddd" ) ) ;
      add( new TextArea(4,4) ) ;
      add( new Label( "dslfk" ) ) ;

      add( new Label("x" ) ) ;
      add( new Label("ddddddddddddddd") ) ;
      add( new Label("D"));
//      add( new HelloPanel("Welcome to Eurogate") ) ;
      setSize( 750 , 500 ) ;
      pack() ;
      setSize( 750 , 500 ) ;
      setVisible( true ) ;
  }
 @Override
 public synchronized void actionPerformed( ActionEvent event ){
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
//      if( args.length < 1 ){
//         System.err.println( "Usage : ... {-host=<hostname> -port=<port>]<panels>" ) ;
//         System.exit(4) ;
//      }
     try{

         new W2Main( new Args( args ) ) ;

      }catch( Exception e ){
         e.printStackTrace() ;
         System.err.println( "Connection failed : "+e.toString() ) ;
         System.exit(4);
      }

   }

}
