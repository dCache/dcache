package dmg.cells.applets.login ;

import java.awt.BorderLayout;
import java.awt.Font;
import java.awt.Frame;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowEvent;
import java.awt.event.WindowListener;

import org.dcache.util.Args;

public class      WasteMain
       extends    Frame
       implements WindowListener,
                  ActionListener {

  private static final long serialVersionUID = -2999383363232848520L;
  //
  // remember to change the following variable to your needs
  //
  private SshLoginPanel     _loginPanel;

  private String    _remoteHost;
  private String    _remotePort;
  private String    _remoteUser;
  private String    _remotePassword;
  private String    _userPanel;
  private String    _title;


  private Font      _font = new Font( "TimesRoman" , 0 , 16 ) ;


  public WasteMain( Args args ){
      super( "CellAlias" ) ;
      setLayout( new BorderLayout() ) ;
//      setLayout( new CenterLayout() ) ;
      addWindowListener( this ) ;
      setLocation( 60 , 60) ;
//
//    get all necessay infos ...
//
      _remoteHost = args.getOpt( "host" ) ;
      _remotePort = args.getOpt( "port" ) ;
      _remoteUser = args.getOpt( "user" ) ;
      _title      = args.getOpt( "title" ) ;

      //
      // we allow to set the "panel" :
      //    only this panel is displayed , or
      // we allow to have panel1, panel2 ...
      //    in this  case the syntax has to be
      //       param name="panel1" value="<name>:<panelClass>"
      //
      _userPanel  = args.getOpt( "panel" ) ;

      _remoteHost = _remoteHost==null?"":_remoteHost ;
      _remotePort = _remotePort==null?"":_remotePort ;
      _remoteUser = _remoteUser==null?"":_remoteUser ;
      _title      = _title==null?"Login":_title;

      _loginPanel = new SshLoginPanel() ;
      _loginPanel.setHost( _remoteHost , true , true ) ;
      _loginPanel.setPort( _remotePort , true , true ) ;
      _loginPanel.setUser( _remoteUser , true , true ) ;
      _loginPanel.setTitle( _title ) ;

      _loginPanel.addActionListener( this ) ;

       _loginPanel.ok() ;

      add( _loginPanel , "Center" ) ;
      setSize( 750 , 500 ) ;
      pack() ;
      setSize( 750 , 500 ) ;
      setVisible( true ) ;
  }
 @Override
 public synchronized void actionPerformed( ActionEvent event ){
     String command = event.getActionCommand() ;
     System.out.println( "Action Applet : "+command ) ;
     switch (command) {
     case "connected":
//         _cardsLayout.show( _switchPanel , "commander" ) ;
         break;
     case "disconnected":
//         _cardsLayout.show( _switchPanel , "login" ) ;
         break;
     case "exit":
         _loginPanel.logout();
         break;
     }
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
      if( args.length < 1 ){
         System.err.println( "Usage : ... {-host=<hostname> -port=<port>]<panels>" ) ;
         System.exit(4) ;
      }
     try{

         new WasteMain( new Args( args ) ) ;

      }catch( Exception e ){
         e.printStackTrace() ;
         System.err.println( "Connection failed : "+e.toString() ) ;
         System.exit(4);
      }

   }

}
