package dmg.cells.applets.login ;

import java.applet.AppletContext;
import java.awt.BorderLayout;
import java.awt.CardLayout;
import java.awt.Component;
import java.awt.Font;
import java.awt.Frame;
import java.awt.Panel;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowEvent;
import java.awt.event.WindowListener;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.StringTokenizer;

import org.dcache.util.Args;

public class      SshLoginMain
       extends    Frame
       implements WindowListener,
                  ActionListener {

  private static final long serialVersionUID = -818925171004174099L;
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

  private AppletContext    _context;
  private CardLayout       _cardsLayout;
  private CommanderPanel   _commanderPanel;
  private Panel            _switchPanel;
  private PanelSwitchBoard _switchBoard;

  private Font      _font = new Font( "TimesRoman" , 0 , 16 ) ;


  private final static Class<?>[] __userPanelArgs = {
       DomainConnection.class
  } ;
  private final static Class<?>[] __userPanelListener = {
       ActionListener.class
  } ;

  public SshLoginMain( Args args ){
      super( "Ssh Login" ) ;
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

      _switchPanel = new Panel( _cardsLayout = new CardLayout() ) ;
//      _switchPanel.add( _loginPanel  , "login" ) ;

      int panelCounter = 0 ;
      if( _userPanel == null  ){
         _switchBoard = new PanelSwitchBoard(true) ;
         for( int i = 0 ; true ; i++ ){
            String tmp = args.argv( i ) ;
            if( ( tmp == null ) || ( tmp.equals("") ) ) {
                break;
            }
            StringTokenizer st = new StringTokenizer(tmp,":");
            String name = null ;
            try{
                           name    = st.nextToken() ;
               Class<? extends Component> pc = Class.forName(st.nextToken()).asSubclass(Component.class);
               Constructor<? extends Component> pcon = pc.getConstructor(__userPanelArgs) ;
               Object []   argx    = new Object[1] ;
                           argx[0] = _loginPanel ; // is the domain connection
               Component   p       = pcon.newInstance(argx);
               _switchBoard.add( p , name ) ;
               panelCounter ++ ;
            }catch( Exception e ){
               System.err.println( "Skipping "+name+" ("+e+")") ;
               e.printStackTrace() ;
            }
         }
         if( panelCounter == 0 ){
              _userPanel = "dmg.cells.applets.login.CommanderPanel" ;
         }else{
              _switchBoard.addActionListener(this);
              _switchPanel.add( _switchBoard , "commander" ) ;
              _cardsLayout.show( _switchPanel , "commander" ) ;
         }

      }
      if( _userPanel != null ){

         try{
            Class<? extends Component> pc = Class.forName(_userPanel).asSubclass(Component.class);
            Constructor<? extends Component> pcon = pc.getConstructor(__userPanelArgs) ;
            Object []   argx    = new Object[1] ;
                        argx[0] = _loginPanel ;
            Component   p       = pcon.newInstance(argx);
            Method      m       = pc.getMethod( "addActionListener" ,
                                                __userPanelListener ) ;
            argx    = new Object[1] ;
            argx[0] = this ;
            m.invoke( p , argx ) ;
            _switchPanel.add( p , "commander" ) ;
         }catch( Exception e ){
            System.out.println( "Problem while new commander instance : "+e ) ;
            System.exit(1) ;
         }
      }
      //
      // everything is done, so far.
      // initially we want to see the 'loginPanel'.
      //
      setLayout( new CenterLayout() ) ;
      add( _loginPanel ) ;
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
          remove(_loginPanel);
          setLayout(new BorderLayout());
          add(_switchPanel, "Center");
          validate();
          break;
      case "disconnected":
//         _cardsLayout.show( _switchPanel , "login" ) ;
          remove(_switchPanel);
          setLayout(new CenterLayout());
          add(_loginPanel);
          validate();
          break;
      case "exit":
      case "Exit":
          _loginPanel.logout();
          _cardsLayout.show(_switchPanel, "login");
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

         new SshLoginMain( new Args( args ) ) ;

      }catch( Exception e ){
         e.printStackTrace() ;
         System.err.println( "Connection failed : "+e.toString() ) ;
         System.exit(4);
      }

   }

}
