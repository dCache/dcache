package dmg.cells.applets.login ;

import java.applet.Applet;
import java.applet.AppletContext;
import java.awt.CardLayout;
import java.awt.Component;
import java.awt.Font;
import java.awt.Panel;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.StringTokenizer;

public class      SshLoginApplet
       extends    Applet
       implements ActionListener  {

    private static final long serialVersionUID = 5453691123861016299L;
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

  @Override
  public void init(){
      System.out.println( "init ..." ) ;
      _context = getAppletContext() ;
//
//    get all necessay infos ...
//
      _remoteHost = getParameter( "host" ) ;
      _remotePort = getParameter( "port" ) ;
      _remoteUser = getParameter( "user" ) ;
      _title      = getParameter( "title" ) ;

      //
      // we allow to set the "panel" :
      //    only this panel is displayed , or
      // we allow to have panel1, panel2 ...
      //    in this  case the syntax has to be
      //       param name="panel1" value="<name>:<panelClass>"
      //
      _userPanel  = getParameter( "panel" ) ;

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

      setLayout( new CenterLayout() ) ;
      _loginPanel.ok() ;

      _switchPanel = new Panel( _cardsLayout = new CardLayout() ) ;
      _switchPanel.add( _loginPanel  , "login" ) ;

      int panelCounter = 0 ;
      if( _userPanel == null  ){
         _switchBoard = new PanelSwitchBoard(true) ;
         for( int i = 0 ; true ; i++ ){
            String tmp = getParameter( "panel"+i ) ;
            if( ( tmp == null ) || ( tmp.equals("") ) ) {
                break;
            }
            StringTokenizer st = new StringTokenizer(tmp,":");
            String name = null ;
            try{
                           name    = st.nextToken() ;
               Class<? extends Component> pc = Class.forName(st.nextToken()).asSubclass(Component.class);
               Constructor<? extends Component> pcon = pc.getConstructor(__userPanelArgs);
               Object []   args    = new Object[1] ;
                           args[0] = _loginPanel ; // is the domain connection
               Component   p       = pcon.newInstance(args);
               _switchBoard.add( p , name ) ;
               panelCounter ++ ;
            }catch( Exception e ){
               System.err.println( "Skipping "+name+" ("+e+")") ;
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
            Object []   args    = new Object[1] ;
                        args[0] = _loginPanel ;
            Component   p       = pcon.newInstance(args) ;
            Method      m       = pc.getMethod( "addActionListener" ,
                                                __userPanelListener ) ;
            args    = new Object[1] ;
            args[0] = this ;
            m.invoke( p , args ) ;
            _switchPanel.add( p , "commander" ) ;
         }catch( Exception e ){
            System.out.println( "Problem while new commander instance : "+e ) ;
            System.exit(1) ;
         }
      }

      _cardsLayout.show( _switchPanel , "login" ) ;
      add( _switchPanel ) ;
  }
  @Override
  public void start(){
      System.out.println("start ..." ) ;
      validate() ;
      repaint() ;
      //
      // get the parameter from the html file
      //
  }
  @Override
  public void stop(){
      System.out.println("stop ..." ) ;
  }
  @Override
  public void destroy(){
      System.out.println("destroy ..." ) ;
//      try{
//      }catch( Exception e ){}
  }
  @Override
  public synchronized void actionPerformed( ActionEvent event ){
     String command = event.getActionCommand() ;
     System.out.println( "Action Applet : "+command ) ;
      switch (command) {
      case "connected":
          _cardsLayout.show(_switchPanel, "commander");
          break;
      case "disconnected":
          _cardsLayout.show(_switchPanel, "login");
          break;
      case "exit":
          _loginPanel.logout();
          _cardsLayout.show(_switchPanel, "login");
          break;
      }
  }

}
