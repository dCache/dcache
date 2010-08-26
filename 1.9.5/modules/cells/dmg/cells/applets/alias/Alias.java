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

public class      Alias 
       extends    Frame 
       implements WindowListener, 
                  ActionListener,
                  ItemListener     {
/*
   private Button _closeButton      = null ;
   private Panel  _domainListPanel  = null ;
*/   
   private class BorderPanel extends Panel {
      public BorderPanel( Component c ){
         super( new BorderLayout() ) ;
         add( c , "Center" ) ;
      }
     
      public Insets getInsets(){ return new Insets( 10 , 10 ,10 , 10 ) ; }
   
   }
   private class Dummy extends Canvas {
       public Dummy(){
       
       }
       public void paint( Graphics g ){
           Dimension d = getSize() ;
           int dx = d.width / 2 ;
           int dy = d.height / 2 ;
           g.setColor( Color.red ) ;
           int off = 0 ;
           for( int i = 0 ;i < 5 ; i++ ){
              g.drawRect( off  , off , 
                          d.width - 1 - 2*off , 
                          d.height - 1 - 2 * off ) ;
              off += 5 ;
           }
       }
   }
   private class SwitchPanel extends Panel {
      private CardLayout _cards = new CardLayout() ;
     
      public SwitchPanel(){
         setLayout( _cards ) ;
         String name = null ;
         Panel panel = null ;
         Enumeration e = _panelHash.keys() ;
         for( ; e.hasMoreElements() ; ){
             name  = (String)e.nextElement() ;
             panel = (Panel)_panelHash.get( name ) ;
             add( panel , name ) ;
         }
         add( new Dummy() , "dummy" ) ;
      }
      public void showIt( String name ){ _cards.show(this,name) ; }
      public void setEnabled( boolean en ){
         if( ! en ){
            _cards.show( this , "dummy" ) ; 
         }
      }
   }
   private SwitchPanel _switchPanel ;
   private AliasDomainConnection _aliasConnection  ;
   private Hashtable _panelHash     = new Hashtable() ;
   private java.awt.List _cellList  = new java.awt.List() ;
   private Label     _messageText   = new Label() ;
   private Button    _connectButton = new Button( "Connect" ) ;
   private Label     _cellLabel     = new Label("Cells",Label.CENTER) ;
   private Label     _topLabel      = 
          new Label( "Cell Alias Controller" ,Label.CENTER ) ;
   private Font   _bigFont = 
             new Font( "SansSerif" , Font.BOLD , 18 )  ; 
   private Font   _bigFont2 = 
             new Font( "SansSerif" , Font.BOLD , 24 )  ; 
  public Alias( String host , int port , String [] cellList ){
      super( "CellAlias" ) ;
      setLayout( new BorderLayout() ) ;
      addWindowListener( this ) ;
      setLocation( 60 , 60) ;
      _aliasConnection = new AliasDomainConnection( host , port ) ;
      scanCellList( cellList ) ;
      
      _switchPanel = new SwitchPanel() ;
      _connectButton.addActionListener( this ) ;
      _cellList.addItemListener( this ) ;
      
      Panel leftPanel = new Panel( new BorderLayout() ) ;
      Panel leftTop   = new Panel( new GridLayout(0,1) ) ;
      leftTop.add( new BorderPanel( _connectButton ) ) ;
      leftTop.add( _cellLabel ) ;
      
      _cellLabel.setFont( _bigFont ) ;
      
      leftPanel.add( leftTop   , "North" ) ;
      leftPanel.add( _cellList , "Center" ) ;
      
      _topLabel.setFont( _bigFont2 ) ;
      _topLabel.setForeground( Color.blue ) ;
      add( new BorderPanel( _topLabel ) , "North" ) ;
      add( new BorderPanel( _switchPanel ) , "Center" ) ;
      add( new BorderPanel( leftPanel )    , "West" ) ;
      add( _messageText ,"South" ) ;
      
      _cellList.setEnabled(false);
      _switchPanel.setEnabled(false);
      
      _messageText.setBackground( Color.red ) ;

      setSize( 750 , 500 ) ;
      pack() ;
      setSize( 750 , 500 ) ;
      setVisible( true ) ;
  
  }
  private static final Class [] __panelArgs = {
     java.lang.String.class ,
     dmg.cells.applets.alias.AliasDomainConnection.class
  } ;
  private void scanCellList( String [] cellList ){
     int p ;
     String cell = null , cellName = null , className = null ;
     Class   c = null ;
     Constructor con = null ;
     Object o = null ;
     Object [] argList = new Object[2] ;
     for( int i= 0 ; i < cellList.length ; i ++ ){
        
        cell = cellList[i] ;
        if( ( cell == null                  ) ||
            ( cell.length() < 1             ) ||
            ( cell.charAt(0) == '#'         ) ||
            ( ( p = cell.indexOf(":") ) < 1 ) ||
            ( p == (cell.length() - 1 )     )   )continue ;
        
        cellName  = cell.substring(0,p) ;
        className = cell.substring(p+1) ;
        try{
           c   = Class.forName( className ) ;
           con = c.getConstructor( __panelArgs ) ;
           argList[0] = cellName ;
           argList[1] = _aliasConnection ;
           o = con.newInstance( argList ) ;
           if( ! ( o instanceof CellMessageListener ) )
              throw new 
              IllegalArgumentException( cellName +" not an AL" ) ;
           _panelHash.put( cellName , o ) ;
           _cellList.add( cellName ) ;
        }catch( Exception e ){
           o = null ;
           System.err.println( "Problem creating "+cell+":"+e ) ;
           System.out.println( "Failed : "+cellName+":"+className ) ;
           continue ;
        }
        System.out.println( "O.K. : "+cellName+":"+className ) ;
     
     }
  }
  //
  // action interface
  //
  public void actionPerformed( ActionEvent event ) {
    Object source = event.getSource() ;
    
    if( source == _connectButton ){
        try{
           if( _connectButton.getLabel().equals("Connect") ){
              _aliasConnection.connect() ;
              _connectButton.setLabel( "DisConnect" ) ;
              _cellList.setEnabled(true) ;
              _switchPanel.setEnabled(true) ;
              sendAliases() ;
           }else{
              _aliasConnection.disconnect() ;
              _connectButton.setLabel( "Connect" ) ;
              _cellList.setEnabled(false) ;
              _switchPanel.setEnabled(false) ;
           }
        }catch( Exception e ){
           _messageText.setText( "connection problem : "+e ) ;
           return ;
        }
        _messageText.setText("O.K.") ;
    }else if( source == _cellList ){
//       String sel = _cellList.getSelectedItem() ;
//       if( sel == null )return ;
//       _switchPanel.showIt( sel ) ;
    }
    
  
  }
  private void sendAliases(){
     try{
        Enumeration e = _panelHash.keys() ;
        while( e.hasMoreElements() ){
            String name = (String)e.nextElement() ;
            System.out.println(" Adding alias : "+name ) ;
            _aliasConnection.addAlias( 
                  name , 
                  (CellMessageListener)_panelHash.get(name)) ;
        }
     }catch(Exception ee ){
        System.out.println( "Problem sending alias : "+ee ) ;
     }
  }
  public void itemStateChanged( ItemEvent event ){
/*
     System.out.println( "itemStateChanged : "+event.getItem().getClass()  ) ;
     Object o = event.getItem() ;
     if( o instanceof Integer ){
        int pos = ((Integer)o).intValue() ;
        _switchPanel.showIt( _cellList.getItem( pos ) ) ;
     }
*/
      ItemSelectable sel = event.getItemSelectable() ;
      Object [] obj = sel.getSelectedObjects() ;
      if( ( obj == null ) || ( obj.length == 0 ) )return ;
      _switchPanel.showIt( obj[0].toString() ) ;
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
      if( args.length < 2 ){
         System.err.println( "Usage : ... <domainHostName> <domainPort>" ) ;
         System.exit(4) ;
      }
      /*
      if( args.length > 2 ){
         Properties props = System.getProperties() ;
         props.put( "bw" , args[2] ) ;     
      }
      */
      int port = Integer.parseInt( args[1] ) ;
      int rest = args.length - 2 ;
      String [] cellList = new String[0] ;
      if( rest > 0 ){
         cellList = new String[rest] ;
         for( int j = 0 ; j < rest ; j++ )
            cellList[j] = args[2+j] ;
         
      }
      try{
            
         new Alias( args[0] , port , cellList ) ;
      
      }catch( Exception e ){
         e.printStackTrace() ;
         System.err.println( "Connection failed : "+e.toString() ) ;
         System.exit(4);
      }
      
   }

}
