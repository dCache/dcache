package dmg.cells.applets.spy ;

import java.applet.*;
import java.awt.* ;
import java.awt.event.* ;
import java.util.* ;
import java.io.* ;
import java.net.* ;

import dmg.util.* ;
import dmg.cells.services.* ;
import dmg.cells.nucleus.* ;
import dmg.cells.network.* ;



class DomainListPanel 
       extends Panel 
      implements ActionListener, FrameArrivable, ItemListener {
      
   private DomainConnection _connection ;
   private Button _updateButton , 
                  _topoButton , 
                  _detailButton ,
                  _contextButton ,
                  _routingButton   ;
   private SpyList       _list ;
   private DomainPanel   _domainPanel ;
   private ContextPanel  _contextPanel ;
   private RoutingPanel  _routingPanel ;
   private CardLayout    _cards = new CardLayout() ;
   private Panel         _cardPanel ;
   private TopologyPanel _topoPanel ;
   private String        _domainMode = "domain" ;
   private boolean       _useColor = false ;
   
   private CellDomainNode [] _nodes = new CellDomainNode[0] ;
   private class LeftPanel extends Panel {
       private LeftPanel(){
          super( new BorderLayout() ) ;
          Label label = new Label( "Domain List" , Label.CENTER ) ;
          label.setFont( new Font( "fixed" , Font.ITALIC , 18 ) ) ;
          _list          = new SpyList() ;
          _updateButton  = new Button( "Update Domain List" ) ;
          _topoButton    = new Button( "Topology" ) ;
          _detailButton  = new Button( "Domain Detail" ) ;
          _contextButton = new Button( "Domain Contex" ) ;
          _routingButton = new Button( "Domain Routing" ) ;
          add( label , "North" ) ;
          add( _list , "Center" ) ;
          Panel buttonPanel = new Panel( new GridLayout(0,1) ) ;
          buttonPanel.add( _updateButton ) ;
          buttonPanel.add( _topoButton ) ;
          buttonPanel.add( _detailButton ) ;
          buttonPanel.add( _contextButton ) ;
          buttonPanel.add( _routingButton ) ;
          add( buttonPanel , "South" ) ;
          if( _useColor )setBackground( Color.yellow ) ;
       }
       public Insets getInsets(){ return new Insets( 10 ,10 ,10 ,10  ) ; }
   }
   DomainListPanel( DomainConnection connection ){
      _useColor   = System.getProperty( "bw" ) == null ;
      _connection = connection ; 
      setLayout( new BorderLayout() ) ;
      if( _useColor )setBackground( Color.blue ) ;
      Panel leftPanel = new LeftPanel() ;
      add( new BorderPanel( leftPanel ) , "West" ) ;
      
      
      _domainPanel    = new DomainPanel( _connection ) ;
      _topoPanel      = new TopologyPanel() ;
      _contextPanel   = new ContextPanel(_connection) ;
      _routingPanel   = new RoutingPanel(_connection) ;
      
      _cardPanel = new Panel( _cards ) ;
      _cardPanel.add( new BorderPanel( _domainPanel ) , "domain" ) ;
      _cardPanel.add( new BorderPanel( _topoPanel )   , "topo" ) ;
      _cardPanel.add( new BorderPanel( _contextPanel )  , "context" ) ;
      _cardPanel.add( new BorderPanel( _routingPanel )  , "routing" ) ;
      _cards.show( _cardPanel , "topo" ) ;
      
      add( _cardPanel , "Center" ) ;
      
      _list.addItemListener( this ) ;
      _updateButton.addActionListener( this ) ;
      _topoButton.addActionListener( this ) ;
      _detailButton.addActionListener( this ) ;
      _contextButton.addActionListener( this ) ;
      _routingButton.addActionListener( this ) ;
      _topoPanel.addActionListener( this ) ;
   }
   public void paint( Graphics g ){
      Dimension   d    = getSize() ;
      Color base = Color.red ;
      for( int i = 0 ; i < 4 ; i++ ){
         g.setColor( base ) ;
         g.drawRect( i , i , d.width-2*i-1 , d.height-2*i-1 ) ;
         base = base.darker() ;
      }
   }
   public Insets getInsets(){ return new Insets( 10 , 10 , 10 ,  10 ) ; }
   public void actionPerformed( ActionEvent event ){
      Object o = event.getSource() ;
      if( o == _updateButton ){
         _connection.send( "topo" , "gettopomap" , this ) ;
         _domainPanel.clear() ;
         _routingPanel.clear() ;
         _contextPanel.clear() ;
         _detailButton.setEnabled(false); 
         _routingButton.setEnabled(false);
         _contextButton.setEnabled(false);
      }else if( o ==  _detailButton ){
         _domainMode = "domain" ;
         _cards.show( _cardPanel , _domainMode ) ;
      }else if( o ==  _topoPanel ){
          _list.select( event.getActionCommand() ) ;
          displayDomain( event.getActionCommand() ) ;
      }else if( o ==  _contextButton ){
         _domainMode = "context" ;
         _cards.show( _cardPanel , _domainMode ) ;
      }else if( o ==  _routingButton ){
         _domainMode = "routing" ;
         _cards.show( _cardPanel , _domainMode ) ;
      }else if( o ==  _topoButton ){
         _cards.show( _cardPanel , "topo" ) ;
      }
   }
   public void frameArrived( MessageObjectFrame frame ){
       Object obj = frame.getObject() ;
       _list.removeAll() ;
       if( ! ( obj instanceof CellDomainNode [] ) )return ;
       _nodes = (CellDomainNode [] )obj ;
       TreeSet sorted = new TreeSet() ;
       for( int i = 0 ; i < _nodes.length ; i++ )
          sorted.add( _nodes[i].getName() );
       Iterator i = sorted.iterator() ;
       while( i.hasNext() )_list.add( (String)i.next() ) ;
       _topoPanel.setTopology( _nodes ) ;
       _cards.show( _cardPanel , "topo" ) ;
   }
   public void itemStateChanged( ItemEvent event ){
      ItemSelectable sel = event.getItemSelectable() ;
      Object [] obj = sel.getSelectedObjects() ;
      if( ( obj == null ) || ( obj.length == 0 ) )return ;
      displayDomain( obj[0].toString() ) ;
   }
   private void displayDomain( String domainName ){
      int i = 0 ;
      for( ; ( i < _nodes.length ) && 
             ( ! _nodes[i].getName().equals( domainName ) ) ; i++ ) ;
      if( i ==  _nodes.length ){
         System.out.println( "No more in list : "+domainName ) ;
         return ;
      }
      _domainPanel.showDomain( _nodes[i] ) ;
      _contextPanel.showDomain( _nodes[i] ) ;
      _routingPanel.showDomain( _nodes[i] ) ;
      _cards.show( _cardPanel , _domainMode ) ;
      _detailButton.setEnabled(true); 
      _routingButton.setEnabled(true);
      _contextButton.setEnabled(true);
   }
   public void setEnabled( boolean enable ){
      if( enable ){
         _connection.send( "topo" , "gettopomap" , this ) ;
      }else{
         _topoPanel.setTopology( new CellDomainNode[0] ) ;
         _cards.show( _cardPanel , "topo" ) ;
         _list.removeAll() ;
      }
      _detailButton.setEnabled(false); 
      _routingButton.setEnabled(false);
      _contextButton.setEnabled(false);
      _updateButton.setEnabled(enable);
      _topoButton.setEnabled(enable);
   }


}
