package dmg.cells.applets.spy ;

import java.awt.* ;
import java.awt.event.* ;
import java.util.* ;

import dmg.cells.services.* ;
import dmg.cells.nucleus.* ;
import dmg.cells.network.* ;



class DomainPanel 
      extends Panel 
      implements ActionListener, FrameArrivable, ItemListener {

   private static final long serialVersionUID = -2086357792768952598L;
   private DomainConnection _connection ;
   private Button   _updateButton , _detailButton , _commandButton ;
   private SpyList  _list ;
   private Label    _topLabel  ;
   private CellDomainNode _domainNode;
   private LeftPanel      _leftPanel ;
   private CellPanel      _cellPanel ;
   private CommandPanel   _commandPanel ;
   
   private CellInfo []  _infos = new CellInfo[0] ;
   private CardLayout   _cards = new CardLayout() ;
   private Panel        _cardPanel ;
   private String       _cellMode = "cell" ;
   private boolean      _useColor;
   
   private class LeftPanel extends Panel {
       private static final long serialVersionUID = 8626662575716491546L;

       private LeftPanel(){
          super( new BorderLayout() ) ;
          _list = new SpyList() ;
          _updateButton  = new Button( "Update Cell List" ) ;
          _detailButton  = new Button( "Cell Detail" ) ;
          _commandButton = new Button( "Commander" ) ;
          Label label = new Label( "Cell List" , Label.CENTER ) ;
          label.setFont( new Font( "fixed" , Font.ITALIC , 18 ) ) ;
          add( label , "North" ) ;
          add( _list , "Center" ) ;
          Panel buttonPanel = new Panel( new GridLayout(0,1) ) ;
          buttonPanel.add( _updateButton ) ;
          buttonPanel.add( _detailButton ) ;
          buttonPanel.add( _commandButton ) ;
          
          add( buttonPanel , "South" ) ;
          if( _useColor ) {
              setBackground(Color.green);
          }
       }
       @Override
       public Insets getInsets(){ return new Insets( 10 ,10 ,10 ,10  ) ; }
   }
   DomainPanel( DomainConnection connection ){
      _connection = connection ; 
      _useColor = System.getProperty( "bw" ) == null ;
      if( _useColor ) {
          setBackground(Color.red);
      }
      setLayout( new BorderLayout() ) ;
      _leftPanel = new LeftPanel() ;
      _topLabel  = new Label( "Domain" , Label.CENTER )  ;
      _topLabel.setFont( new Font( "fixed" , Font.ITALIC , 18 ) ) ;
      add( _topLabel , "North" ) ;
      add( new BorderPanel( _leftPanel ) , "West" ) ;
      
      _cellPanel    = new CellPanel( _connection ) ;
      _commandPanel = new CommandPanel( _connection ) ;
      
      _cardPanel = new Panel( _cards ) ;
      _cardPanel.add( new BorderPanel(_cellPanel )    , "cell" ) ;
      _cardPanel.add( new BorderPanel(_commandPanel) , "command" ) ;
      _cards.show( _cardPanel , "cell" ) ;

      add( _cardPanel , "Center" ) ;
      
      _updateButton.addActionListener( this ) ;
      _detailButton.addActionListener( this ) ;
      _commandButton.addActionListener( this ) ;
      _list.addItemListener( this ) ;
   }
   @Override
   public Insets getInsets(){ return new Insets( 20 , 20 ,20 , 20 ) ; }
   @Override
   public void actionPerformed( ActionEvent event ){
       Object o = event.getSource() ;
       if( o == _updateButton ){
          updateDomain() ;
         _cellPanel.clear() ;
         _commandPanel.clear() ;
       }else if( o == _detailButton ){
          _cards.show( _cardPanel , "cell" ) ;
          _cellMode = "cell" ;
       }else if( o == _commandButton ){
          _cards.show( _cardPanel , "command" ) ;
          _cellMode = "command" ;
       }
   }
   @Override
   public void frameArrived( MessageObjectFrame frame ){
       Object obj = frame.getObject() ;
       if( obj instanceof CellInfo [] ){
          _infos = (CellInfo[])obj ;
          _list.removeAll() ;
          int systemIndex = -1 ;
          TreeSet<String> sorted = new TreeSet<>() ;
           for (CellInfo cellInfo : _infos) {
               sorted.add(cellInfo.getCellName());
           }
          Iterator<String> it = sorted.iterator() ;
          for( int i = 0 ; it.hasNext() ; i++ ){
              String name = it.next();
             _list.add( name ) ;
             if( name.equals("System") ) {
                 systemIndex = i;
             }
          }
          if( systemIndex > -1 ){
             _cellPanel.showCell( _infos[systemIndex]   ,
                                  _domainNode.getAddress()) ;
             _commandPanel.showCell(_infos[systemIndex] , 
                                    _domainNode.getAddress()+":System") ;
             _list.select("System") ;
          }
       }    
   }
   public void clear(){        
      _list.removeAll() ;
      _cellPanel.clear() ;
      _commandPanel.clear() ;
   }

   public void showDomain( CellDomainNode domainNode ){
      _topLabel.setText( ">>> "+domainNode.getName()+" <<<" ) ;
      _domainNode = domainNode ;
      _cellPanel.clear() ;
      _commandPanel.clear() ;
      updateDomain() ;
   }
   private void updateDomain(){
      if( _domainNode == null ) {
          return;
      }
      _connection.send( _domainNode.getAddress() , "getcellinfos" , this ) ;
   
   }
   @Override
   public void itemStateChanged( ItemEvent event ){
      ItemSelectable sel = event.getItemSelectable() ;
      Object [] obj = sel.getSelectedObjects() ;
      if( ( obj == null ) || ( obj.length == 0 ) ) {
          return;
      }
     
      String cellName = obj[0].toString() ;
      int i = 0 ;
      for( ; ( i < _infos.length ) && 
             ( ! _infos[i].getCellName().equals( cellName ) ) ; i++ ) {
      }
      if( i ==  _infos.length ){
         System.out.println( "No more in list : "+cellName ) ;
         return ;
      }
      _cellPanel.showCell( _infos[i] , _domainNode.getAddress()) ;
      _commandPanel.showCell(_infos[i] , _domainNode.getAddress()+":"+cellName) ;
      _cards.show( _cardPanel , _cellMode  ) ;
   }

 
}
