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



class CellPanel 
      extends Panel 
      implements ActionListener, FrameArrivable {
      
   private DomainConnection _connection ;
   private Button _updateButton ;
   private Label  _topLabel  ;
   private Font   _bigFont   = new Font( "SansSerif" , Font.ITALIC , 18 )  ;
   private Font   _smallFont = new Font( "SansSerif" , Font.ITALIC|Font.BOLD , 14 )  ;
   private Font   _textFont  = new Font( "Monospaced" , Font.BOLD   , 14 )  ;
   //
   // SansSerif, Serif
   //
   private Label  _classLabel ;
   private Label  _shortInfoLabel ;
   private TextArea _privateInfo ;
   private CellInfo _cellInfo;
   private String   _cellAddress;
   private boolean  _useColor;
   
   CellPanel( DomainConnection connection ){
      _useColor   = System.getProperty( "bw" ) == null ;
      _connection = connection ; 
      if( _useColor ) {
          setBackground(Color.orange);
      }
      setLayout( new BorderLayout() ) ;

      _topLabel = new Label( "<Cell>" , Label.CENTER )  ;
      _topLabel.setFont( _bigFont ) ;
      _updateButton = new Button( "Update This Cell" ) ;
      _updateButton.addActionListener( this ) ;
      
      _classLabel = new Label("") ;
      _classLabel.setFont( _smallFont ) ;
      _shortInfoLabel = new Label( "" ) ;
      _shortInfoLabel.setFont( _smallFont ) ;
      _privateInfo = new TextArea() ;
      _privateInfo.setFont( _textFont ) ;
      
      Panel labelPanel = new Panel( new GridLayout( 0 , 1 ) ) ;
      labelPanel.add( _topLabel ) ;
      labelPanel.add( _classLabel ) ;
      labelPanel.add( _shortInfoLabel ) ;
      Panel buttonPanel = new Panel( new FlowLayout() ) ;
      buttonPanel.add( _updateButton ) ;
      
      add( labelPanel   , "North" ) ;
      add( _privateInfo , "Center" ) ;
      add( buttonPanel  , "South" ) ;
      
   }
   public void clear(){
       _classLabel.setText("") ;
       _shortInfoLabel.setText("") ;
       _privateInfo.setText("") ;
       _topLabel.setText("<className>");
   }
   @Override
   public Insets getInsets(){ return new Insets( 10 ,10 ,10 ,10  ) ; }
   @Override
   public void actionPerformed( ActionEvent event ){
       Object source = event.getSource() ;
       if( source == _updateButton ) {
           updateCell();
       }
   }
   @Override
   public void frameArrived( MessageObjectFrame frame ){
       Object obj = frame.getObject() ;
//       System.out.println( "Class arrived : "+obj.getClass().getName() ) ;
       if( obj instanceof CellInfo ){
           CellInfo info = (CellInfo)obj ;
          _topLabel.setText( ">>> "+info.getCellName()+"<<<" ) ;
          _classLabel.setText( info.getCellClass() ) ;
          _shortInfoLabel.setText( info.getShortInfo() ) ;
          _privateInfo.setText( info.getPrivatInfo() ) ;
       }else{
          _privateInfo.setText( obj.toString() ) ;
       }
   }
   public void showCell( CellInfo info , String address ){
      _cellInfo    = info ;
      _cellAddress = address ;
      _topLabel.setText( ">>> "+info.getCellName()+"<<<" ) ;
      _classLabel.setText( info.getCellClass() ) ;
      _shortInfoLabel.setText( info.getShortInfo() ) ;
      _privateInfo.setText( info.getPrivatInfo() ) ;
   
   } 
   private void updateCell(){
      if( _cellAddress == null ) {
          return;
      }
      _connection.send( _cellAddress , 
                        "getcellinfo "+_cellInfo.getCellName() , this ) ;
   
   }
  
}
