package dmg.apps.libraryServer ;
import java.awt.* ;
import java.awt.event.* ;
import dmg.cells.applets.login.RowColumnLayout ;


public class MountPanel 
       extends MulticastPanel 
       implements ActionListener ,
                  TextListener {
   private String _cartridge = "U0011" ;
   private String _drive     = "AmpexExt" ;
   private Label  _titleLabel = new Label("Drive",Label.CENTER);
   private Label  _driveLabel = new Label("Drive",Label.CENTER);
   private Label  _cartridgeLabel =  new Label( "Cartridge",Label.CENTER ) ;
   private Button _okButton  = new Button("O.K.");
   private Button _notFoundButton = new Button( "Tape not found");
   private Button _brokenButton   = new Button( "Drive Broken" );
   private Button _failedButton   = new Button( "Failed" ) ;
   private TextField _failText    = new TextField() ;
   private Font _headerFont = new Font( "SansSerif" , 0 , 24 ) ;
   private boolean  _mount     = false ;
   private boolean  _readWrite = false ;
   private HsmEvent _event     = null ;  
   public MountPanel( boolean readWrite , boolean mount ){
      _mount     = mount ;
      _readWrite = readWrite ;
      setBackground(new Color(100,200,100) ) ;
      _okButton.addActionListener(this) ;
      _failedButton.addActionListener(this);
      _notFoundButton.addActionListener(this);
      _brokenButton.addActionListener(this);
      _failText.addTextListener(this);
      
      _failedButton.setEnabled(false);
      
      
      _okButton.setFont( _headerFont ) ;
      _notFoundButton.setForeground(Color.red);
      _brokenButton.setForeground(Color.red);
      _failedButton.setForeground(Color.red);
      BorderLayout bl = new BorderLayout() ;
      bl.setVgap(10);
      bl.setHgap(10);
      setLayout(bl);
      
      _driveLabel.setFont(_headerFont);
      _driveLabel.setForeground(Color.red);
      _cartridgeLabel.setFont(_headerFont);
      _cartridgeLabel.setForeground(Color.red);
      _titleLabel.setFont(_headerFont);
      
//      add( _titleLabel , "North" ) ;
      
      Panel textPanel = new Panel( new GridLayout(0,1) ) ;
      Label tmp = new Label( 
                    _mount ? "Please Mount" : "Please Dismount" ,
                    Label.CENTER
                            ) ;
      tmp.setFont(_headerFont);
      textPanel.add( tmp ) ;
      textPanel.add( _cartridgeLabel ) ;
      tmp = new Label( _mount ? "into" : "from"  , Label.CENTER) ;
      tmp.setFont(_headerFont);
      textPanel.add( tmp ) ;
      textPanel.add( _driveLabel ) ;
      
      add( textPanel  , "Center" ) ;
           
      RowColumnLayout rcl = new RowColumnLayout(_mount?5:4,
                                                RowColumnLayout.LAST) ;
      rcl.setVgap(10);
      rcl.setHgap(10);
      Panel buttonPanel = new SimpleBorderPanel( rcl , 15 , Color.blue ) ;
      buttonPanel.add( _okButton ) ;
      if(_mount)buttonPanel.add( _notFoundButton);
      buttonPanel.add( _brokenButton);
      buttonPanel.add( _failedButton ) ;
      buttonPanel.add( _failText ) ;
      if( ! _readWrite ){
         _okButton.setEnabled(false);
         _brokenButton.setEnabled(false);
         _failedButton.setEnabled(false);
         _failText.setEnabled(false);
         _notFoundButton.setEnabled(false);
      }
      add( buttonPanel , "South" ) ;
      display() ;
   }
   private void display(){
      _driveLabel.setText(_drive) ;
      _titleLabel.setText(_drive);
      _cartridgeLabel.setText(_cartridge);
   }
   public void setEvent( HsmEvent event ){
      _event = event ;
      if( event instanceof MountEvent ){
         MountEvent e = (MountEvent)event ;
         _cartridge = e.getCartridge() ;
         _drive     = e.getDrive() ;
      }else if( event instanceof DismountEvent ){
         DismountEvent e = (DismountEvent)event ;
         _cartridge = e.getCartridge() ;
         _drive     = e.getDrive() ;
      }
      display() ;
   }
   public void setCartridge( String cartridge ){
      _cartridge = cartridge ;
      display() ;
   }
   public void setDrive( String drive ){
      _drive = drive ; 
      display() ;
   }
   public void actionPerformed( ActionEvent event ){
      if( _event == null )return ;
      Object source = event.getSource() ;
      int base = _mount ? 200 : 300 ;
      HsmEvent e = null ;
      if( source == _okButton ){
         e = new HsmEvent(_event.getSource(),base+1) ;
      }else if( source == _failedButton ){
         e = new HsmEvent(_event.getSource(),base+11,_failText.getText()) ;
      }else if( source == _brokenButton ){
         e = new HsmEvent(_event.getSource(),base+12) ;
      }else if( source == _notFoundButton ){
         e = new HsmEvent(_event.getSource(),base+13) ;
      }
      processActionEvent( e ) ;
   }
   public void textValueChanged(TextEvent e){
      _failedButton.setEnabled( ! _failText.getText().equals("") ) ;
   }                                                                       
       
}
