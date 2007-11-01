package dmg.apps.libraryServer ;
import java.awt.* ;
import java.awt.event.* ;


public class InfoPanel 
       extends MulticastPanel 
       implements ActionListener {
                  
   private Button _okButton  = new Button("Got It");
   private Label  _infoLabel = new Label("",Label.CENTER);
   private Label  _titleLabel = new Label("P A N I C",Label.CENTER);
   private Font _headerFont = new Font( "SansSerif" , 0 , 24 ) ;
   private Font _biggerFont = new Font( "SansSerif" , 0 , 32 ) ;
   private InfoEvent _event  = null ;
   public InfoPanel( boolean readWrite ){
      setBackground(new Color(100,200,100) ) ;

      BorderLayout bl = new BorderLayout() ;
      bl.setVgap(10);
      bl.setHgap(10);
      setLayout(bl);
      
      _okButton.addActionListener(this) ;
      _okButton.setFont(_headerFont) ;
      
      _titleLabel.setFont(_headerFont);
      _infoLabel.setFont(_biggerFont);
      _infoLabel.setForeground(Color.red);
      add( _titleLabel , "North" ) ;
      add( _infoLabel  , "Center" ) ;
      
      Panel p = new Panel( new FlowLayout(FlowLayout.CENTER) ) ;
      
      if( ! readWrite )_okButton.setEnabled(false);
      p.add( _okButton ) ;

      add( p , "South" ) ;          
   }
   public void setEvent( InfoEvent event  ){
      _event = event ;
      setText( event.getInfo() ) ;
   }
   public void setText( String text ){
      _infoLabel.setText(text);
   }
   public void actionPerformed( ActionEvent event ){
  
      processActionEvent( new HsmEvent(_event.getSource(),401)  ) ;
   }
}
