package dmg.cells.applets.login ;

import dmg.cells.applets.login.CenterLayout ;
import java.lang.reflect.* ;
import java.applet.*;
import java.awt.* ;
import java.awt.event.* ;
import java.util.* ;


public class      UserAdminPanel 
       extends    Panel 
       implements ActionListener,DomainConnectionListener   {

   
   private class     PasswordPanel 
          extends    Panel
          implements TextListener, ActionListener {
      private int _b = 5 ;
      public void paint( Graphics g ){
         Dimension   d    = getSize() ;
         g.setColor( Color.white ) ;
         g.drawRect( _b/2 , _b/2 , d.width-_b , d.height-_b ) ;
      }
      public Insets getInsets(){ return new Insets(_b,_b,_b,_b) ; }
      
      private ActionListener _actionListener = null ;
      public void addActionListener( ActionListener al ){
         _actionListener = al ;
      }
      private TextField _pwd   = new TextField();
      private TextField _vpwd  = new TextField();
      private Button _okButton = new Button( "Change Password" ) ;
      public PasswordPanel(){
          BorderLayout bl = new BorderLayout() ;
          bl.setHgap(5) ;
          bl.setVgap(5) ;
          setLayout( bl ) ;
          GridLayout gl = new GridLayout(2,2) ;
          gl.setHgap(5) ;
          gl.setVgap(5) ;
          Panel top = new Panel( gl ) ;
          top.add( new Label( "New Password" ) ) ;
          top.add( _pwd ) ;
          
          top.add( new Label( "Verify Password" ) ) ;
          top.add( _vpwd ) ; 
          add( top , "North" ) ;
          add( _okButton , "South" ) ;
          _pwd.addTextListener(this);
          _vpwd.addTextListener(this);
          _pwd.setEchoChar('*');
          _vpwd.setEchoChar('*') ;
          _okButton.setEnabled(false);
          _okButton.addActionListener(this);
      }
      public String getText(){ return _pwd.getText() ; }
      public void textValueChanged( TextEvent event ){
          _okButton.setEnabled(
             ( _pwd.getText().length() > 0 ) &&
             ( _vpwd.getText().equals(_pwd.getText()) )
          ) ;
      }
      public void actionPerformed( ActionEvent event ){
         if( _actionListener != null ) {
             _actionListener.actionPerformed(
                     new ActionEvent(this, 0, _pwdPanel.getText()));
         }
              
         _pwd.setText("") ; 
         _vpwd.setText("") ;
      }
   }
   private class UserPanel extends Panel {
      private int _b = 5 ;
      public void paint( Graphics g ){
         Dimension   d    = getSize() ;
         g.setColor( Color.white ) ;
         g.drawRect( _b/2 , _b/2 , d.width-_b , d.height-_b ) ;
      }
      public Insets getInsets(){ return new Insets(_b,_b,_b,_b) ; }
      public UserPanel(){
          BorderLayout bl = new BorderLayout() ;
          bl.setHgap(5) ;
          bl.setVgap(5) ;
          setLayout( bl ) ;
          GridLayout gl = new GridLayout(1,0) ;
          gl.setHgap(5) ;
          gl.setVgap(5) ;
          GridLayout gl2 = new GridLayout(1,0) ;
          gl2.setHgap(5) ;
          gl2.setVgap(5) ;
          Panel top    = new Panel( gl ) ;
          Panel bottom = new Panel( gl2 ) ;
//          top.setFont( _bigFont ) ;
          top.add( new Label( "User Name" ) ) ;
          top.add( _user  ) ;
          bottom.add( _addUser ) ;
          bottom.add( _rmUser ) ;
          add( top , "North" ) ;
          add( bottom , "South" ) ;
      }
   }
   private TextField _email = new TextField() ;
   private Button    _emailButton 
                         = new Button("Change E-mail address") ;
   private class      EmailPanel 
           extends    Panel
           implements TextListener {
           
      private int _b = 5 ;
      public void paint( Graphics g ){
         Dimension   d    = getSize() ;
         g.setColor( Color.white ) ;
         g.drawRect( _b/2 , _b/2 , d.width-_b , d.height-_b ) ;
      }
      public Insets getInsets(){ return new Insets(_b,_b,_b,_b) ; }
      public EmailPanel(){
          BorderLayout bl = new BorderLayout() ;
          bl.setHgap(5) ;
          bl.setVgap(5) ;
          setLayout( bl ) ;
          GridLayout gl = new GridLayout(0,1) ;
          gl.setHgap(5) ;
          gl.setVgap(5) ;
          Panel top = new Panel( gl ) ;
//          top.setFont( _bigFont ) ;
          top.add( new Label( "E-Mail Address" ) ) ;
          top.add( _email  ) ;
          top.add( _emailButton ) ;
          add( top , "North" ) ;
          _email.addTextListener(this);
      }
      public void textValueChanged( TextEvent event ){
          _emailButton.setEnabled(
             ( _email.getText().length() > 0 ) &&
             ( _user.getText().length() > 0  )
          ) ;
      }
   }
   private Font   _bigFont = 
             new Font( "SansSerif" , Font.BOLD , 18 )  ; 

   private Label     _errorLabel = new Label() ;
   private TextField _user       = new TextField();
   private Button    _addUser    = new Button( "Add User" ) ;
   private Button    _rmUser     = new Button( "Remove User" ) ;
   private PasswordPanel _pwdPanel   = new PasswordPanel() ;
   private DomainConnection _domain ;
   public UserAdminPanel( DomainConnection dc ){
       _domain = dc ;
       BorderLayout bl = new BorderLayout() ;
       bl.setHgap(5) ;
       bl.setVgap(5) ;
       setLayout( bl ) ;
       
       _user.addActionListener( this ) ;
       _addUser.addActionListener(this);
       _rmUser.addActionListener(this);
       _pwdPanel.addActionListener(this);
       _emailButton.addActionListener(this);
       Panel up = new UserPanel() ;
       Panel ep = new EmailPanel() ;
       
       Label topLabel = new Label("User Administration",Label.CENTER ) ;
       topLabel.setFont( _bigFont ) ;
       
       add( topLabel , "North" ) ;
       
       Panel l1Panel = new Panel(new BorderLayout()) ;
       add( l1Panel , "Center" ) ;
       add( _errorLabel , "South" ) ;
       _errorLabel.setBackground( Color.yellow ) ;
       
       l1Panel.add( up , "North" ) ;
       
       Panel l2Panel = new Panel(new BorderLayout()) ;
       l1Panel.add( l2Panel , "South" ) ;
       
       l2Panel.add( _pwdPanel , "North" ) ;
       
       Panel l3Panel = new Panel(new BorderLayout()) ;
       l2Panel.add( l3Panel , "South" ) ;
       
       l3Panel.add( ep , "North" ) ;
       
   }
     public void actionPerformed( ActionEvent event ){
        Object source = event.getSource() ;
        Object [] r = null ;
        if( _user.getText().length() == 0 ){
           _errorLabel.setText( "User ????" ) ;
           return ;
        }
        if( source == _rmUser ){
           _errorLabel.setText("");
           return ;
        }else if( source == _pwdPanel ){
           r = new Object[5] ;
           r[0] = "request" ;
           r[1] = "*" ;
           r[2] = "set-password" ;
           r[3] = _user.getText() ;
           r[4] = _pwdPanel.getText() ;
        }else if( source == _addUser ){
           r = new Object[4] ;
           r[0] = "request" ;
           r[1] = "*" ;
           r[2] = "create-user" ;
           r[3] = _user.getText() ;
        }else if( source == _user ){
           r = new Object[4] ;
           r[0] = "request" ;
           r[1] = "*" ;
           r[2] = "get-user-attr" ;
           r[3] = _user.getText() ;
        }else if( source == _emailButton ){
           Object [] a = new Object[1] ;
           String [] p = new String[2] ;
           a[0] = p ;
           p[0] = "e-mail" ;
           p[1] = _email.getText() ;
           
           r = new Object[5] ;
           r[0] = "request" ;
           r[1] = "*" ;
           r[2] = "set-user-attr" ;
           r[3] = _user.getText() ;
           r[4] = a ;
        }else{
           return ;
        }
        try{
          _domain.sendObject( r , this , 0 ) ;
        }catch(Exception e){
          _errorLabel.setText( "E="+e.getMessage() ) ;
        }
     }
    public void domainAnswerArrived( Object obj , int id ){
       if( obj instanceof Object [] ){
          _errorLabel.setText("O.K") ;
          Object [] array = (Object[])obj ;
          if( array.length < 5 ) {
              return;
          }
          if( array[2].toString().endsWith( "et-user-attr" ) ){
             if( ( array.length < 5 ) ||
                 ( ! ( array[4] instanceof Object [] ) ) ) {
                 return;
             }
             array = (Object[])array[4] ;
             String [] p ;
             for( int i = 0 ; i < array.length ; i++ ){
                if( ( array[i] instanceof String [] ) &&
                    (  (String[])array[i])[0].equals("e-mail") ){
                 
                    _email.setText(  ((String[])array[i])[1] ) ;
                    break ;
                }
             }
          }
       }else if( obj instanceof Exception ){
          _errorLabel.setText( ((Exception)obj).getMessage() ) ;
       }else{
          _errorLabel.setText( obj.toString() ) ;
       }
    }
    
}
 
