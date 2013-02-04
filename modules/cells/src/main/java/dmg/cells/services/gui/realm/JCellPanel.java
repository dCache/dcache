// $Id: JCellPanel.java,v 1.1 2002-04-03 15:00:51 cvs Exp $
package dmg.cells.services.gui.realm ;

import javax.swing.*;
import javax.swing.border.*;
import java.awt.*;
import java.awt.event.*;

import dmg.cells.applets.login.DomainConnection ;
import dmg.cells.applets.login.DomainConnectionListener ;
import dmg.cells.nucleus.* ;

public class JCellPanel 
       extends JPanel 
       implements ActionListener , DomainConnectionListener {
    private static final long serialVersionUID = -3612893682757173093L;
    private DomainConnection _connection;
    
    private String _address;
    private String _name;
    private CellInfo _cellInfo;
    
    private JLabel     _label    = new JLabel( "Command" ) ;
    private JButton    _button   = new JButton("Info") ;
    private JTextField _text     = new JHistoryTextField() ;
    private JTextArea  _display  = new JTextArea() ;
    private JLabel     _cellName   = new JLabel("<unknown>",SwingConstants.LEFT);
    private JLabel     _domainName = new JLabel("<unknown>",SwingConstants.LEFT);
    
    public JCellPanel( DomainConnection connection ){
       _connection = connection ;
       
       BorderLayout l = new BorderLayout() ;
       l.setVgap(10) ;
       l.setHgap(10);
       setLayout(l) ;
       
       l = new BorderLayout() ;
       l.setVgap(10) ;
       l.setHgap(10);

       JPanel controller = new JPanel(l) ;

       l = new BorderLayout() ;
       l.setVgap(10) ;
       l.setHgap(10);
              
       JPanel tmp = new JPanel( l ) ;
       
       tmp.add( _button , "West" ) ;
       tmp.add( _label  , "Center" ) ;
       controller.add( tmp , "West" ) ;
       controller.add( _text , "Center" ) ;

       _text.addActionListener( this ) ;
              
       JPanel titlePanel = new JPanel( new GridBagLayout() ) ;
       titlePanel.setOpaque(true);
       titlePanel.setBackground(Color.green);
       GridBagConstraints gbc = new GridBagConstraints() ;
       gbc.gridx = 0 ;
       gbc.gridy = 0 ;
       gbc.gridwidth  = 1 ;
       gbc.gridheight = 1 ;
       gbc.fill = GridBagConstraints.NONE ;
          
       JLabel jl = new JLabel("Cell Name : ",SwingConstants.RIGHT) ;
       jl.setOpaque(true);
       jl.setBackground(Color.yellow);
       titlePanel.add( jl , gbc ) ;
       
       gbc.gridx = 1 ;
       gbc.gridy = 0 ;
       gbc.fill = GridBagConstraints.HORIZONTAL ; 
       _cellName.setOpaque(true);
       _cellName.setBackground(Color.blue);     
       titlePanel.add( _cellName , gbc ) ;
       
       gbc.gridx = 0 ;
       gbc.gridy = 1 ;
       gbc.fill = GridBagConstraints.NONE ;      
       jl = new JLabel("Domain Name : ",SwingConstants.RIGHT) ;
       jl.setOpaque(true);
       jl.setBackground(Color.yellow);
       titlePanel.add( jl , gbc ) ;
       
       gbc.gridx = 1 ;
       gbc.gridy = 1 ;
       gbc.fill = GridBagConstraints.HORIZONTAL ;      
       _domainName.setOpaque(true);
       _domainName.setBackground(Color.blue);     
       titlePanel.add( _domainName , gbc ) ;
       
       
       
       add( "North" , titlePanel ) ;
       add( "Center" , _display ) ;
       add( "South" , controller ) ;
       
       _button.addActionListener( this ) ;
       
       setBorder( 
         BorderFactory.createCompoundBorder( 
           BorderFactory.createEmptyBorder(10,10,10,10) ,
           BorderFactory.createCompoundBorder( 
                BorderFactory.createTitledBorder(
                   null , "Cell Controller" , TitledBorder.LEFT , TitledBorder.TOP ) ,
                  BorderFactory.createEmptyBorder(10,10,10,10)   )
                                            ) 

       ) ;
       
    }
    public void setCell( String address , CellInfo info ){
       _address  = address ;
       _cellInfo = info ;
       _name     = info.getCellName() ;
       _cellName.setText(_name);
       _display.setText( info.getPrivatInfo() ) ;
       String tmp = _address.substring(_address.lastIndexOf(':')+1) ;
       _domainName.setText( tmp.substring( 0, tmp.indexOf('@') ) ) ;      
    }
    @Override
    public void actionPerformed( ActionEvent event ){
        Object source = event.getSource() ;
        if( source == _button ) {
            updateCellInfo();
        } else if( source == _text ) {
            textArrived();
        }
    }
    private void textArrived(){
       String text = _text.getText() ;
       _text.setText("");
       try{
          _connection.sendObject(_address,text,this,0);
       }catch(Exception ee ){
          ee.printStackTrace() ;
       }
    }
    private void updateCellInfo(){
       try{
          _connection.sendObject(_address,"getcellinfo "+_name,this,0);
       }catch(Exception ee ){
          ee.printStackTrace() ;
       }
    }
    @Override
    public void domainAnswerArrived( Object obj , int subid ){
       _display.setText(obj.toString());
    }
}
