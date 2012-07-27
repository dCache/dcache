// $Id: LmSetup.java,v 1.3 2005-03-07 14:56:32 patrick Exp $
//
package dmg.cells.services.gui ;
//
import java.awt.*;
import java.awt.event.*;
import javax.swing.*;
import javax.swing.border.*;
import java.io.* ;



/**
 */
public class LmSetup extends JFrame {

    private JLabel      _fileLabel = new JLabel(" Current File : ") ;
    private File        _file;
    private JMenuBar    _bar       = new JMenuBar() ;
    private MovingPigs  _draw;
    private JTextField  _text;
    private Container   _pane;
    private boolean     _textActive;
    public LmSetup(String title ) {
        super( title ) ;

        _draw  = new MovingPigs() ;
        _draw.setBorder( new BevelBorder(BevelBorder.LOWERED));

        _text = new JTextField() ;
        _text.setOpaque(true) ;
        _text.setBackground( new Color( 2 , 88 , 130 ) ) ;
        _text.setForeground( new Color( 255,255,255) ) ;
        _text.setBorder( new BevelBorder(BevelBorder.LOWERED) ) ;
        _text.setFont( new Font( "Courier" , Font.ITALIC , 24 ) ) ;
        _text.addActionListener(
           new ActionListener(){
              @Override
              public void actionPerformed( ActionEvent event ){
                  String string = _text.getText() ;
                  _draw.command(string) ;
                  _text.setText("");
              }
           }
        ) ;

        WindowListener l = new WindowAdapter() {
            @Override
            public void windowClosing(WindowEvent e) { _draw.shutdown() ; }
        };
        addWindowListener(l);


        _pane = getContentPane() ;
        BorderLayout bl = new BorderLayout() ;
        bl.setVgap(10) ;
        bl.setHgap(10) ;
        _pane.setLayout( bl ) ;

        _pane.add("Center", _draw );

        _fileLabel.setBorder( new BevelBorder(BevelBorder.LOWERED) ) ;
        _pane.add("North" , _fileLabel ) ;
//        _pane.add("South" , _text ) ;

        _bar.add( new FileActionListener() ) ;

        _bar.add( _draw.getEditMenu() ) ;

        JMenu menu = new JMenu("Help") ;

        menu.add( new HelpListener() ) ;
        menu.add( new CommandMenu() ) ;

        _bar.add( menu ) ;


        setJMenuBar( _bar ) ;

    }
    private LmSetupHelp _helpMenu;
    WindowListener l = new WindowAdapter() {
            @Override
            public void windowClosing(WindowEvent e) {System.exit(0);}
    };
    private class LMFileFilter extends javax.swing.filechooser.FileFilter {
       @Override
       public String getDescription(){ return "LocationManager (*.lm)" ; }
       @Override
       public boolean accept( File file ){
         return file.isDirectory() || file.getName().endsWith(".lm") ;
       }
    }
    private class CommandMenu extends JMenuItem implements ActionListener {
        private CommandMenu(){
           super("Commander") ;
           addActionListener(this) ;
           setAccelerator(
               KeyStroke.getKeyStroke(KeyEvent.VK_C,InputEvent.CTRL_MASK,false) ) ;
        }
        @Override
        public void actionPerformed( ActionEvent event ){
          if( _textActive ){
             _pane.remove( _text ) ;
             _textActive = false ;
          }else{
             _textActive = true ;
             _pane.add("South" , _text ) ;
          }
          _pane.doLayout() ;
        }
    }
    private class HelpListener extends JMenuItem implements ActionListener {
        private HelpListener(){
           super( "Help") ;
           addActionListener(this);
        }
        @Override
        public void actionPerformed( ActionEvent event ){
           if( _helpMenu == null ){
              _helpMenu = new LmSetupHelp("LocationManager Help");
              _helpMenu.pack();
              Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
              _helpMenu.setLocation(200,200);
              _helpMenu.setSize(600,400);
              _helpMenu.addWindowListener(
                 new WindowAdapter(){
                   @Override
                   public void windowClosing(WindowEvent e) {
                      _helpMenu.setVisible(false) ;
                   }
                 }
              );
            }
            _helpMenu.setVisible(true);
        }
    }
    private class FileActionListener extends JMenu implements ActionListener {
        private JMenuItem  _new    = new JMenuItem( "New..." ) ;
        private JMenuItem  _open   = new JMenuItem( "Open ..." ) ;
        private JMenuItem  _save   = new JMenuItem( "Save" ) ;
        private JMenuItem  _saveAs = new JMenuItem( "Save As ..." ) ;
        private JMenuItem  _revert = new JMenuItem( "Revert to saved" ) ;
        private JMenuItem  _exit   = new JMenuItem( "Exit" ) ;
        private JFileChooser _chooser = new JFileChooser() ;
        private File         _directory;
        private FileActionListener(){
           super("File");

           _new.setAccelerator(
               KeyStroke.getKeyStroke(KeyEvent.VK_N,InputEvent.CTRL_MASK,false) ) ;
           _open.setAccelerator(
               KeyStroke.getKeyStroke(KeyEvent.VK_O,InputEvent.CTRL_MASK,false) ) ;
           _save.setAccelerator(
               KeyStroke.getKeyStroke(KeyEvent.VK_S,InputEvent.CTRL_MASK,false) ) ;
           _saveAs.setAccelerator(
               KeyStroke.getKeyStroke(KeyEvent.VK_W,InputEvent.CTRL_MASK,false) ) ;
           _revert.setAccelerator(
               KeyStroke.getKeyStroke(KeyEvent.VK_R,InputEvent.CTRL_MASK,false) ) ;
           add( _new ) ;
           addSeparator() ;
           add( _open ) ;
           add( _save ) ;
           add( _saveAs ) ;
           add( _revert ) ;
           addSeparator() ;
           add( _exit ) ;
           _new.addActionListener(this) ;
           _open.addActionListener(this);
           _save.addActionListener(this);
           _saveAs.addActionListener(this);
           _revert.addActionListener(this);
           _exit.addActionListener(this);
           _save.setEnabled(false);
           _revert.setEnabled(false);
           _chooser.setFileFilter( new LMFileFilter() ) ;
           _chooser.setCurrentDirectory(_directory) ;

        }
        @Override
        public void actionPerformed( ActionEvent event ){
           if( event.getSource() == _new ){
              newAction(event) ;
           }else if( event.getSource() == _open ){
              openAction(event) ;
           }else if( event.getSource() == _save ){
              saveAction(event) ;
           }else if( event.getSource() == _saveAs ){
              saveActionAs(event) ;
           }else if( event.getSource() == _revert ){
              revertAction(event) ;
           }else if( event.getSource() == _exit ){
              _draw.shutdown() ;
           }
        }
        private void setProcessing(){
           _fileLabel.setForeground( Color.red ) ;
           _fileLabel.setText( " Processing File : "+_file.getName() ) ;
        }
        private void setOk(){
           _fileLabel.setForeground( Color.blue ) ;
           _fileLabel.setText( " Current File : "+
                  (_file==null?"<none>":_file.getName()) ) ;
           _save.setEnabled(true);
           _revert.setEnabled(true);
        }
        private void setProblem(String problem){
           _fileLabel.setForeground( Color.red ) ;
           _fileLabel.setText( " "+problem+" : "+
                  _file==null?"":_file.getName() ) ;
        }
        private void newAction( ActionEvent event ){
           _draw.clear() ;
           _file = null ;
           setOk() ;
           _save.setEnabled(false);
           _revert.setEnabled(false);
        }
        private void saveActionAs( ActionEvent event ){
           if( ( _file = getFile() ) == null ) {
               return;
           }
           saveAction(event);
           setOk() ;
        }
        private void saveAction( ActionEvent event ){
           if( _file == null ) {
               return;
           }
           try{
               PrintWriter pw = new PrintWriter( new FileWriter(_file));
               try{
                  _draw.writeSetup( pw ) ;
               }finally{
                   pw.close() ;
               }
           }catch(IOException ee){

           }
        }
        private void openAction( ActionEvent event ){
           if( ( _file = getFile() ) == null ) {
               return;
           }
           setProcessing() ;
           _bar.setEnabled(false) ;
           new Thread(
              new Runnable(){
                @Override
                public void run(){
                    if( runInterpreter( _file ) != 0 ){
                        setProblem("Illegal File Format");
                    }else{
                        setOk() ;
                    }
                    _bar.setEnabled(true) ;
                }
              }
           ).start() ;

        }
        private void revertAction( ActionEvent event ){
           if( _file == null ) {
               return;
           }
           setProcessing() ;
           _bar.setEnabled(false) ;
           new Thread(
              new Runnable(){
                @Override
                public void run(){
                    if( runInterpreter( _file ) != 0 ){
                        setProblem("Illegal File Format");
                    }else{
                        setOk() ;
                    }
                    _bar.setEnabled(true) ;
                }
              }
           ).start() ;

        }
        private int runInterpreter( File file ){
           try{
               BufferedReader br = new BufferedReader(
                                         new FileReader( file ) ) ;
               _draw.clear() ;
               try{
                  String line;
                  while( ( line = br.readLine() ) != null ){
                     _draw.command( line ) ;
                     Thread.sleep(200) ;
                  }
               }finally{
                   try{ br.close() ; }catch(IOException ee){}
               }
           }catch(IOException ee){
              return -1 ;
           }catch(InterruptedException ee){
              return -1 ;
           }
           return 0 ;
        }
        private File getFile(){
           _chooser.setCurrentDirectory( _directory ) ;
           int result = _chooser.showOpenDialog(LmSetup.this) ;
           if( result == 0 ){

              File file  = _chooser.getSelectedFile() ;
              _directory = _chooser.getCurrentDirectory() ;
              return file ;
           }
           return null ;
        }

    }

    public static void main(String argv[]) {
        LmSetup f = new LmSetup("LocationManager Setup Tool");

        f.pack();
        f.setLocation(100,100);
        f.setSize(600,400);
        f.setVisible(true);
    }
}
