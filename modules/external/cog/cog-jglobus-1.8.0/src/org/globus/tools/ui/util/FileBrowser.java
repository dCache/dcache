/*
 * Copyright 1999-2006 University of Chicago
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.globus.tools.ui.util;

import java.io.File;
import java.awt.Insets;
import java.awt.BorderLayout;
import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;
import javax.swing.filechooser.FileFilter;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JFileChooser;

public class FileBrowser extends JPanel implements ActionListener {

    private JTextField file;
    private JButton browser;
    private FileFilter fileFilter;

    boolean specialfilter = false;
    private String title, okLabel;
    

    public FileBrowser(String title, String label, String okLabel) {
	
	this.title = title;
	this.okLabel = okLabel;
	
	file = new JTextField(30);
	browser = new JButton("...");
	browser.addActionListener(this);
	
	setLayout(new BorderLayout(5, 1));
	add("North", new JLabel(label));
	add("Center", file);
	add("East", browser);
    }

  public FileBrowser(String title, String okLabel) {
    this.title = title;
    this.okLabel = okLabel;
    
    file = new JTextField(30);
    browser = new JButton("...");
    browser.addActionListener(this);
    
    setLayout(new BorderLayout(5, 1));
    add("Center", file);
    add("East", browser);
  }
  
    public Insets getInsets() {
	return new Insets(5, 2, 5, 2);
    }

    public void enableSpecialFilter() {
	specialfilter = true;
    }

  private File getSelectedFile() {
    JFileChooser filechooser = new JFileChooser();

    if (fileFilter != null) {
	filechooser.addChoosableFileFilter( fileFilter );
	filechooser.setFileFilter( fileFilter );
    }

    File ff  = null;
    String tt = file.getText();
    if (tt.trim().length() == 0) {
	ff = new File(".");
    } else {
	ff = new File( file.getText() );
    }

    filechooser.setCurrentDirectory( ff );
    filechooser.setSelectedFile( ff );

    filechooser.setApproveButtonText(okLabel);
    filechooser.setDialogTitle(title);
   
    if(filechooser.showOpenDialog(this) == JFileChooser.APPROVE_OPTION) {
      File f = filechooser.getSelectedFile();
      return f;
    } 
    return null;
  }

  public void actionPerformed(ActionEvent e) {
    File f = getSelectedFile();
    if (f != null)
      file.setText( f.getAbsolutePath() );
  }

  public String getFile() {
    return file.getText().trim();
  }

    public void setFileFilter(FileFilter filter) {
	fileFilter = filter;
    }

  public void setFile(String f) {
    file.setText(f);
  }

}
