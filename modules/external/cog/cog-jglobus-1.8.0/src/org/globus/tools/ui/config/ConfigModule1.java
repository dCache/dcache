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
package org.globus.tools.ui.config;

import java.awt.Color;
import java.awt.GridBagConstraints;
import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;
import java.security.cert.X509Certificate;
import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JOptionPane;

import org.globus.common.CoGProperties;
import org.globus.tools.ui.util.FileBrowser;
import org.globus.tools.ui.util.CustomFileFilter;

public class ConfigModule1 extends BaseModule {

    private FileBrowser usercert, userkey;
    private JButton verifyBT;

    public ConfigModule1(CoGProperties props) {
	super(props);
	
	JLabel label = new JLabel("Configuring User Credentials");
	label.setFont(getFont(font, 1));
	label.setForeground(Color.black);
	
	add(label,
	    1, 1, 1, 1);
	
	label = new JLabel(" ");
	add(label,
	    1, 2, 1, 1);
	

	CustomFileFilter filter = new CustomFileFilter(".pem", "PEM files (.pem)");

	usercert = new FileBrowser("Open User Certificate", 
				   "User Certificate: ",
				   "Select");
	usercert.setFileFilter(filter);
	usercert.setFile( props.getUserCertFile() );


	add(usercert,
	    1, 3, 1, 1);

	userkey = new FileBrowser("Open User Key", 
				  "User Private Key: ",
				  "Select");
	userkey.setFileFilter(filter);
	userkey.setFile( props.getUserKeyFile() );
	
	add(userkey,
	    1, 4, 1, 1);


	verifyBT = new JButton("Verify");
	verifyBT.addActionListener(new ActionListener() {
	    public void actionPerformed(ActionEvent e) {
		verifyKeyPair();
	    }
	});
	verifyBT.setEnabled(false);

	setAnchor(GridBagConstraints.WEST);
	/*
	add(verifyBT,
	    1, 5, 1, 1);
	    */
    }

    public void verifyKeyPair() {
	System.out.println("checks if private key goes with public key");
    }

    public void saveSettings() {
	props.setUserCertFile(usercert.getFile());
	props.setUserKeyFile(userkey.getFile());
    }
    
    public boolean verifySettings() {
	boolean rs;
	
	rs = checkFile(usercert.getFile(), "User Certificate");
	if (!rs) return false;
	
	rs = checkFile(userkey.getFile(), "User Key");
	if (!rs) return false;
	
	rs = verifyCertificate();
	if (!rs) return false;

	return true;
    }

    private boolean verifyCertificate() {
	try {
	    X509Certificate cert = 
		Configure.verifyUserCertificate(usercert.getFile());
	    
	    // save the certificate for verification
	    props.put("internal.usercert", cert);
	    
	    return true;
	} catch (Exception e) {
	    JOptionPane.showMessageDialog(this, 
					  e.getMessage(),
					  "User Certificate Error",
					  JOptionPane.ERROR_MESSAGE);
	    return false;
	}
    }
    
}

