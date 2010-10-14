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
package org.globus.tools.ui.proxy;

import java.awt.BorderLayout;
import java.awt.Container;
import java.awt.GridBagConstraints;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowEvent;
import java.awt.event.WindowListener;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JTextField;

import org.globus.gsi.CertUtil;
import org.globus.gsi.GSIConstants;
import org.globus.gsi.GlobusCredential;
import org.globus.gsi.OpenSSLKey;
import org.globus.gsi.X509ExtensionSet;
import org.globus.gsi.bc.BouncyCastleCertProcessingFactory;
import org.globus.gsi.bc.BouncyCastleOpenSSLKey;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.globus.myproxy.MyProxy;
import org.globus.tools.ui.util.JJPanel;
import org.globus.tools.ui.util.UITools;
import org.globus.util.Util;
import org.ietf.jgss.GSSCredential;

public class MyproxyInit extends MyproxyGUI implements ActionListener,
        WindowListener {

    public static final int FRAME_WIDTH = 430;

    public static final int FRAME_HEIGHT = 180;

    private JTextField usernameTF = new JTextField(15);

    private JPasswordField passwordTF = new JPasswordField(15);

    private JButton createButton, sendButton, killLocButton, killRemButton;

    private boolean runAsApplication = true;

    private GlobusCredential gridProxy = null;

    private String myproxyPassword = "";

    /**
     * Create the interface
     */
    public MyproxyInit() {

        // set up GUI
        setTitle("Myproxy Init");

        setSize(FRAME_WIDTH, FRAME_HEIGHT);

        addWindowListener(this);

        Container contentPane = this.getContentPane();

        JMenuBar menuBar = new JMenuBar();
        setJMenuBar(menuBar);

        JMenu fileMenu = new JMenu("File");
        menuBar.add(fileMenu);

        JMenuItem exitMenuItem = new JMenuItem("Exit");
        exitMenuItem.addActionListener(this);
        fileMenu.add(exitMenuItem);

        JMenu editMenu = new JMenu("Edit");
        menuBar.add(editMenu);

        JMenuItem cpMenuItem = new JMenuItem("Certificate Properties");
        cpMenuItem.addActionListener(this);
        editMenu.add(cpMenuItem);

        JMenuItem mpMenuItem = new JMenuItem("Myproxy Properties");
        mpMenuItem.addActionListener(this);
        editMenu.add(mpMenuItem);

        JPanel buttonPanel = new JPanel();
        // buttonPanel.setLayout(new BorderLayout());

        JPanel proxyPanel = new JPanel();
        proxyPanel.setBorder(BorderFactory.createTitledBorder(" Local Proxy "));

        createButton = new JButton("Create");
        killLocButton = new JButton("Destroy");

        createButton.addActionListener(this);
        killLocButton.addActionListener(this);

        proxyPanel.add(createButton);
        proxyPanel.add(killLocButton);

        JPanel myproxyPanel = new JPanel();
        myproxyPanel.setBorder(BorderFactory.createTitledBorder(" Myproxy "));

        sendButton = new JButton("Init");
        killRemButton = new JButton("Destroy");

        sendButton.addActionListener(this);
        killRemButton.addActionListener(this);

        myproxyPanel.add(sendButton);
        myproxyPanel.add(killRemButton);

        buttonPanel.add(proxyPanel); // , BorderLayout.WEST);
        buttonPanel.add(myproxyPanel); // , BorderLayout.EAST);

        JJPanel mainPanel = new JJPanel();
        // mainPanel.setBorder(BorderFactory.createEtchedBorder());
        mainPanel.setInsets(5, 5, 5, 5);
        mainPanel.setAnchor(GridBagConstraints.EAST);

        mainPanel.add(new JLabel("Username: "), 0, 0, 1, 1);

        mainPanel.add(new JLabel("PEM Passphrase: "), 0, 1, 1, 1);

        mainPanel.setAnchor(GridBagConstraints.WEST);
        mainPanel.gbc.weightx = 1;
        mainPanel.setFill(GridBagConstraints.HORIZONTAL);

        mainPanel.add(usernameTF, 1, 0, 1, 1);

        usernameTF.addActionListener(this);

        mainPanel.add(passwordTF, 1, 1, 1, 1);

        passwordTF.addActionListener(this);

        contentPane.add(mainPanel, BorderLayout.CENTER);
        contentPane.add(buttonPanel, BorderLayout.SOUTH);
    }

    /**
     * Event handlers for button panel
     */
    public void actionPerformed(ActionEvent evt) {
        Object source = evt.getSource();
        String cmd = evt.getActionCommand();

        /** make okButton the default input button * */
        // if( source == passwordTF ) {
        // source = okButton;
        // }
        /* Check menu */
        if (cmd.equals("Certificate Properties")) {
            GridProxyDialog gpd = new GridProxyDialog(this.gridProps);
            UITools.center(this, gpd);
            gpd.show();
            return;
        } else if (cmd.equals("Myproxy Properties")) {
            MyproxyDialog mpd = new MyproxyDialog(this);
            UITools.center(this, mpd);
            mpd.show();
            return;
        } else if (cmd.equals("Exit")) {
            this.exit(0);
            return;
        }

        /* Check Buttons */
        if (source == createButton) {

            if (validatePassword()) {
                if (createProxy(gridProps.getHours())) {
                    JOptionPane.showMessageDialog(this,
                            "Succesfully created a proxy in "
                                    + gridProps.getProxyFile(), "Success",
                            JOptionPane.PLAIN_MESSAGE);
                }
            }

        } else if (source == sendButton) {

            if (validateUsername() && validatePassword()
                    && validateMyproxyPassword()) {
                if (createProxy(gridProps.getCredLifetime()))
                    sendProxy();
            }

        } else if (source == killLocButton) {
            deleteProxy();
        } else if (source == killRemButton) {

            if (validateUsername() && validateMyproxyPassword()) {
                deleteMyproxy();
            }

        } else {
            System.err.println("Unidentified event in MyproxyInit" + cmd
                    + source);
        }
    }

    private boolean validateUsername() {
        if (usernameTF.getText().trim().equals("")) {
            JOptionPane.showMessageDialog(this, "Please enter your username.",
                    "Need More Information", JOptionPane.WARNING_MESSAGE);
            return false;
        }
        return true;
    }

    private boolean validatePassword() {
        if (passwordTF.getPassword().length == 0) {
            JOptionPane.showMessageDialog(this, "Please enter your password.",
                    "Need More Information", JOptionPane.WARNING_MESSAGE);
            return false;
        }
        return true;
    }

    private boolean validateMyproxyPassword() {
        if (myproxyPassword.equals("")) {
            PasswordDialog passDialog = new PasswordDialog(this);
            UITools.center(this, passDialog);
            passDialog.show();
            return !passDialog.isCanceled();
        }
        return true;
    }

    public void deleteProxy() {
        gridProxy = null;
        if (!gridProps.getProxyFile().equals("")) {
            File proxyFile = new File(gridProps.getProxyFile());
            if (proxyFile.exists()) {
                if (!Util.destroy(proxyFile)) {
                    JOptionPane.showMessageDialog(this,
                            "Unable to destroy local proxy", "Error",
                            JOptionPane.ERROR_MESSAGE);
                } else {
                    JOptionPane.showMessageDialog(this,
                            "Succesfully destroyed proxy in "
                                    + gridProps.getProxyFile(), "Success",
                            JOptionPane.PLAIN_MESSAGE);
                }
            } else {
                JOptionPane.showMessageDialog(this, "No proxy exists in "
                        + gridProps.getProxyFile(), "Information",
                        JOptionPane.INFORMATION_MESSAGE);
            }
        }
    }

    public void deleteMyproxy() {
        if (gridProxy == null)
            return;

        try {
            MyProxy.destroy(gridProps.getMyproxyServer(), gridProps
                    .getMyproxyPort(), new GlobusGSSCredentialImpl(gridProxy,
                    GSSCredential.INITIATE_AND_ACCEPT), usernameTF.getText()
                    .trim(), myproxyPassword);
            JOptionPane.showMessageDialog(this,
                    "Succesfully destroyed proxy from myproxy server",
                    "Success", JOptionPane.PLAIN_MESSAGE);
            return;
        } catch (Exception e) {
            JOptionPane.showMessageDialog(this,
                    "Failed to delete proxy from myproxy server!\n\""
                            + e.getMessage() + "\"", "Error",
                    JOptionPane.ERROR_MESSAGE);
        }
    }

    public boolean createProxy(int hours) {      
        X509Certificate userCert = null;
        PrivateKey userKey = null;
        char[] pwd;
        FileOutputStream out = null;
        try {
            userCert = CertUtil.loadCertificate(gridProps.getUserCertFile());

            if (userCert == null) {
                JOptionPane.showMessageDialog(this, "Failed to load cert: "
                        + gridProps.getUserCertFile(), "Error",
                        JOptionPane.ERROR_MESSAGE);
                return false;
            }

            pwd = passwordTF.getPassword();

            OpenSSLKey key = new BouncyCastleOpenSSLKey(gridProps
                    .getUserKeyFile());

            if (key.isEncrypted()) {
                key.decrypt(new String(pwd));
            }

            userKey = key.getPrivateKey();

            pwd = null;

            if (userKey == null) {
                JOptionPane.showMessageDialog(this,
                        "Failed to load private key: "
                                + gridProps.getUserKeyFile(), "Error",
                        JOptionPane.ERROR_MESSAGE);
                return false;
            }

            BouncyCastleCertProcessingFactory factory = 
                BouncyCastleCertProcessingFactory
                .getDefault();

            int proxyType = (gridProps.getLimited()) ? 
                GSIConstants.GSI_4_LIMITED_PROXY
                : GSIConstants.GSI_4_IMPERSONATION_PROXY;

            X509ExtensionSet extSet = null;

            gridProxy = factory.createCredential(
                    new X509Certificate[] { userCert }, userKey, gridProps
                            .getBits(), hours*3600, proxyType, extSet);

            if (gridProxy == null) {
                JOptionPane.showMessageDialog(this, "Failed to create proxy!",
                        "Error", JOptionPane.ERROR_MESSAGE);
                return false;
            }
            
            File file = Util.createFile(gridProps.getProxyFile());
            // set read only permissions
            if (!Util.setOwnerAccessOnly(gridProps.getProxyFile())) {

            }
            out = new FileOutputStream(file);
            gridProxy.save(out);

        } catch (Exception e) {
            e.printStackTrace();
            JOptionPane
                    .showMessageDialog(this, "Failed to create proxy!\n\""
                            + e.getMessage() + "\"", "Error",
                            JOptionPane.ERROR_MESSAGE);
            return false;
        } finally {
            if (out != null) {
                try {
                    out.close();
                } catch (IOException e) {
                }
            }
        }
        return true;
    }

    public void sendProxy() {

        if ((gridProxy == null) || (myproxyPassword.equals("")))
            return;

        if (myproxyPassword.equals("")) {
            PasswordDialog passDialog = new PasswordDialog(this);
            UITools.center(this, passDialog);
            passDialog.show();
        }

        try {
            MyProxy.put(gridProps.getMyproxyServer(), gridProps
                    .getMyproxyPort(), new GlobusGSSCredentialImpl(gridProxy,
                    GSSCredential.INITIATE_AND_ACCEPT), usernameTF.getText()
                    .trim(), myproxyPassword, gridProps.getCredLifetime());
            JOptionPane.showMessageDialog(this,
                    "Succesfully delegated a proxy to myproxy server",
                    "Success", JOptionPane.PLAIN_MESSAGE);
        } catch (Exception e) {
            JOptionPane.showMessageDialog(this,
                    "Failed to delegate proxy to myproxy server!\n\""
                            + e.getMessage() + "\"", "Error",
                    JOptionPane.ERROR_MESSAGE);
        }
        return;
    }

    public void setMyproxyPassword(String password) {
        this.myproxyPassword = password;
    }

    public void setRunAsApplication(boolean runAsApplication) {
        this.runAsApplication = runAsApplication;
    }

    public boolean isRunAsApplication() {
        return this.runAsApplication;
    }

    private void exit(int exitValue) {
        if (this.isRunAsApplication()) {
            System.exit(exitValue);
        } else {
            this.setVisible(false);
        }
    }

    /** * WindowListener Methods ** */
    public void windowActivated(WindowEvent we) {
    }

    public void windowClosed(WindowEvent we) {
    }

    public void windowDeactivated(WindowEvent we) {
    }

    public void windowDeiconified(WindowEvent we) {
    }

    public void windowIconified(WindowEvent we) {
    }

    public void windowOpened(WindowEvent we) {
    }

    public void windowClosing(WindowEvent we) {
        this.exit(0);
    }

    /** *************************** */

    /**
     * Start the program
     */
    public static void main(String args[]) {
        for (int i=0;i<args.length;i++) {
            if (args[i].equals("-help")) {
                System.out.println("Syntax: java MyproxyInit");
                System.exit(0);
            }
        }
        MyproxyInit mpiFrame = new MyproxyInit();
        UITools.center(null, mpiFrame);
        mpiFrame.setVisible(true);
    }

}
