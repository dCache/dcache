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
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPasswordField;

import org.globus.tools.ui.util.JJPanel;

public class PasswordDialog extends JDialog implements ActionListener {

    protected MyproxyInit myproxyInit;

    private JPasswordField passwd1TF = new JPasswordField(10);

    private JPasswordField passwd2TF = new JPasswordField(10);

    private JButton okButton, cancelButton;

    private String password = null;

    private boolean cancel = false;

    public PasswordDialog(MyproxyInit parent) {
        super(parent, "Enter Password", true);
        myproxyInit = parent;
        setSize(450, 125);

        Container contentPane = getContentPane();

        JJPanel gridPanel = new JJPanel();
        gridPanel.setInsets(2, 2, 2, 2);
        gridPanel.setAnchor(GridBagConstraints.EAST);
        gridPanel.setBorder(BorderFactory.createEtchedBorder());

        gridPanel.add(new JLabel("Enter Password to Protect Proxy: "), 0, 0, 1,
                1);

        gridPanel.add(new JLabel("Enter Password Again: "), 0, 1, 1, 1);

        gridPanel.setAnchor(GridBagConstraints.WEST);

        gridPanel.gbc.weightx = 1;
        gridPanel.setFill(GridBagConstraints.HORIZONTAL);

        gridPanel.add(passwd1TF, 1, 0, 1, 1);

        gridPanel.add(passwd2TF, 1, 1, 1, 1);

        contentPane.add(gridPanel, BorderLayout.CENTER);

        JPanel buttonPanel = new JPanel(new FlowLayout(FlowLayout.CENTER));
        okButton = new JButton("OK");
        cancelButton = new JButton("Cancel");

        buttonPanel.add(okButton);
        buttonPanel.add(cancelButton);
        okButton.addActionListener(this);
        cancelButton.addActionListener(this);
        contentPane.add(buttonPanel, BorderLayout.SOUTH);
    }

    private boolean validateSettings() {

        char[] pwd1 = passwd1TF.getPassword();
        char[] pwd2 = passwd2TF.getPassword();

        if ((pwd1 != null) && (pwd2 != null)) {
            String p1 = new String(pwd1);
            String p2 = new String(pwd2);
            if (p1.equals(p2)) {
                password = p1;
                return true;
            }
        }

        JOptionPane.showMessageDialog(this, "Passwords do not match!", "Error",
                JOptionPane.ERROR_MESSAGE);
        return false;
    }

    public boolean isCanceled() {
        return cancel;
    }

    /**
     * Handles button events for saving and exiting
     * 
     * @param evt
     *            an ActionEvent
     */
    public void actionPerformed(ActionEvent evt) {
        Object source = evt.getSource();
        if (source == okButton) {
            if (validateSettings()) {
                myproxyInit.setMyproxyPassword(password);
                cancel = false;
                setVisible(false);
            }
        } else if (source == cancelButton) {
            cancel = true;
            setVisible(false);
        } else {
            System.err.println("Unidentified event in PasswordDialog");
        }
    }

}
