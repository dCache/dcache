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
import javax.swing.JTextField;

import org.globus.tools.ui.util.JJPanel;

public class MyproxyDialog extends JDialog implements ActionListener {

    protected MyproxyGUI myproxyInit;

    private GridProxyProperties gridProps;

    private JTextField portalLifetimeTF = new JTextField(4);

    private JTextField credLifetimeTF = new JTextField(4);

    private JTextField myproxyServerTF = new JTextField(10);

    private JTextField myproxyPortTF = new JTextField(6);

    private JButton okButton, saveButton, exitButton;

    public MyproxyDialog(MyproxyGUI parent) {
        myproxyInit = parent;
        gridProps = parent.getGridProxyProperties();
        setTitle("Myproxy Properties");
        setSize(300, 200);

        Container contentPane = getContentPane();

        JJPanel gridPanel = new JJPanel();
        gridPanel.setBorder(BorderFactory.createEtchedBorder());

        gridPanel.setAnchor(GridBagConstraints.EAST);

        gridPanel.add(new JLabel("Myproxy Server: "), 0, 0, 1, 1);

        gridPanel.add(new JLabel("Myproxy Port: "), 0, 1, 1, 1);

        gridPanel.add(new JLabel("Portal Lifetime: "), 0, 2, 1, 1);

        gridPanel.add(new JLabel("Credential Lifetime: "), 0, 3, 1, 1);

        gridPanel.setAnchor(GridBagConstraints.WEST);

        myproxyServerTF.setText(gridProps.getMyproxyServer());
        gridPanel.add(myproxyServerTF, 1, 0, 1, 1);

        myproxyPortTF.setText("" + gridProps.getMyproxyPort());
        gridPanel.add(myproxyPortTF, 1, 1, 1, 1);

        contentPane.add(gridPanel, BorderLayout.CENTER);

        JPanel portalLifetimePanel = new JPanel(new FlowLayout(FlowLayout.LEFT,
                0, 0));

        portalLifetimeTF.setText("" + gridProps.getPortalLifetime());
        portalLifetimePanel.add(portalLifetimeTF);
        portalLifetimePanel.add(new JLabel(" hours"));

        gridPanel.add(portalLifetimePanel, 1, 2, 1, 1);

        JPanel credLifetimePanel = new JPanel(new FlowLayout(FlowLayout.LEFT,
                0, 0));

        credLifetimePanel.add(credLifetimeTF);
        credLifetimeTF.setText("" + gridProps.getCredLifetime());
        credLifetimePanel.add(new JLabel(" hours"));

        gridPanel.add(credLifetimePanel, 1, 3, 1, 1);

        JPanel buttonPanel = new JPanel(new FlowLayout(FlowLayout.CENTER));
        okButton = new JButton("OK");
        saveButton = new JButton("Save");
        exitButton = new JButton("Cancel");

        buttonPanel.add(okButton);
        buttonPanel.add(saveButton);
        buttonPanel.add(exitButton);

        okButton.addActionListener(this);
        saveButton.addActionListener(this);
        exitButton.addActionListener(this);

        contentPane.add(buttonPanel, BorderLayout.SOUTH);
    }

    private boolean error(String msg) {
        JOptionPane.showMessageDialog(this, msg, "Need More Information",
                JOptionPane.WARNING_MESSAGE);
        return false;
    }

    private boolean validateSettings() {
        if (myproxyServerTF.getText().trim().equals("")) {
            return error("Please enter the hostname of the myproxy server");
        } else if (myproxyPortTF.getText().trim().equals("")) {
            return error("Please enter the port number of the myproxy server");
        } else if (credLifetimeTF.getText().trim().equals("")) {
            return error("Please enter the lifetime of the credential on the myproxy server");
        } else if (portalLifetimeTF.getText().trim().equals("")) {
            return error("Please enter the maximum allowed lifetime of the proxy on the portal");
        }

        gridProps.setMyproxyServer(myproxyServerTF.getText().trim());
        gridProps.setMyproxyPort(new Integer(myproxyPortTF.getText().trim())
                .intValue());
        gridProps.setCredLifetime(new Integer(credLifetimeTF.getText().trim())
                .intValue());
        gridProps.setPortalLifetime(new Integer(portalLifetimeTF.getText()
                .trim()).intValue());

        return true;
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
            if (!validateSettings())
                return;
            myproxyInit.setGridProxyProperties(gridProps);
            setVisible(false);
        } else if (source == saveButton) {
            if (!validateSettings())
                return;
            myproxyInit.setGridProxyProperties(gridProps);
            gridProps.saveProperties();
        } else if (source == exitButton) {
            setVisible(false);
        } else {
            System.err.println("Unidentified event in MyproxyDialog");
        }
    }

}
