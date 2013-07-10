------------------ MIGRATION SQL FOR ADDING P2P ATTRIBUTE -------------------
--- Run this sql after applying the liquibase changelog for 2.5; it
--- will make the data in billinginfo consistently record the 'p2p'
--- attribute as true or false for pre-existent data.  Note that this
--- operation will take time proportional to the size of the table.
-----------------------------------------------------------------------------

UPDATE billinginfo set p2p='f' where p2p is null AND initiator like '%door:%';

UPDATE billinginfo set p2p='t' where p2p is null;
