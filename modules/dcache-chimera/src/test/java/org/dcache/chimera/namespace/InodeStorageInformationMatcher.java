/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dcache.chimera.namespace;

import org.dcache.chimera.FsInode;
import org.dcache.chimera.store.InodeStorageInformation;
import org.mockito.ArgumentMatcher;

/**
 * An Mockito ArgumentMatcher for InodeStorageInformation with a fluent interface.
 */
public class InodeStorageInformationMatcher implements ArgumentMatcher<InodeStorageInformation> {

    private FsInode inode;
    private String hsm;
    private String group;
    private String subgroup;

    public static InodeStorageInformationMatcher matchesAnInodeStorageInformation() {
        return new InodeStorageInformationMatcher();
    }

    public InodeStorageInformationMatcher withInode(FsInode inode) {
        this.inode = inode;
        return this;
    }

    public InodeStorageInformationMatcher withHsm(String hsm) {
        this.hsm = hsm;
        return this;
    }

    public InodeStorageInformationMatcher withStorageGroup(String group) {
        this.group = group;
        return this;
    }

    public InodeStorageInformationMatcher withStorageSubgroup(String subgroup) {
        this.subgroup = subgroup;
        return this;
    }

    @Override
    public boolean matches(InodeStorageInformation argument) {
        return (inode == null || inode.equals(argument.inode()))
              &&
              (hsm == null || hsm.equals(argument.hsmName()))
              &&
              (group == null || group.equals(argument.storageGroup()))
              &&
              (subgroup == null || subgroup.equals(argument.storageSubGroup()));
    }
}
