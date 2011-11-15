/*
 * This library is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library General Public License as
 * published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this program (see the file COPYING.LIB for more
 * details); if not, write to the Free Software Foundation, Inc.,
 * 675 Mass Ave, Cambridge, MA 02139, USA.
 */

package org.dcache.chimera.nfs.v4;

public interface OperationFactoryMXBean {

    long getAccess();

    long getClose();

    long getCommit();

    long getCreate();

    long getDelegpurge();

    long getDelegreturn();

    long getGetattr();

    long getGetfh();

    long getLink();

    long getLock();

    long getLockt();

    long getLocku();

    long getLookup();

    long getLookupp();

    long getNverify();

    long getOpen();

    long getOpenattr();

    long getOpenConfirm();

    long getOpenDowngrade();

    long getPutfh();

    long getPutpubfh();

    long getPutrootfh();

    long getRead();

    long getReaddir();

    long getReadlink();

    long getRemove();

    long getRename();

    long getRenew();

    long getRestorefh();

    long getSavefh();

    long getSecinfo();

    long getSetattr();

    long getSetclientid();

    long getSetclientidConfirm();

    long getVerify();

    long getWrite();

    long getReleaseLockowner();

    long getBackchannelCtl();

    long getBindConnToSession();

    long getExchangeId();

    long getCreateSession();

    long getDestroySession();

    long getFreeStateid();

    long getGetDirDelegation();

    long getGetdeviceinfo();

    long getGetdevicelist();

    long getLayoutcommit();

    long getLayoutget();

    long getLayoutreturn();

    long getSecinfoNoName();

    long getSequence();

    long getSetSsv();

    long getTestStateid();

    long getWantDelegation();

    long getDestroyClientid();

    long getReclaimComplete();

    long getIllegal();

}
