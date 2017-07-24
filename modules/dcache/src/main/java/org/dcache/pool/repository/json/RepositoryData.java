/*
COPYRIGHT STATUS:
Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
software are sponsored by the U.S. Department of Energy under Contract No.
DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
non-exclusive, royalty-free license to publish or reproduce these documents
and software for U.S. Government purposes.  All documents and software
available from this server are protected under the U.S. and Foreign
Copyright Laws, and FNAL reserves all rights.

Distribution of the software available from this server is free of
charge subject to the user following the terms of the Fermitools
Software Legal Information.

Redistribution and/or modification of the software shall be accompanied
by the Fermitools Software Legal Information  (including the copyright
notice).

The user is asked to feed back problems, benefits, and/or suggestions
about the software to the Fermilab Software Providers.

Neither the name of Fermilab, the  URA, nor the names of the contributors
may be used to endorse or promote products derived from this software
without specific prior written permission.

DISCLAIMER OF LIABILITY (BSD):

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.

Liabilities of the Government:

This software is provided by URA, independent from its Prime Contract
with the U.S. Department of Energy. URA is acting independently from
the Government and in its own private capacity and is not acting on
behalf of the U.S. Government, nor as its contractor nor its agent.
Correspondingly, it is understood and agreed that the U.S. Government
has no connection to this software and in no manner whatsoever shall
be liable for nor assume any responsibility or obligation for any claim,
cost, or damages arising out of or resulting from the use of the software
available from this server.

Export Control:

All documents and software available from this server are subject to U.S.
export control laws.  Anyone downloading information from this server is
obligated to secure any necessary Government licenses before exporting
documents or software obtained from this server.
 */
package org.dcache.pool.repository.json;

import java.io.PrintWriter;
import java.io.Serializable;

/**
 * <p>Corresponds to the information delivered
 * from the {@link org.dcache.pool.repository.v5.ReplicaRepository} using
 * {@link dmg.cells.nucleus.CellInfoProvider#getInfo(PrintWriter)}.</p>
 */
public class RepositoryData implements Serializable {
    private static final long serialVersionUID = -5462572587893174680L;
    private String label;
    private String  state;
    private Integer initializationProgress;
    private Integer files;
    private String  filesException;
    private String  totalDiskSpace;
    private Long    freeDiskSpace;
    private Long    usedDiskSpace;
    private Long    preciousDiskSpace;
    private Long    removableDiskSpace;
    private Double  usedDiskSpaceRatio;
    private Double  preciousDiskSpaceRatio;
    private Double  removableDiskSpaceRatio;
    private Long    fileSystemSize;
    private Long    fileSystemFree;
    private Double  fileSystemRatioFreeToTotal;
    private Long    fileSystemMaxSpace;
    private Long    staticallyConfiguredMax;
    private Long    runtimeConfiguredMax;
    private Long    gap;
    private Long    lru;

    public Long getFileSystemFree() {
        return fileSystemFree;
    }

    public Long getFileSystemMaxSpace() {
        return fileSystemMaxSpace;
    }

    public Double getFileSystemRatioFreeToTotal() {
        return fileSystemRatioFreeToTotal;
    }

    public Long getFileSystemSize() {
        return fileSystemSize;
    }

    public Integer getFiles() {
        return files;
    }

    public String getFilesException() {
        return filesException;
    }

    public Long getFreeDiskSpace() {
        return freeDiskSpace;
    }

    public Long getGap() {
        return gap;
    }

    public Integer getInitializationProgress() {
        return initializationProgress;
    }

    public String getLabel() {
        return label;
    }

    public Long getLru() {
        return lru;
    }

    public Long getPreciousDiskSpace() {
        return preciousDiskSpace;
    }

    public Double getPreciousDiskSpaceRatio() {
        return preciousDiskSpaceRatio;
    }

    public Long getRemovableDiskSpace() {
        return removableDiskSpace;
    }

    public Double getRemovableDiskSpaceRatio() {
        return removableDiskSpaceRatio;
    }

    public Long getRuntimeConfiguredMax() {
        return runtimeConfiguredMax;
    }

    public String getState() {
        return state;
    }

    public Long getStaticallyConfiguredMax() {
        return staticallyConfiguredMax;
    }

    public String getTotalDiskSpace() {
        return totalDiskSpace;
    }

    public Long getUsedDiskSpace() {
        return usedDiskSpace;
    }

    public Double getUsedDiskSpaceRatio() {
        return usedDiskSpaceRatio;
    }

    public void print(PrintWriter pw) {
        pw.append("State : ").append(String.valueOf(state));
        if (initializationProgress != null) {
            pw.append(" (").append(
                            String.valueOf(initializationProgress)).append(
                            "% done)");
        }
        pw.println();

        if (files != null) {
            pw.println("Files : " + files);
        } else if (filesException != null) {
            pw.println("Files : " + filesException);
        }

        pw.println("Disk space");
        pw.println("    Total    : " + totalDiskSpace);
        pw.println("    Used     : " + usedDiskSpace + "    ["
                                   + usedDiskSpaceRatio + "]");
        pw.println("    Free     : " + freeDiskSpace + "    Gap : " + gap);
        pw.println("    Precious : " + preciousDiskSpace + "    ["
                                   + preciousDiskSpaceRatio + "]");
        pw.println("    Removable: " + removableDiskSpace + "    ["
                                   + removableDiskSpaceRatio + "]");
        pw.println("File system");
        pw.println("    Size : " + fileSystemSize);
        pw.println("    Free : " + fileSystemFree +
                                   "    [" + fileSystemRatioFreeToTotal + "]");
        pw.println("Limits for maximum disk space");
        pw.println("    File system          : " + fileSystemMaxSpace);
        pw.println("    Statically configured: " + staticallyConfiguredMax);
        pw.println("    Runtime configured   : " + runtimeConfiguredMax);
    }

    public void setFileSystemFree(Long fileSystemFree) {
        this.fileSystemFree = fileSystemFree;
    }

    public void setFileSystemMaxSpace(Long fileSystemMaxSpace) {
        this.fileSystemMaxSpace = fileSystemMaxSpace;
    }

    public void setFileSystemRatioFreeToTotal(
                    Double fileSystemRatioFreeToTotal) {
        this.fileSystemRatioFreeToTotal = fileSystemRatioFreeToTotal;
    }

    public void setFileSystemSize(Long fileSystemSize) {
        this.fileSystemSize = fileSystemSize;
    }

    public void setFiles(Integer files) {
        this.files = files;
    }

    public void setFilesException(String filesException) {
        this.filesException = filesException;
    }

    public void setFreeDiskSpace(Long freeDiskSpace) {
        this.freeDiskSpace = freeDiskSpace;
    }

    public void setGap(Long gap) {
        this.gap = gap;
    }

    public void setInitializationProgress(Integer initializationProgress) {
        this.initializationProgress = initializationProgress;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public void setLru(Long lru) {
        this.lru = lru;
    }

    public void setPreciousDiskSpace(Long preciousDiskSpace) {
        this.preciousDiskSpace = preciousDiskSpace;
    }

    public void setPreciousDiskSpaceRatio(Double preciousDiskSpaceRatio) {
        this.preciousDiskSpaceRatio = preciousDiskSpaceRatio;
    }

    public void setRemovableDiskSpace(Long removableDiskSpace) {
        this.removableDiskSpace = removableDiskSpace;
    }

    public void setRemovableDiskSpaceRatio(Double removableDiskSpaceRatio) {
        this.removableDiskSpaceRatio = removableDiskSpaceRatio;
    }

    public void setRuntimeConfiguredMax(Long runtimeConfiguredMax) {
        this.runtimeConfiguredMax = runtimeConfiguredMax;
    }

    public void setState(String state) {
        this.state = state;
    }

    public void setStaticallyConfiguredMax(Long staticallyConfiguredMax) {
        this.staticallyConfiguredMax = staticallyConfiguredMax;
    }

    public void setTotalDiskSpace(String totalDiskSpace) {
        this.totalDiskSpace = totalDiskSpace;
    }

    public void setUsedDiskSpace(Long usedDiskSpace) {
        this.usedDiskSpace = usedDiskSpace;
    }

    public void setUsedDiskSpaceRatio(Double usedDiskSpaceRatio) {
        this.usedDiskSpaceRatio = usedDiskSpaceRatio;
    }
}
