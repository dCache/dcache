class SharedFilesPage extends DcacheViewMixins.Commons(Polymer.Element)
{
    static get is()
    {
        return "shared-files-page";
    }
    constructor()
    {
        super();
        this._openFileListener = this._openFile.bind(this);
        this._showSharedFileListListener = this._showSharedFileList.bind(this);
    }
    connectedCallback()
    {
        super.connectedCallback();
        window.addEventListener('dv-file-sharing-open-file', this._openFileListener);
        window.addEventListener('dv-file-sharing-show-file-list-page', this._showSharedFileListListener);
    }
    disconnectedCallback()
    {
        super.disconnectedCallback();
        window.removeEventListener('dv-file-sharing-open-file', this._openFileListener);
        window.removeEventListener('dv-file-sharing-show-file-list-page', this._showSharedFileListListener);
    }
    _openContextMenu(e)
    {
        if (e.screenX === 0 && e.screenY === 0) {
            //FIXME - use dispatchEvent
            const cc = new SharedFileListContextualContent(this.$['shared-file-list'].selectionMonitoring[0]);
            app.buildAndOpenContextMenu(e, cc, 245);
        }
    }
    _openFile(e)
    {
        if (e.detail.file.fileMetaData.fileType === "DIR") {
            //open
            this.$['shared-file-list'].classList.replace('normal', 'none');
            this.$['shared-directory-view'].classList.replace('none', 'normal');
            this.dispatchEvent(
                new CustomEvent('dv-file-sharing-page-switch', {
                    detail: {page: 'shared-directory-view'}, bubbles: true, composed: true}));
            this.$['shared-directory-view'].authenticationParameters = {
                "scheme": "Bearer",
                "value": e.detail.file.macaroon
            };
            this.$['shared-directory-view'].path = e.detail.file.filePath;
        } else if (e.detail.file.fileMetaData.fileType === "REGULAR") {
            //download
            const worker = new Worker('./scripts/tasks/download-task.js');
            const fileURL = this.getFileWebDavUrl(e.detail.file.filePath, "read")[0];
            worker.addEventListener('message', (file) => {
                worker.terminate();
                const windowUrl = window.URL || window.webkitURL;
                const url = windowUrl.createObjectURL(file.data);
                const link = app.$.download;
                link.href = url;
                link.download = e.detail.file.fileMetaData.fileName;
                link.click();
                windowUrl.revokeObjectURL(url);
            }, false);
            worker.addEventListener('error', (e)=> {
                console.info(e);
                worker.terminate();
                this.dispatchEvent(new CustomEvent('dv-namespace-show-message-toast', {
                    detail: {message: e.message}, bubbles: true, composed: true
                }));
            }, false);
            this.authenticationParameters = {"scheme": "Bearer", "value": e.detail.file.macaroon};
            worker.postMessage({
                'url' : fileURL,
                'mime' : e.detail.file.fileMetaData.fileMimeType,
                'upauth' : this.getAuthValue(),
                'return': 'blob'
            });
        }
    }
    _showSharedFileList()
    {
        this.$['shared-directory-view'].path = undefined;
        this.$['shared-file-list'].classList.replace('none', 'normal');
        this.$['shared-directory-view'].classList.replace('normal', 'none');
    }
}
window.customElements.define(SharedFilesPage.is, SharedFilesPage);