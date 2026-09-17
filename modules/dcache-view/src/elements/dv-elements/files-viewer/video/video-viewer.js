class VideoViewer extends DcacheViewMixins.FilesViewerMixin(Polymer.Element)
{
    constructor(src)
    {
        super();
        if (src) this.src = src;
        this._stopListener = this._stop.bind(this);
    }
    static get is()
    {
        return 'video-viewer';
    }
    connectedCallback()
    {
        super.connectedCallback();
        window.addEventListener('dv-namespace-close-files-viewer-appliance', this._stopListener);
    }
    disconnectedCallback()
    {
        super.disconnectedCallback();
        window.removeEventListener('dv-namespace-close-files-viewer-appliance', this._stopListener);
    }
    _load(src)
    {
        this.$.video.src = src;
        this._done();
        this.$.video.play();
    }
    _stop()
    {
        this.$.video.pause();
    }
    /*_error(err) {
        throw new Error ("Sorry, your browser doesn't support embedded videos, " +
            "but don't worry, you can download it and watch it with your favorite " +
            "video player!")
    }*/

}
window.customElements.define(VideoViewer.is, VideoViewer);