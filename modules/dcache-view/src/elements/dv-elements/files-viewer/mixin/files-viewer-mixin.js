if (!window.DcacheViewMixins) {
    window.DcacheViewMixins = {};
}

DcacheViewMixins.FilesViewerMixin =  Polymer.dedupingMixin((superclass) =>
{
    /**
     *
     *
     * @polymer
     * @mixinClass
     */
    return class extends superclass
    {
        static get properties()
        {
            return {
                src: {
                    type: String
                }
            };
        }
        static get observers()
        {
            return [ '_load(src)' ];
        }
        _done()
        {
            this.dispatchEvent(
                new CustomEvent('dv-namespace-files-viewer-finished-loading',
                    {bubbles:true, composed:true})
            );
        }
        _download()
        {
            this.dispatchEvent(
                new CustomEvent('dv-namespace-files-viewer-download',
                    {bubbles: true, composed: true})
            );
        }
        _error(err)
        {
            this.dispatchEvent(
                new CustomEvent('dv-namespace-files-viewer-error',
                    {detail:{message: err}, bubbles: true, composed: true})
            );
        }
    }
});