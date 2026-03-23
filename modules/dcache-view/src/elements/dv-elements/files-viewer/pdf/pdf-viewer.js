class PdfViewer extends DcacheViewMixins.FilesViewerMixin(Polymer.Element)
{
    constructor(src)
    {
        super();
        if (src) this.src = src;
        this._previousListener = this._previousPage.bind(this);
        this._nextListener = this._nextPage.bind(this);
        this._goToListener = this._goToPage.bind(this);
        this._zoomInListener = this._zoomIn.bind(this);
        this._zoomOutListener = this._zoomOut.bind(this);
        this._rotateLeftListener = this._rotateLeft.bind(this);
        this._rotateRightListener = this._rotateRight.bind(this);

        this.pdfjsLib = window['pdfjs-dist/build/pdf'];
        this.pdfjsLib.GlobalWorkerOptions.workerSrc =
            `${window.location.href}bower_components/pdfjs-dist/build/pdf.worker.min.js`;
    }
    ready()
    {
        super.ready();
        Polymer.RenderStatus.afterNextRender(this, () => {
            this.pdfjsLib.getDocument(this.src).promise.then((doc) => {
                this.pdfDoc = doc;

                this.dispatchEvent(
                    new CustomEvent('dv-namespace-files-viewer-nav-page-total', {detail:{total:
                            this.pdfDoc.numPages}, bubbles:true, composed:true}));
                this.scale = 0.8;
            })
        });
    }
    connectedCallback()
    {
        super.connectedCallback();
        window.addEventListener('dv-namespace-files-viewer-nav-previous', this._previousListener);
        window.addEventListener('dv-namespace-files-viewer-nav-next', this._nextListener);
        window.addEventListener('dv-namespace-files-viewer-nav-goto', this._goToListener);
        window.addEventListener('dv-namespace-files-viewer-zoom-in', this._zoomInListener);
        window.addEventListener('dv-namespace-files-viewer-zoom-out', this._zoomOutListener);
        window.addEventListener('dv-namespace-files-viewer-rotate-left', this._rotateLeftListener);
        window.addEventListener('dv-namespace-files-viewer-rotate-right', this._rotateRightListener);
    }
    disconnectedCallback()
    {
        super.disconnectedCallback();
        window.removeEventListener('dv-namespace-files-viewer-nav-previous', this._previousListener);
        window.removeEventListener('dv-namespace-files-viewer-nav-next', this._nextListener);
        window.removeEventListener('dv-namespace-files-viewer-nav-goto', this._goToListener);
        window.removeEventListener('dv-namespace-files-viewer-zoom-in', this._zoomInListener);
        window.removeEventListener('dv-namespace-files-viewer-zoom-out', this._zoomOutListener);
        window.removeEventListener('dv-namespace-files-viewer-rotate-left', this._rotateLeftListener);
        window.removeEventListener('dv-namespace-files-viewer-rotate-right', this._rotateRightListener);
    }
    static get is()
    {
        return 'pdf-viewer';
    }
    static get properties()
    {
        return {
            pdfjsLib: {
                type: Object,
                notify: true
            },
            pdfDoc: {
                type: Object
            },
            pageRendering: {
                type: Boolean,
                value: false,
            },
            pageNumPending: {
                type: String,
                value: null
            },
            pageNum: {
                type: Number,
                value: 1,
                notify: true
            },
            scale: {
                type: Number,
                notify: true
            },
            rotate: {
                type: Number,
                notify: true
            }
        }
    }
    static get observers()
    {
        return [
            '_observePageChange(pageNum)',
            '_refresh(scale)',
            '_refresh(rotate)'
        ];
    }
    _load()
    {
        this._done()
    }
    _observePageChange(pageNum)
    {
        this.dispatchEvent(
            new CustomEvent('dv-namespace-files-viewer-nav-page-change',
                {detail: {page: pageNum}, bubbles:true, composed:true})
        );
    }
    _renderPage(num)
    {
        this.pageRendering = true;
        const canvas = this.$.canvas;
        const ctx = canvas.getContext('2d');

        this.pdfDoc.getPage(num).then((page) => {
            const viewport = this.rotate === undefined ? page.getViewport(this.scale):
                page.getViewport(this.scale, this.rotate);
            canvas.height = viewport.height;
            canvas.width = viewport.width;
            const renderContext = {
                canvasContext: ctx,
                viewport: viewport
            };
            const renderTask = page.render(renderContext);
            renderTask.promise.then(() => {
                this.pageRendering = false;
                if (this.pageNumPending !== null) {
                    this._renderPage(this.pageNumPending);
                    this.pageNumPending = null;
                }
            });
        });
    }
    _queueRenderPage(num)
    {
        if (this.pageRendering) {
            this.pageNumPending = num;
        } else {
            this._renderPage(num);
        }
    }
    _previousPage()
    {
        if (this.pageNum <= 1) {
            return;
        }
        this.pageNum--;
        this._queueRenderPage(this.pageNum);
    }
    _nextPage()
    {
        if (this.pageNum >= this.pdfDoc.numPages) {
            return;
        }
        this.pageNum++;
        this._queueRenderPage(this.pageNum);
    }
    _goToPage(e)
    {
        this.pageNum = +e.detail.page;
        this._queueRenderPage(this.pageNum);
    }
    _refresh()
    {
        this._renderPage(this.pageNum);
    }
    _zoomIn()
    {
        this.scale += 0.2;
    }
    _zoomOut()
    {
        this.scale -= 0.2;
    }
    _rotateLeft()
    {
        this.rotate = this.rotate ? this.rotate - 90 : - 90;
    }
    _rotateRight()
    {
        this.rotate = this.rotate ? this.rotate + 90 : 90;
    }
}
window.customElements.define(PdfViewer.is, PdfViewer);