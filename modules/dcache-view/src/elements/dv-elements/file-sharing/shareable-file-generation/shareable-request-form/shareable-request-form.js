class ShareableRequestForm extends Polymer.Element
{
    constructor(fn,fp,ft)
    {
        super();
        this.fileName = fn;
        this.filePath = fp;
        this.fileType = ft;
        this.dirOrFile = this.typeChanged(this.fileType);
    }
    static get is()
    {
        return "shareable-request-form";
    }
    static get properties()
    {
        return {
            fileName: {
                type: String,
                notify: true
            },
            filePath: {
                type: String,
                notify: true
            },
            fileType: {
                type: String,
                observer: 'typeChanged',
                notify: true
            },
            dirOrFile: {
                type: String,
                notify: true
            },
            activities: {
                type: Array,
                value: function () {
                    return [
                        {name: 'Download', description: '', checked: true},
                        {name: 'Upload', description: '', checked: false},
                        {name: 'Delete', description: '', checked: false},
                        {name: 'List', description: '', checked: true},
                    ];
                },
                notify: true
            },
            loading: {
                type: Boolean,
                notify: true
            },
        }
    }
    typeChanged(fileType) {
        this.dirOrFile = fileType === 'DIR' ? 'directory' : 'file';
    }
    _getLink()
    {
        this.loading = true;
        this.classList.add('none');
        const activitiesList = [];
        for (let i=0; i<4; i++) {
            if (this.activities[i].checked) activitiesList.push(this.activities[i].name.toUpperCase());
        }
        this.dispatchEvent(new CustomEvent('dv-file-sharing-generate-response', {
            detail: {
                activitiesList: activitiesList,
                validity: this.$.validity.selectedItem.getAttribute("value")
            }, bubbles: true, composed: true}));
    }
}
window.customElements.define(ShareableRequestForm.is, ShareableRequestForm);
