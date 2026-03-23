'use strict';

const gulp = require('gulp');
const del = require('del');
const mergeStream = require('merge-stream');
const polymerBuild = require('polymer-build');

const polymerJson = require('./polymer.json');
const polymerProject = new polymerBuild.PolymerProject(polymerJson);
const buildDirectory = 'target';
const copyAllArray = [
    {"source" : "./src/robots.txt", "destination": buildDirectory},
    {"source" : "./src/index.html", "destination": buildDirectory},
    {"source" : "./src/favicons/*", "destination": `${buildDirectory}/favicons`},
    {"source" : "./src/styles/main.css", "destination": `${buildDirectory}/styles`},
    {"source" : "./target/elements/src/elements/elements.html", "destination": `${buildDirectory}/elements`},
    {"source" : "./src/scripts/**/*", "destination": `${buildDirectory}/scripts`, "exclude": ["./src/scripts/config.js"]},
    {"source" : "./src/bower_components/pdfjs-dist/build/**/*", "destination": `${buildDirectory}/bower_components/pdfjs-dist/build`},
    {"source" : "./src/bower_components/webcomponentsjs/**/*", "destination": `${buildDirectory}/bower_components/webcomponentsjs`}
];

function waitFor(stream) {
    return new Promise((resolve, reject) => {
        stream.on('end', resolve);
        stream.on('error', reject);
    });
}
function copy(source, destination, exclude) {
    let arr;
    if (exclude && exclude !== "") {
        const len = exclude.length;
        for (let i=0; i<len; i++) {
            exclude[i] = `!${exclude[i]}`;
        }
        arr = [source, ...exclude]
    } else {
        arr = [source];
    }
    return gulp.src(arr, {base: ''})
        .pipe(gulp.dest(`${destination}`));
}
function build() {
    return new Promise((resolve, reject) => {
        let sourcesStreamSplitter = new polymerBuild.HtmlSplitter();
        let dependenciesStreamSplitter = new polymerBuild.HtmlSplitter();
        console.log(`Deleting ${buildDirectory} directory...`);
        del([buildDirectory])
            .then(() => {
                let sourcesStream = polymerProject.sources()
                    .pipe(sourcesStreamSplitter.split())
                    .pipe(sourcesStreamSplitter.rejoin());
                let dependenciesStream = polymerProject.dependencies()
                    .pipe(dependenciesStreamSplitter.split())
                    .pipe(dependenciesStreamSplitter.rejoin());
                let buildStream = mergeStream(sourcesStream, dependenciesStream)
                    .once('data', () => {
                        console.log('Analyzing build dependencies...');
                    });
                buildStream = buildStream.pipe(polymerProject.bundler());
                buildStream = buildStream.pipe(gulp.dest(`${buildDirectory}/elements`));
                return waitFor(buildStream);
            })
            .then(() => {
                console.log('Bundling complete!');
                resolve();
            });
    })
}
gulp.task('copy-all', function (done) {
    console.log('Start copying files...');
    const len = copyAllArray.length;
    for (let i = 0; i < len; i++) {
        console.log(`copying ${copyAllArray[i].source} to ${copyAllArray[i].destination}`);
        copy(copyAllArray[i].source, copyAllArray[i].destination,
            copyAllArray[i].exclude ? copyAllArray[i].exclude: "")
    }
    console.log('Files copying completed!');
    done();
});
gulp.task('build', build);
gulp.task('delete', function (done) {
    console.log(`Deleting ./target/elements/src directory...`);
    del(['./target/elements/src']);
    done();
});
gulp.task('default', gulp.series('build', 'copy-all', 'delete'));