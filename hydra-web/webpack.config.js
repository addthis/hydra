// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

// http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/*eslint-env node*/

'use strict';

const path = require('path');

function getResolve() {
    return {
        root: [
            __dirname + '/src/js'
        ],

        modulesDirectories: ['modules', 'node_modules'],

        /* Migrated from RequireJS require.config paths */
        alias: {
            'jscookie': 'js-cookie',
            'domReady': 'vendor/domReady',
            'date': 'vendor/date',
            'json': 'vendor/json',
            'text': 'vendor/text',
            'localstorage': 'backbone.localstorage',
            'alertify': 'vendor/alertify',
            'datatables.net': 'vendor/datatable-1.9.4-modified',
            'js-mode': 'brace/mode/javascript',
            'js-worker': 'brace/worker/worker',
            'ace': 'brace',
            'ace/ext/searchbox': 'brace/ext/searchbox',
            'git.template': '../../templates/git.properties.html'
        },
        extensions: ['', '.es6.js', '.js']
    };
}

function getLoaders() {
    return [
        {
            test: /\.es6.js$/,
            loader: 'babel-loader',
            query: {
                presets: ['stage-0', 'es2015', 'react'],
                plugins: [
                    'babel-plugin-transform-decorators-legacy',
                    'transform-class-properties'
                ]
            }
        },
        {
            test: /\.css$/,
            loader: 'css-loader'
        },
        {
            test: /\.(jpe?g|png|gif|svg)$/i,
            loader: 'url?limit=10000!img?progressive=true'
        }
    ];
}

module.exports = [{
    entry: 'main',
    output: {
        filename: 'main.js',
        publicPath: '/spawn2/build/',
        path: path.resolve(__dirname, '../hydra-main/web/spawn2/build')
    },
    module: {
        loaders: getLoaders().concat([
            /* require.js shims */
            {
                test: require.resolve('./src/js/vendor/datatable-1.9.4-modified'),
                loader: 'imports?$=jquery,jQuery=jquery'
            },
            {
                test: /node_modules\/bootstrap/,
                loader: 'imports?$=jquery,jQuery=jquery'
            }
        ])
    },
    resolve: getResolve(),
    devtool: '#source-map'
}, {
    entry: 'search-results',
    output: {
        filename: 'search-results.js',
        publicPath: '/spawn2/build/',
        path: path.resolve(__dirname, '../hydra-main/web/spawn2/build')
    },
    module: {
        loaders: getLoaders()
    },
    resolve: getResolve(),
    devtool: '#source-map'
}];
