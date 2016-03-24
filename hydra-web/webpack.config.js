// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

// http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

var path = require('path');

module.exports = {
	entry: 'main',
    output: {
        filename: 'main.js',
        publicPath: '/spawn2/build/',
        path: path.resolve(__dirname, '../hydra-main/web/spawn2/build')
    },
	module: {
		loaders: [
			{
				test: /\.es6\.js$/, 
				exclude: /node_modules/, 
				loader: 'babel-loader',
			},
			{
				test: /\.css$/, 
				loader: 'css-loader',
			},
			{
				test: /\.(jpe?g|png|gif|svg)$/i,
				loader: 'url?limit=10000!img?progressive=true'
			},

			/* require.js shims */
            {
                test: require.resolve('datatables.net'),
                loader: 'imports?$=jquery,jQuery=jquery'
            },
			{
                test: /node_modules\/bootstrap/,
                loader: 'imports?$=jquery,jQuery=jquery'
            },
		]
	},
	resolve: {
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
	        'js-mode': 'brace/mode/javascript',
	        'js-worker': 'brace/worker/worker',
	        'ace': 'brace',
	        'ace/ext/searchbox': 'brace/ext/searchbox',
	        'git.template': '../../templates/git.properties.html'
		},
		extensions: ['', '.es6.js', '.js']
	},
	devtool: '#source-map'
}
