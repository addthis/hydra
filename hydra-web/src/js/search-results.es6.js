import SearchResults from 'components/search/search-results';
import React from 'react';
import ReactDOM from 'react-dom';
import colors from 'style/color-palette';

const searchString = 'op';

document.body.style.backgroundColor = colors.darkGray;

function render() {
	const searchString = window.location.hash.substring(1);

	ReactDOM.render(
		React.createElement(SearchResults, {searchString}),
		document.getElementById('render-target')
	);
}

render();
window.onhashchange = render;