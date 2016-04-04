'use strict';

import SearchResultsView from 'components/views/search-results-view';
import ReactDOM from 'react-dom';
import React from 'react';
import palette from 'style/color-palette';

document.body.style.backgroundColor = palette.background0;

ReactDOM.render(
    <SearchResultsView/>,
    document.getElementById('render-target')
);
