'use strict';

import SearchResults from 'components/search/search-results';
import SearchBar from 'components/search/search-bar';
import React from 'react';
import palette from 'style/color-palette';
import {HotKeys} from 'react-hotkeys';
import keyMap from 'key-maps/default-key-map';
import autofocus from 'util/autofocus';

export default class SearchResultsView extends React.Component {
    constructor(props) {
        super(props);
        this.handleHashChange = this.handleHashChange.bind(this);
    }

    state = {
        searchString: window.location.hash.substring(1),
        searchBarVisible: false
    }

    componentWillMount() {
        this.handlers = {
            searchAll: this.handleSearchAllJobs.bind(this),
            esc: this.handleEsc.bind(this)
        };

        window.addEventListener('hashchange', this.handleHashChange);
    }

    componentDidMount() {
        this.focusApp();
    }

    componentsWillUnmount() {
        this.handlers = null;

        window.removeEventListener('hashchange', this.handleHashChange);
    }

    handleHashChange() {
        this.setState({
            searchString: window.location.hash.substring(1)
        });
    }

    handleSearchAllJobs() {
        this.setState({
            searchBarVisible: true
        });
    }

    handleEsc() {
        this.focusApp();
        this.setState({
            searchBarVisible: false
        });
    }

    focusApp() {
        autofocus(this.refs.hotKeys);
    }

    handleSeachBarSubmit(searchTerm) {
        window.location.hash = searchTerm;
    }

    render() {
        const {
            searchBarVisible,
            searchString
        } = this.state;

        return (
            <HotKeys
                keyMap={keyMap}
                handlers={this.handlers}
                ref={'hotKeys'}>
                <SearchResults
                    palette={palette}
                    searchString={searchString}
                />
                <SearchBar
                    palette={palette}
                    visible={searchBarVisible}
                    onSubmit={this.handleSeachBarSubmit.bind(this)}
                />
            </HotKeys>
        );
    }
}
