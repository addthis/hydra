'use strict';

import palette from 'style/color-palette';
import SearchBar from 'components/search/search-bar';
import React from 'react';
import {HotKeys} from 'react-hotkeys';
import keyMap from 'key-maps/default-key-map';
import autofocus from 'util/autofocus';

export default class SpawnView extends React.Component {
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

    render() {
        const {
            searchBarVisible
        } = this.state;

        return (
            <HotKeys
                keyMap={keyMap}
                handlers={this.handlers}
                ref={'hotKeys'}>
                {this.props.children}
                <SearchBar
                    palette={palette}
                    visible={searchBarVisible}
                    target={'_blank'}
                />
            </HotKeys>
        );
    }
}
