'use strict';

import palette from 'style/color-palette';
import SearchBar from 'components/search/search-bar';
import React from 'react';
import {HotKeys} from 'react-hotkeys';
import keyMap from 'key-maps/default-key-map';
import autofocus from 'util/autofocus';
import {ServerStateError, ServerStateResponse} from 'actions/server-state-actions';
import $ from 'jquery';
import {view} from 'fluxthis';
import spawnStore from 'stores/instances/spawn-store-instance';

@view(spawnStore)
export default class SpawnView extends React.Component {
    state = {
        searchString: window.location.hash.substring(1),
        searchBarVisible: false
    }

    fetchServerState() {
        this.xhr = $.get('/update/setup')
            .done(res => this.dispatch(ServerStateResponse, res))
            .fail(err => this.dispatch(ServerStateError, err));
    }

    componentWillMount() {
        this.handlers = {
            searchAll: this.handleSearchAllJobs.bind(this),
            esc: this.handleEsc.bind(this)
        };


        // Poll for server state changes
        this.fetchServerState();
        this.interval = setInterval(() => this.fetchServerState(), 5000);

        window.addEventListener('hashchange', this.handleHashChange);
    }

    componentDidMount() {
        this.focusApp();
    }

    componentsWillUnmount() {
        this.handlers = null;

        // if xhr doesnt exist, or isnt mid-flight, it should throw an error
        try {this.xhr.abort();} catch(err) {}
        clearInterval(this.interval);

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
