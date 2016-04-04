'use strict';

import oboe from 'oboe';
import React from 'react';
import SearchResult from './search-result';
import palette from 'style/color-palette';
import shallowCompare from 'react-addons-shallow-compare';

export default class SearchResults extends React.Component {
    static propTypes = {
        searchString: React.PropTypes.string.isRequired
    }

    state = {
        totalFiles: 0,
        filesWithMatches: 0,
        results: {},
        done: false
    }

    componentWillMount() {
        this.performSearch(this.props.searchString);
    }

    componentWillReceiveProps(nextProps) {
        if (nextProps.searchString !== this.props.searchString) {
            this.performSearch(nextProps.searchString);
        }
    }

    shouldComponentUpdate(nextProps, nextState) {
        return shallowCompare(this, nextProps, nextState);
    }

    componentWillUnmount() {
        this.cancelCurrentSearch();
    }


    performSearch(searchString) {
        const q = encodeURIComponent(searchString);

        if (this.pendingSearch) {
            this.cancelCurrentSearch();
        }

        this.setState({results: {}, totalFiles: 0, filesWithMatches: 0});

        this.pendingSearch = oboe({url: `/search/all?q=${q}`})
            .node('!.totalFiles', (totalFiles) => {
                this.setState({totalFiles});
            })
            .node('!.jobs[*]', () => {
                this.setState({
                    filesWithMatches: this.state.filesWithMatches + 1
                });
            })
            .done((result) => {
                this.setState({
                    results: result.jobs,
                    done: true
                });
            });
    }

    cancelCurrentSearch() {
        if (this.pendingSearch) {
            this.pendingSearch.abort();
            this.pendingSearch = null;
        }
    }

    render() {
        let totalMatches = 0;
        const {done, filesWithMatches, totalFiles, results} = this.state;
        const {searchString} = this.props;

        const searchResultStyle = {
            paddingBottom: '1.5em'
        };

        const searchResults = Object.keys(results).map(jobId => {
            let jobMatches = 0;

            const groupedResults = results[jobId];

            const jobStyle = {
                color: palette.detail1
            };

            const metadataStyle = {
                color: palette.text2
            };

            const blobResults = groupedResults.map((result) => {
                const {matches, contextLines, startLine} = result;

                jobMatches += matches.length;
                totalMatches += matches.length;

                return (
                    <div key={startLine} style={searchResultStyle}>
                        <SearchResult
                            palette={palette}
                            key={jobId}
                            job={jobId}
                            matches={matches}
                            contextLines={contextLines}
                            startLine={startLine}
                        />
                    </div>
                );
            });

            return (
                <div key={jobId}>
                    <span style={jobStyle}>
                        {`Job ${jobId}`}
                    </span>
                    <span style={metadataStyle}>
                        {` (${jobMatches} matches)`}
                    </span>
                    {blobResults}
                </div>
            );
        });

        const headerStyle = {
            background: `linear-gradient(to bottom, ${palette.background0} 0%,${palette.background0} 50%,rgba(0,0,0,0) 100%)`,
            width: '100%',
            fontSize: '1em',
            paddingBottom: '5px',
            color: palette.text0,
            fontFamily: 'monospace',
            height: '2em',
            position: 'fixed',
            top: 0
        };

        const matchTotalsStyle = {
            color: palette.text2
        };

        const resultsContainerStyle = {
            paddingTop: '1.2em',
            fontFamily: 'monospace',
            fontSize: '1em',
            tabSize: 2
        };

        const searchStringStyle = {
            color: palette.detail3,
            fontStyle: 'italic'
        };

        return (
            <div style={{backgroundColor: palette.background0}}>
                <div style={headerStyle}>

                    Searching for <span style={searchStringStyle}> {searchString} </span>
                    {totalFiles === 0 ?
                        '... ' :
                        `in ${totalFiles} files... `}
                    {done ?
                        <span style={matchTotalsStyle}>
                            found {totalMatches} occurences in {filesWithMatches} jobs)
                        </span> :
                        ''}
                </div>
                <div style={resultsContainerStyle}>
                    {searchResults}
                </div>
            </div>
        );
    }
}
