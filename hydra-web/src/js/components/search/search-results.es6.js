import oboe from 'oboe';
import React from 'react';
import SearchResult from './search-result';
import palette from 'style/color-palette';
import shallowCompare from 'react-addons-shallow-compare';

function SearchHeader({
    headerStyle,
    searchStringStyle,
    searchString,
    totalFiles,
    matchTotalsStyle,
    totalMatches,
    jobsWithMatches,
    done
}) {
    return (
        <div style={headerStyle}>
            Search results for <span style={searchStringStyle}> {searchString} </span>
            {totalFiles === 0 ? '... ' : `in ${totalFiles} jobs... `}
            {totalMatches > 0 || done ?
                <span style={matchTotalsStyle}>
                    found {totalMatches} occurences in {jobsWithMatches} jobs
                    {done ? '' : ' so far...'}
                </span> :
                null}
        </div>
    );
}

SearchHeader.propTypes = {
    headerStyle: React.PropTypes.object.isRequired,
    searchStringStyle: React.PropTypes.object.isRequired,
    matchTotalsStyle: React.PropTypes.object.isRequired,
    searchString: React.PropTypes.string.isRequired,
    totalFiles: React.PropTypes.number.isRequired,
    totalMatches: React.PropTypes.number.isRequired,
    jobsWithMatches: React.PropTypes.number.isRequired,
    done: React.PropTypes.bool.isRequired
};

function wrapSearchResult(type, searchResult) {
    let jobMatches = 0;

    const searchResultStyle = {
        paddingBottom: '1.5em'
    };

    const jobStyle = {
        color: palette.detail1
    };

    const metadataStyle = {
        color: palette.text2
    };

    const {id, results, description} = searchResult;

    const blobResults = results.map(result => {
        const {matches, contextLines, startLine} = result;

        jobMatches += matches.length;

        return (
            <div key={startLine} style={searchResultStyle}>
                <SearchResult
                    palette={palette}
                    key={id}
                    type={type}
                    id={id}
                    description={description}
                    matches={matches}
                    contextLines={contextLines}
                    startLine={startLine}
                />
            </div>
        );
    });

    return (
        <div key={id}>
            <span style={jobStyle}>
                <div>{description}</div>
                {`${type.slice(0, 1).toUpperCase() + type.slice(1).toLowerCase()} ${id}`}
            </span>
            <span style={metadataStyle}>
                {` (${jobMatches} matches)`}
            </span>
            {blobResults}
        </div>
    );
}

export default class SearchResults extends React.Component {
    static propTypes = {
        searchString: React.PropTypes.string.isRequired
    }

    state = {
        totalFiles: 0,
        totalMatches: 0,
        jobsWithMatches: 0,
        results: [],
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

        this.setState({
            totalFiles: 0,
            totalMatches: 0,
            jobsWithMatches: 0,
            jobs: [],
            macros: [],
            done: false
        });

        this.pendingSearch = oboe({url: `/search/all?q=${q}`})
            .node('!.totalFiles', (totalFiles) => {
                this.setState({totalFiles});
            })
            .node('!.jobs[*].results', (result) => {
                const matches = result.map(groupMatch => groupMatch.matches.length)
                    .reduce((a, b) => a + b, 0);

                this.setState({
                    jobsWithMatches: this.state.jobsWithMatches + 1,
                    totalMatches: this.state.totalMatches + matches
                });
            })
            .done((result) => {
                this.setState({
                    jobs: result.jobs,
                    macros: result.macros,
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
        const {totalMatches, done, jobsWithMatches, totalFiles, jobs, macros} = this.state;
        const {searchString} = this.props;

        const jobSearchResults = jobs.map(job => {
            return wrapSearchResult('job', job);
        });
        const macroSearchResults = macros.map(macro => {
            return wrapSearchResult('macro', macro);
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

        const dividerStyle = {
            color: palette.detail3
        };

        return (
            <div style={{backgroundColor: palette.background0}}>
                <SearchHeader
                    headerStyle={headerStyle}
                    totalMatches={totalMatches}
                    totalFiles={totalFiles}
                    searchStringStyle={searchStringStyle}
                    searchString={searchString}
                    matchTotalsStyle={matchTotalsStyle}
                    jobsWithMatches={jobsWithMatches}
                    done={done}
                />
                <div style={resultsContainerStyle}>
                    {macroSearchResults.length ?
                        <div style={dividerStyle}>Matches found in macros:</div> :
                        null}
                    {macroSearchResults}
                    {jobSearchResults.length ?
                        <div style={dividerStyle}>Matches found in jobs:</div> :
                        null}
                    {jobSearchResults}
                </div>
            </div>
        );
    }
}
