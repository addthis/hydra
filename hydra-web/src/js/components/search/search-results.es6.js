import oboe from 'oboe';
import React from 'react';
import SearchResult from './search-result';
import colors, {colorPropType} from 'style/color-palette';

export default class SearchResults extends React.Component {
	static propTypes = {
		searchString: React.PropTypes.string.isRequired
	}

	state = {
		totalFiles: 0,
		filesSearched: 0,
		results: []
	}

	componentWillMount() {
		this.performSearch(this.props.searchString);
	}

	componentWillUnmount() {
		this.cancelCurrentSearch();
	}

	componentWillReceiveProps(nextProps) {
		if (nextProps.searchString !== this.props.searchString) {
			this.performSearch(nextProps.searchString)	
		}
	}

	performSearch(searchString) {
		const q = encodeURIComponent(searchString);

		if (this.pendingSearch) {
			this.cancelCurrentSearch();
		}

		this.setState({results: [], totalFiles: 0, filesSearched: 0});
		
		this.pendingSearch = oboe({url: `/search/all?q=${q}`})
			.node('totalFiles', (totalFiles) => {
				this.setState({totalFiles});
			})
			.node('jobs[*]', (result, path) => {
				const {contextLines, matches, startLine} = result;
				const jobId = path.pop();

				// mutate and reset results to prevent a ton of extra array
				// allocation
				this.setState({
					filesSearched: this.state.filesSearched + 1,
					results: this.state.results.concat({
						jobId,
						contextLines,
						matches,
						startLine
					}),
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
		const {totalFiles, filesSearched, results} = this.state;
		const {searchString} = this.props;
		
		const searchResultStyle = {
			paddingBottom: '1.5em'
		};

		const resultsByJobId = results.reduce((collector, result) => {
			const {jobId} = result;

			if (collector[jobId] === undefined) {
				collector[jobId] = [];
			}

			collector[jobId].push(result);
			return collector;
		}, {})

		const searchResults = Object.keys(resultsByJobId).map(jobId => {
			let jobMatches = 0;

			const groupedResults = resultsByJobId[jobId];

			const jobStyle = {
				color: colors.lightBlue
			};

			const metadataStyle = {
				color: colors.gray
			};

			const blobResults = groupedResults.map((result) => {
				const {jobId, matches, contextLines, startLine} = result;

				jobMatches += matches.length;
				totalMatches += matches.length;

				return (
					<div key={jobId + ':' + startLine} style={searchResultStyle}>
						<SearchResult 
							backgroundColor={colors.darkGray}
							lineNumberColor={colors.gray}
							lineGutterBorderColor={colors.gray}
							metadataColor={colors.gray}
							mainTextColor={colors.offWhite}
							matchedTextColor={colors.darkPink}
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
				<div>
					<span style={jobStyle}>
						{`Job ${jobId}`}
					</span>
					<span style={metadataStyle}>
						{` (${jobMatches} matches)`}
					</span>
					{blobResults}
				</div>
			);
		})

		const headerStyle = {
			background: `linear-gradient(to bottom, ${colors.darkGray} 0%,${colors.darkGray} 50%,rgba(0,0,0,0) 100%)`,
			width: '100%',
			fontSize: '1em',
			paddingBottom: '5px',
			color: colors.offWhite,
			fontFamily: 'monospace',
			height: '2em',
			position: 'fixed',
			top: 0
		};

		const matchTotalsStyle = {
			color: colors.gray
		};


		const resultsContainerStyle = {
			paddingTop: '1.2em',
			fontFamily: 'monospace',
			fontSize: '1em',
			tabSize: 2
		};

		const searchStringStyle = {
			color: colors.orange,
			fontStyle: 'italic'
		};

		return (
			<div style={{backgroundColor: colors.darkGray}}>
				<div style={headerStyle}>
					Search results for 
					<span style={searchStringStyle}> {searchString}</span>
					<span style={matchTotalsStyle}> ({totalMatches} occurences)</span>
				</div>
				<div style={resultsContainerStyle}>
					{searchResults}
				</div>
			</div>
		);
	}
}