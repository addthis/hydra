'use strict';

import React from 'react';
import SearchContextLine from './search-context-line';
import {requiredPaletteProp} from 'style/color-palette';

export default class SearchResult extends React.Component {
    static propTypes = {
        job: React.PropTypes.string.isRequired,
        description: React.PropTypes.node,
        contextLines: React.PropTypes.arrayOf(React.PropTypes.string).isRequired,
        startLine: React.PropTypes.number.isRequired,
        matches: React.PropTypes.arrayOf(
            React.PropTypes.shape({
                lineNum: React.PropTypes.number.isRequired,
                startChar: React.PropTypes.number.isRequired,
                endChar: React.PropTypes.number.isRequired
            })
        ).isRequired,
        palette: requiredPaletteProp
    }

    static defaultProps = {
        description: <i>No job description</i>
    }

    render() {
        const {
            matches,
            job,
            contextLines,
            startLine,
            palette
        } = this.props;

        // Group matches by line num
        const matchesByLine = {};
        matches.forEach(match => {
            const {lineNum} = match;

            if (matchesByLine[lineNum] === undefined) {
                matchesByLine[lineNum] = [];
            }

            matchesByLine[lineNum].push(match);
        });

        const digits = String(startLine + contextLines.length + 1).length;

        const lines = contextLines.map((line, i) => {
            return (
                <SearchContextLine
                    key={startLine + i + 1}
                    palette={palette}
                    job={job}
                    lineNum={startLine + i + 1}
                    lineNumPadding={digits}
                    text={line}
                    matches={matchesByLine[startLine + i]}
                />
            );
        });

        const style = {
            backgroundColor: palette.background0,
            color: palette.text0
        };

        return (
            <div style={style}>
                {lines}
            </div>
        );
    }
}
