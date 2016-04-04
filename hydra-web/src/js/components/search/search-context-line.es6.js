'use strict';

import React from 'react';
import {requiredPaletteProp} from 'style/color-palette';

function FormattedLineNum({lineNumPadding, style, lineNum}) {
    let strLineNum = String(lineNum);

    while (strLineNum.length <= lineNumPadding) {
        strLineNum = ' ' + strLineNum;
    }

    return (
        <span style={style}>
            {strLineNum + ' '}
        </span>
    );
}

FormattedLineNum.propTypes = {
    lineNumPadding: React.PropTypes.number.isRequired,
    style: React.PropTypes.object.isRequired,
    lineNum: React.PropTypes.number.isRequired
};

export default function SearchContextLine(props) {
    const {
        job,
        text,
        lineNum,
        lineNumPadding,
        matches,
        palette
    } = props;

    const matchedLinkStyle = {
        color: palette.detail0,
        textDecoration: 'none'
    };

    const lineNumStyle = {
        color: palette.text2,
        borderRight: '1px solid ' + palette.text2
    };

    const content = [];

    let lastIndex = 0;
    while (matches.length) {
        const match = matches.shift();
        const first = text.slice(lastIndex, match.startChar);
        const middle = text.slice(match.startChar, match.endChar);
        const href = `/spawn2/#jobs/${job}/line/${lineNum}/col/${match.startChar}/conf`;
        lastIndex = match.endChar;
        content.push(
            first,
            <a style={matchedLinkStyle}
                key={match.startChar}
                target={'_blank'}
                href={href}>
                {middle}
            </a>
        );
    }

    const divStyle = {
        whiteSpace: 'pre-wrap',
        margin: 0
    };

    content.push(text.slice(lastIndex, text.length));

    return (
        <div style={divStyle}>
            <FormattedLineNum
                style={lineNumStyle}
                lineNum={lineNum}
                lineNumPadding={lineNumPadding}
            />
            {content}
        </div>
    );
}

SearchContextLine.propTypes = {
    job: React.PropTypes.string.isRequired,
    text: React.PropTypes.string.isRequired,
    lineNum: React.PropTypes.number.isRequired,
    lineNumPadding: React.PropTypes.number.isRequired,
    matches: React.PropTypes.arrayOf(
        React.PropTypes.shape({
            lineNum: React.PropTypes.number.isRequired,
            startChar: React.PropTypes.number.isRequired,
            endChar: React.PropTypes.number.isRequired
        })
    ),
    palette: requiredPaletteProp
};

SearchContextLine.defaultProps = {
    matches: []
};
