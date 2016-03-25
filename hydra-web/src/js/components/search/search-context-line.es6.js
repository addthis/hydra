import React from 'react';
import {colorPropType} from 'style/color-palette';

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

export default function SearchContextLine(props) {
	const {
		job,
		text,
		lineNum,
		lineNumPadding,
		matches,
		lineNumberColor,
		lineGutterBorderColor,
		mainTextColor,
		matchedTextColor,
		backgroundColor
	} = props;

	const matchedLinkStyle = {
		color: matchedTextColor,
		textDecoration: 'none'
	};

	const lineNumStyle = {
		color: lineNumberColor,
		borderRight: '1px solid ' + lineGutterBorderColor
	};

	const href = `/spawn2/#jobs/${job}/conf`;
	const content = [];
	
	let lastIndex = 0;
	while (matches.length) {
		const match = matches.shift();
		const first = text.slice(lastIndex, match.startChar);
		const middle = text.slice(match.startChar, match.endChar);
		lastIndex = match.endChar;
		content.push(
			first, 
			<a style={matchedLinkStyle} target={'_blank'} href={href}>{middle}</a>
		);
	}

	const preStyle = {
		margin: 0
	};	

	content.push(text.slice(lastIndex, text.length));

	return (
		<div>
			<pre style={preStyle}>
				<FormattedLineNum 
					style={lineNumStyle} 
					lineNum={lineNum}
					lineNumPadding={lineNumPadding}
				/>
				{content}
			</pre>
		</div>
	);
}

SearchContextLine.propTypes = {
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
	lineNumberColor: colorPropType,
	lineGutterBorderColor: colorPropType,
	mainTextColor: colorPropType,
	matchedTextColor: colorPropType,
	backgroundColor: colorPropType
};

SearchContextLine.defaultProps = {
	matches: []
};
