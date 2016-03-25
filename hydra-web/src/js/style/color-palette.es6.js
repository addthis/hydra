import React from 'react';

const colors = {
	darkGray: '#272822',
	gray: '#B7AEAE',
	darkPink: '#F92672',
	lightBlue: '#66D9EF',
	limeGreen: '#A6E22E',
	orange: '#FD971F',
	offWhite: '#F6F6F6'
};

export default colors;
export const colorPropType = React.PropTypes.oneOf(
	Object.keys(colors).map(name => colors[name])
);