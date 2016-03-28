'use strict';

import React from 'react';

const colors = {
	darkGray: '#272822',
	gray: '#696161',
	lightGray: '#B7AEAE',
	offWhite: '#F6F6F6',
	darkPink: '#F92672',
	lightBlue: '#66D9EF',
	limeGreen: '#A6E22E',
	orange: '#FD971F'
};

class ColorPalette {
	constructor() {
		this.background0 = colors.darkGray;
		this.background1 = colors.offWhite;
		this.background2 = colors.gray;
		this.background3 = colors.lightGray;
		this.text0 = colors.offWhite;
		this.text1 = colors.darkGray;
		this.text2 = colors.lightGray;
		this.detail0 = colors.darkPink;
		this.detail1 = colors.lightBlue;
		this.detail2 = colors.limeGreen;
		this.detail3 = colors.orange;
	}
}

export default new ColorPalette();

export const optionalPaletteProp = React.PropTypes.instanceOf(ColorPalette);
export const requiredPaletteProp = React.PropTypes.instanceOf(ColorPalette).isRequired;
