'use strict';

import React from 'react';
import {Cell} from 'fixed-data-table';

function convertBytesToString(bytes) {
	const units = ['B', 'K', 'M', 'G', 'T'];
	let shorterSize = bytes;

	if (typeof bytes === 'number' && bytes > 0) {
		let u;
		for (u = 0; u < units.length && Math.floor(shorterSize / 10) > 10; u++) {
			shorterSize = (shorterSize / 1024).toFixed(1);
		}
		return shorterSize + ' ' + units[u];
	}
	else {
		return '-';
	}
}

export default function BytesCell({size, ...props}) {
	return <Cell {...props}>{convertBytesToString(size)}</Cell>;
}

BytesCell.propTypes = {
	size: React.PropTypes.number.isRequired
};
