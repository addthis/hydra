'use strict';

import React from 'react';
import {Cell} from 'fixed-data-table';

export default function LinkCell({href, children, style = {}, ...props}) {
	return (
		<Cell style={style} {...props}>
			<a href={href}>{children}</a>
		</Cell>
	);
}

LinkCell.propTypes = {
	href: React.PropTypes.string.isRequired,
	style: React.PropTypes.object
};
