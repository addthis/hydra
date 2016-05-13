'use strict';

import React from 'react';
import LinkCell from './link-cell';
import Immutable from 'immutable';

export default function JobIDLinkCell({rows, rowIndex, ...props}) {
	const jobId = rows.get(rowIndex).get('id');
	const style = {
		overflow: 'hidden',
		width: '1000px'
	};

	return (
		<LinkCell href={`/spawn2/index.html#jobs/${jobId}/quick`} style={style} {...props}>
			{jobId}
		</LinkCell>
	);
}

JobIDLinkCell.propTypes = {
	rowIndex: React.PropTypes.number.isRequired,
	rows: React.PropTypes.instanceOf(Immutable.List).isRequired
};
