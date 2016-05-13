import React from 'react';
import Immutable from 'immutable';
import LinkCell from './link-cell';

export default function JobDescriptionCell({rows, rowIndex, ...props}) {
	const row = rows.get(rowIndex, Immutable.Map());
	const jobId = row.get('id');

	return (
		<LinkCell href={`/spawn2/index.html#jobs/${jobId}/conf`} {...props}>
			{row.get('description')}
		</LinkCell>
	);
}

JobDescriptionCell.propTypes = {
	rowIndex: React.PropTypes.number.isRequired,
	rows: React.PropTypes.instanceOf(Immutable.List).isRequired
};
