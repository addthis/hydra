import React from 'react';
import Immutable from 'immutable';
import moment from 'moment';
import {Cell} from 'fixed-data-table';

export default function TimeCell({rows, rowIndex, ...props}) {
	const row = rows.get(rowIndex, Immutable.Map());
	const time = row.get('submitTime');

	return (
		<Cell {...props}>
			{time > 0 ? moment(time).format('MM/DD/YY, h:mm:ss a') : '-'}
		</Cell>
	);
}

TimeCell.propTypes = {
	rows: React.PropTypes.instanceOf(Immutable.List).isRequired,
	rowIndex: React.PropTypes.number.isRequired
};

