import React from 'react';
import Immutable from 'immutable';
import {Cell} from 'fixed-data-table';

const states = [
    'IDLE',
    'SCHEDULED',
    'RUNNING',
    'DEGRADED',
    'UNKNOWN',
    'ERROR',
    'REBALANCE'
];

const stateLabels = [
    'label-default',
    'label-info',
    'label-success',
    'label-inverse',
    'label-inverse',
    'label-danger',
    'label-info'
];

export default function JobStateCell({rows, rowIndex, ...props}) {
	const jobState = rows.get(rowIndex).get('state');

	return (
		<Cell {...props}>
			<span className={stateLabels[jobState]}>
				{states[jobState]}
			</span>
		</Cell>
	);
}

JobStateCell.propTypes = {
	rowIndex: React.PropTypes.number.isRequired,
	rows: React.PropTypes.instanceOf(Immutable.List).isRequired
};
