import React from 'react';
import {Cell} from 'fixed-data-table';

export default function JobStatusCell({rowIndex, rows, ...props}) {
	return <Cell {...props}>{rows.get(rowIndex).get('status')}</Cell>;
}
