'use strict';

import React from 'react';
import Immutable from 'immutable';
import JobStateCell from './cells/job-state-cell';
import JobIDLinkCell from './cells/job-id-link-cell';
import JobStatusCell from './cells/job-status-cell';
import TimeCell from './cells/time-cell';
import BytesCell from './cells/bytes-cell';
import JobDescriptionCell from './cells/job-description-cell';
import stringComparator from 'comparators/string-comparator';
import numberComparator from 'comparators/number-comparator';
import SortableSelectableTable from './sortable-selectable-table';

export default class JobTable extends React.Component {
    static propTypes = {
        rows: React.PropTypes.instanceOf(Immutable.List).isRequired,
        height: React.PropTypes.number.isRequired,
        width: React.PropTypes.number.isRequired,

        // Immutable list of selected IDs
        selectedIDs: React.PropTypes.instanceOf(Immutable.Set).isRequired,

        onSelectionChange: React.PropTypes.func.isRequired,
        onSortChange: React.PropTypes.func.isRequired,
        sortInfo: React.PropTypes.instanceOf(Immutable.OrderedMap).isRequired
    }

    render() {
        const {
            rows,
            height,
            width,
            selectedIDs,
            onSelectionChange,
            onSortChange,
            sortInfo
        } = this.props;

        return (
            <SortableSelectableTable
                idField={'id'}
                selectedIDs={selectedIDs}
                onSelectionChange={onSelectionChange}
                onSortChange={onSortChange}
                rowHeight={30}
                rows={rows}
                width={width}
                height={height}
                headerHeight={40}
                sortInfo={sortInfo}
                columns={[
                    {
                        header: 'ID',
                        columnKey: 'id',
                        cell: props => <JobIDLinkCell rows={rows} {...props}/>,
                        sort: stringComparator,
                        weight: 4
                    },
                    {
                        header: 'Creator',
                        columnKey: 'creator',
                        sort: stringComparator,
                        weight: 3
                    },
                    {
                        header: 'State',
                        columnKey: 'state',
                        cell: props => <JobStateCell rows={rows} {...props}/>,
                        sort: numberComparator,
                        weight: 4
                    },
                    {
                        header: 'Status',
                        columnKey: 'status',
                        cell: props => <JobStatusCell rows={rows} {...props}/>,
                        sort: stringComparator,
                        weight: 4
                    },
                    {
                        header: 'Submitted',
                        columnKey: 'submitTime',
                        cell: props => <TimeCell rows={rows} {...props}/>,
                        sort: numberComparator,
                        weight: 4
                    },
                    {
                        header: 'Ended',
                        columnKey: 'endTime',
                        cell: props => <TimeCell rows={rows} {...props}/>,
                        sort: numberComparator,
                        weight: 4
                    },
                    {
                        header: 'Description',
                        columnKey: 'description',
                        cell: props => <JobDescriptionCell rows={rows} {...props}/>,
                        sort: stringComparator,
                        weight: 4
                    },
                    {
                        header: 'maxT',
                        columnKey: 'maxRunTime',
                        sort: numberComparator,
                        weight: 2
                    },
                    {
                        header: 'rekT',
                        columnKey: 'rekickTimeout',
                        sort: numberComparator,
                        weight: 2
                    },
                    {
                        header: 'Nodes',
                        columnKey: 'nodes',
                        sort: numberComparator,
                        weight: 2
                    },
                    {
                        header: 'Size',
                        columnKey: 'bytes',
                        cell: props => <BytesCell size={rows.get(props.rowIndex).get('bytes')} {...props}/>,
                        sort: numberComparator,
                        weight: 2
                    }
                ]}
            />
        );
    }
}
