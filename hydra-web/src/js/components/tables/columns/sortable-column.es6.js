'use strict';

import React from 'react';
import {Cell, Column} from 'fixed-data-table';
import SortableHeaderCell from 'components/tables/cells/sortable-header-cell';

function DefaultCell({rowIndex, rows, field, ...props}) {
    return <Cell {...props}>{rows[rowIndex][field]}</Cell>;
}

DefaultCell.propTypes = {
    rowIndex: React.PropTypes.number.isRequired,
    rows: React.PropTypes.arrayOf(React.PropTypes.object).isRequired,
    field: React.PropTypes.string.isRequired
};

export default function SortableColumn({
    rows,
    title,
    field,
    sortDir,
    onSort,
    sorted,
    isResizable = true,
    cell = DefaultCell,
    columnKey = field,
    width = 200,
    flexGrow = 0
}) {
    return (
        <Column
            header={
                <SortableHeaderCell
                    label={title}
                    sortDir={sorted ? sortDir : 0}
                    onOrderChange={order => onSort(field, order)}
                />
            }
            cell={props => cell({rows, field, ...props})}
            width={width}
            isResizable={isResizable}
            columnKey={columnKey}
            flexGrow={flexGrow}
        />
    );
}

SortableColumn.propTypes = {
    sorted: React.PropTypes.bool.isRequired,
    sortDir: React.PropTypes.oneOf([-1, 0, 1]).isRequired,
    rows: React.PropTypes.arrayOf(React.PropTypes.object).isRequired,
    onSort: React.PropTypes.func.isRequired,
    field: React.PropTypes.string.isRequired,
    title: React.PropTypes.string.isRequired,
    width: React.PropTypes.number.isRequired,
    cell: React.PropTypes.func,
    isResizable: React.PropTypes.bool,
    columnKey: React.PropTypes.string,
    flexGrow: React.PropTypes.number
};
