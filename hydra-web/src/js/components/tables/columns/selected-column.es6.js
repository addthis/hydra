'use strict';

import React from 'react';
import {Cell, Column} from 'fixed-data-table';

function SelectedCell({selected, onChange, ...props}) {
    return (
        <Cell {...props}>
            <input type={'checkbox'}
                checked={selected}
                onChange={() => onChange(!selected)}
            />
        </Cell>
    );
}

SelectedCell.propTypes = {
    selected: React.PropTypes.bool.isRequired,
    onChange: React.PropTypes.func.isRequired
};

export default function SelectedColumn({rows, onChange, width = 50}) {
    return (
        <Column
            cell={({rowIndex, ...props}) => {
                return (
                    <SelectedCell rowIndex={rowIndex}
                        selected={Boolean(rows[rowIndex].selected)}
                        onChange={newVal => onChange(rowIndex, newVal)}
                        {...props}
                    />
                );
            }}
            width={width}
        />
    );
}

SelectedColumn.propTypes = {
    rows: React.PropTypes.arrayOf(React.PropTypes.bool).isRequired,
    onChange: React.PropTypes.func.isRequired,
    width: React.PropTypes.number.isRequired
};
