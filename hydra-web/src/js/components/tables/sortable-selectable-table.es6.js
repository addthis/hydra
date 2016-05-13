'use strict';

import React from 'react';
import Immutable from 'immutable';
import {Table, Column, Cell} from 'fixed-data-table';
import {immutableRenderDecorator as pure} from 'react-immutable-render-mixin';
import autobind from 'autobind-decorator';
import calculateWidths from './util/calculate-widths';
import SortableHeaderCell from 'components/tables/cells/sortable-header-cell';

@pure
export default class SortableSelectableTable extends React.Component {
    static propTypes = {
        rows: React.PropTypes.instanceOf(Immutable.List).isRequired,
        columns: React.PropTypes.arrayOf(React.PropTypes.shape({
            columnKey: React.PropTypes.string.isRequired,
            weight: React.PropTypes.number.isRequired,
            header: React.PropTypes.node.isRequired,
            cell: React.PropTypes.func,
            sort: React.PropTypes.func
        })).isRequired,
        idField: React.PropTypes.string.isRequired,
        selectedIDs: React.PropTypes.instanceOf(Immutable.Set).isRequired,
        onSelectionChange: React.PropTypes.func.isRequired,
        width: React.PropTypes.number.isRequired,
        height: React.PropTypes.number.isRequired,
        sortInfo: React.PropTypes.instanceOf(Immutable.OrderedMap),
        onSortChange: React.PropTypes.func.isRequired,
        rowHeight: React.PropTypes.number,
        headerHeight: React.PropTypes.number,
        selectColumnWidth: React.PropTypes.number
    };

    static defaultProps = {
        rowHeight: 30,
        headerHeight: 35,
        selectColumnWidth: 40,
        sortInfo: Immutable.OrderedMap()
    };

    constructor(props) {
        super(props);

        const {width, columns} = props;
        const widths = columns.map(col => {
            return {
                key: col.columnKey,
                value: col.weight
            };
        });

        // The table keeps its own array in state so it doesn't have to re-sort
        this.state = {
            columnWidths: calculateWidths(width - props.selectColumnWidth, widths),
            rows: this.getSortedRows(props)
        };
    }

    componentWillReceiveProps(newProps) {
        const {
            rows,
            sortInfo
        } = this.props;

        // If the sort or the data changes, we need to update our internal
        // representation of the data
        if (!Immutable.is(newProps.rows, rows) ||
            !Immutable.is(newProps.sortInfo, sortInfo)) {
            this.setState({
                rows: this.getSortedRows(newProps)
            });
        }
    }

    @autobind
    defaultCell(columnKey) {
        return ({rowIndex, ...props}) => {
            const {rows} = this.state;
            return <Cell {...props}>{rows.get(rowIndex).get(columnKey)}</Cell>;
        };
    }

    /**
     * Creates a map of id => column data for easier lookup
     * @return {Object}
     */
    @autobind
    createColumnMap() {
        const map = {};

        this.props.columns.forEach(col => {
            map[col.columnKey] = col;
        });

        return map;
    }

    /**
     * Combines multiple comparator functions into a single one, which executes the individual
     * comparators in order, until an inequality is found.
     * @param  {Function[]} funcs
     * @return {Function}
     */
    @autobind
    composeComparators(funcs) {
        return function composedComparator(a, b) {
            for (let func of funcs) {
                const result = func(a, b);
                if (result !== 0) {
                    return result;
                }
            }

            console.log('equal?', a.toJS(), b.toJS());
            return 0;
        };
    }

    /**
     * Take rows, columns, sortInfo from props, and return a sorted ImmutableList of rows.
     * @return {Immutable.List}
     */
    @autobind
    getSortedRows({rows, sortInfo}) {
        const columnMap = this.createColumnMap();

        const fns = sortInfo.mapEntries(([columnKey, direction]) => {
            const {sort} = columnMap[columnKey];

            const fn = function (a, b) {
                if (direction === undefined || direction === 0) {
                    return 0;
                }
                else if (direction === 1) {
                    return sort(a.get(columnKey), b.get(columnKey));
                }
                else {
                    return sort(b.get(columnKey), a.get(columnKey));
                }
            };

            return [columnKey, fn];
        });

        // Last added sort fn should be highest priority (eval'd first)
        const sort = this.composeComparators(fns.toList().reverse());

        return rows.sort(sort);
    }

    @autobind
    handleResizeEnd(columnWidth, columnKey) {
        this.setState(({columnWidths}) => ({
            columnWidths: {
                ...columnWidths,
                [columnKey]: columnWidth
            }
        }));
    }

    @autobind
    handleRowSelect(id, selected) {
        const {
            onSelectionChange,
            selectedIDs
        } = this.props;

        const newSelection = selected ?
            selectedIDs.add(id) : selectedIDs.remove(id);

        this.setState({selectedIDs: newSelection});
        onSelectionChange(newSelection);
    }

    @autobind
    createSelectionCell({rowIndex, ...props}) {
        const {rows} = this.state;
        const {
            selectedIDs,
            idField
        } = this.props;

        const id = rows.get(rowIndex, Immutable.Map()).get(idField);
        const selected = selectedIDs.has(id);

        return (
            <Cell {...props}>
                <input type={'checkbox'}
                    checked={selected}
                    onChange={() => this.handleRowSelect(id, !selected)}
                />
            </Cell>
        );
    }

    @autobind
    createSelectionColumn() {
        return (
            <Column width={this.props.selectColumnWidth}
                isResizable={false}
                cell={this.createSelectionCell}
            />
        );
    }

    @autobind
    createDataColumns() {
        const {
            columns,
            sortInfo,
            onSortChange
        } = this.props;

        const {
            columnWidths
        } = this.state;

        return columns.map(col => {
            const {
                columnKey,
                cell,
                header
            } = col;

            return (
                <Column key={columnKey}
                    header={
                        <SortableHeaderCell label={header}
                            sortDir={sortInfo.get(columnKey, 0)}
                            onOrderChange={direction => onSortChange(columnKey, direction)}
                        />
                    }
                    width={columnWidths[columnKey]}
                    cell={cell !== undefined ? cell : this.defaultCell(columnKey)}
                />
            );
        });
    }

    render() {
        const {rows} = this.state;
        const selectionColumn = this.createSelectionColumn();
        const dataColumns = this.createDataColumns();

        // The child nodes of Table MUST be actual Columns, so we can't render our stateless guys
        // here, instead, we have to call them as functions.
        return (
            <Table
                {...this.props}
                rowsCount={rows.size}
                onColumnResizeEndCallback={this.handleResizeEnd}
                isColumnResizing={false}>
                {selectionColumn}
                {dataColumns}
            </Table>
        );
    }
}
