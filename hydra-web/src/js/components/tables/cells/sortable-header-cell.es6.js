'use strict';

import React from 'react';
import {Cell} from 'fixed-data-table';

export default class SortableHeaderCell extends React.Component {
	static propTypes = {
		sortDir: React.PropTypes.oneOf([-1, 0, 1]),
		label: React.PropTypes.string.isRequired,
		onOrderChange: React.PropTypes.func.isRequired
	}

	static defaultProps = {
		sortDir: 0
	}

	constructor(props) {
		super(props);

		this.state = {
			sortDir: this.props.sortDir
		};

		this.handleClick = this.handleClick.bind(this);
	}

	componentWillReceiveProps(newProps) {
		if (newProps.sortDir !== this.state.sortDir) {
			this.setState({sortDir: newProps.sortDir});
		}
	}

	handleClick() {
		const {onOrderChange} = this.props;
		const sortDir = this.state.sortDir !== 0 ?
			0 - this.state.sortDir :
			-1;

		this.setState({sortDir});
		onOrderChange(sortDir);
	}

	render() {
		const {label, ...props} = this.props;
		const {sortDir} = this.state;

		return (
			<Cell onClick={this.handleClick} {...props}>
				<span>
					{label} {sortDir > 0 ? '▲' : sortDir < 0 ? '▼' : ''}
				</span>
			</Cell>
		);
	}
}
