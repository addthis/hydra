'use strict';

import React from 'react';

const defaultHiddenStyle = {
	display: 'none'
};

const defaultVisibleStyle = {
	display: undefined
};

export default function FloatingBottomFooter({
	visible,
	children,
	visibleStyle = defaultVisibleStyle,
	hiddenStyle = defaultHiddenStyle
}) {
	const style = visible ? visibleStyle : hiddenStyle;

	return (
		<div style={style}>
			{children}
		</div>
	);
}

FloatingBottomFooter.propTypes = {
	visible: React.PropTypes.bool.isRequired,
	visibleStyle: React.PropTypes.object,
	hiddenStyle: React.PropTypes.object
};
