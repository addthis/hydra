'use strict';

/**
 * @typedef Weight
 * @attribute {String} key
 * @attribute {Number} weight
 */

/**
 * scales up relative 'weights' to fit the absolute 'width'
 * @param  {number} width
 * @param  {Weight[]} weights
 * @return {Object}
 */
export default function calculateWidths(width, weights) {
	let sum = 0;
	for (let weight of weights) {
		sum += weight.value;
	}

	const scale = width / sum;
	const widths = {};
	for (let weight of weights) {
		widths[weight.key] = weight.value * scale;
	}

	return widths;
}
