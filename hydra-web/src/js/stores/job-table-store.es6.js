import {Store, handle} from 'fluxthis';
import Immutable from 'immutable';
import {JobTableSelectionChange, JobTableSortChange} from '../actions/job-table-actions';

export default class JobTableStore extends Store {
	constructor() {
		super();
		this.sortInfo = Immutable.OrderedMap();
		this.selectedIDs = Immutable.Set();
	}

	/**
	 * @param  {String} info - the columnKey being sorted
	 */
	@handle(JobTableSortChange)
	updateSort(newInfo) {
		function nextDir(dir) {
											// -1, 0, 1
			const normal = dir + 1;			//  0, 1, 2
			const next = (normal + 1) % 3;
			return next - 1;				// -1, 0, 1
		}

		const current = this.sortInfo.get(newInfo, 0);
		this.sortInfo = this.sortInfo.remove(newInfo)
			.update(newInfo, current, nextDir);
	}

	@handle(JobTableSelectionChange)
	updateSelection(ids) {
		this.selectedIDs = ids;
	}
}
