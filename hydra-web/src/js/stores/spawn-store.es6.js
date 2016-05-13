import {Store} from 'fluxthis';
import JobStore from 'stores/job-store';
import JobTableStore from 'stores/job-table-store';

export default class SpawnStore extends Store {
	constructor() {
		const jobStore = new JobStore();
		const jobTableStore = new JobTableStore();

		super(jobStore, jobTableStore);
		this.jobStore = jobStore;
		this.jobTableStore = jobTableStore;
	}
}
