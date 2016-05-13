import {Store, handle} from 'fluxthis';
import Immutable from 'immutable';
import {ServerStateResponse} from '../actions/server-state-actions';

export default class JobStore extends Store {
	constructor() {
		super();
		this.jobs = Immutable.List();
	}

	@handle(ServerStateResponse)
	update({jobs}) {
		this.jobs = Immutable.fromJS(jobs);
	}
}
