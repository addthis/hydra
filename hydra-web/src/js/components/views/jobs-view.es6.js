import React from 'react';
import {view} from 'fluxthis';
import spawnStore from 'stores/instances/spawn-store-instance';
import {JobTableSelectionChange, JobTableSortChange} from 'actions/job-table-actions';
import JobTable from 'components/tables/job-table';

const jobStore = spawnStore.jobStore;
const jobTableStore = spawnStore.jobTableStore;

@view(jobStore, jobTableStore)
export default class JobsView extends React.Component {
    static propTypes = {
        width: React.PropTypes.number.isRequired,
        height: React.PropTypes.number.isRequired
    };

    getStateFromStores() {
        return {
            jobs: jobStore.jobs,
            sortInfo: jobTableStore.sortInfo,
            selectedIDs: jobTableStore.selectedIDs
        };
    }

    render() {
        const {
            width,
            height
        } = this.props;

        const {
            jobs,
            selectedIDs,
            sortInfo
        } = this.state;

        return (
            <div>
                {/* TODO jobs header */}
                <JobTable
                    rows={jobs}
                    height={height}
                    width={width}
                    selectedIDs={selectedIDs}
                    sortInfo={sortInfo}
                    onSelectionChange={ids => this.dispatch(JobTableSelectionChange, ids)}
                    onSortChange={info => this.dispatch(JobTableSortChange, info)}
                />
            </div>
        );
    }
}
