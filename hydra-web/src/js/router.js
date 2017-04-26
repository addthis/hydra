/*
 * Licensed under the Apache License, Version 2.0 (the 'License');
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

define([
    'backbone'
],
    function(Backbone){
        var Router = Backbone.Router.extend({
            initialize: function(){
                this.params = {};
            },
            routes: {
                '': 'showIndex',
                'jobs': 'showJobsTable',
                'jobs/compact': 'showJobCompactTable',
                'jobs/comfortable': 'showJobComfyTable',
                'jobs/:jobid/quick': 'showQuickTask',
                'jobs/:jobid/quick/:node': 'showQuickTaskDetail',

                'jobs/:jobid/conf': 'showJobConf',
                'jobs/:jobid/line/:line/conf': 'showJobConf',
                'jobs/:jobid/line/:line/col/:col/conf': 'showJobConf',

                'jobs/:jobid/conf/clone': 'showJobConfClone',
                'jobs/:jobid/settings': 'showJobSettings',
                'jobs/:jobid/settings/clone': 'showJobSettingsClone',
                'jobs/:jobid/alerts': 'showJobAlerts',
                'jobs/:jobid/alerts/clone': 'showJobAlertsClone',
                'jobs/:jobid/deps': 'showJobDeps',
                'jobs/:jobid/expanded': 'showJobExpConf',
                'jobs/:jobid/history': 'showJobHistory',
                'jobs/:jobid/history/:commit': 'showJobHistoryView',
                'jobs/:jobid/history/:commit/diff': 'showJobHistoryDiff',
                'jobs/:jobid/tasks': 'showJobTaskTable',
                'jobs/:jobid/tasks/:node': 'showJobTaskDetail',
                'hosts': 'showHostTable',
                'hosts/:uuid': 'showHostTaskDetail',
                'hosts/:uuid/tasks': 'showHostTaskDetail',
                'macros': 'showMacroTable',

                'macros/:name': 'showMacroDetail',
                'macros/:name/conf': 'showMacroDetail',
                'macros/:name/line/:line/conf': 'showMacroDetail',
                'macros/:name/line/:line/col/:col/conf': 'showMacroDetail',

                'alias': 'showAliasTable',
                'alias/:name': 'showAliasDetail',
                // 'alias/:jobid/:tab/conf': 'showJobConf',
                'commands': 'showCommandTable',
                'commands/:name': 'showCommandDetail',
                'git': 'showGitProperties',
                'rebalanceParams': 'showRebalanceParams',
                'alerts': 'showAlertsTable',
                'alertsFiltered/:jobIdFilter': 'showAlertsTableFiltered',
                'alerts/:alertId': 'showAlertsDetail',
                'alerts/:alertId/:jobIds': 'showAlertsDetail'
            }
        });
        return Router;
    });
