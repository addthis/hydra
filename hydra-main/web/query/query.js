/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
(function() {

/* stub console log if browser doesn't support it */
if ( !(window.console && console.log) ) {
    window.console = { 
        log: function() { }
    };
}

var busyimg = '<img width="32" height="32" src="spinner.gif">',
    navstack = [ "" ],
    nodinfsho = 0,
    queries = [],
    render = 1,
    maxnav = 25,
    store = window.localStorage || {},
    tabs = ['completedqueries','browse','runningqueries','setup'],
    // dict of query string kv-pairs
    qs = document.location.search.slice(1),
    qkv = qs.parseQuery(),
    jobid = qkv['job'] || fetchValue('job'),
    // dict of hash kv-pairs
    hs = document.location.hash.slice(1),
    hkv = {},
    hostUpdater=null,
    liveQueryPolling=null; 

// dict of hash kv-pairs
try {
    // console.log('trying... unesc(hs): '+hs);
    var conv = unesc(hs);
    for (var i=0; i<2 && conv.indexOf("=")<0; i++) {
        conv = unesc(conv);
    }
    hkv = conv.parseQuery();
} catch(e){
    console.log(['hkv fail',e]);
}

/* escape ++ */
function esc(v) {
    return encodeURIComponent(v || '').replace('-','%2d');
}

/* unescape ++ */
function unesc(v) {
    return decodeURIComponent(v || '');
}

/* turn rpc response into an object */
function rpcDecode(rpc) {
    return eval("("+rpc.responseText+")");
}

/* hide and show selected elements */
function showTab(tab) {
    for (var i=0; i<tabs.length; i++) {
        if (tabs[i] == tab) {
            $(tabs[i]).style.display = 'block';
            $('b_'+tabs[i]).style.backgroundColor = '#fea';
            storeValue('tab',tab);
        } else {
            $(tabs[i]).style.display = 'none';
            $('b_'+tabs[i]).style.backgroundColor = '#fff';
        }
        console.log(tabs[i]);
    }
    stopHostPolling();
    stopLiveQueryPolling();
    switch (tab) {
        case 'completedqueries':
            cacheRescan();
            break;
        case 'runningqueries':
            queriesRescan();
            break;
    }
}

/* format number with comma separators */
function fcsnum(n) {
    if (n == 0) {
        return n;
    }
    var pre = n < 0 ? "-" : "";
    var x = 1000;
    var a = [];
    n = Math.abs(n);
    while (n != 0) {
        var d = n % x;
        a.push(d/(x/1000));
        n -= d;
        x *= 1000;
    }
    a = a.reverse();
    for (var i=1; i<a.length; i++) {
        a[i] = a[i].toString();
        a[i] = '000'.substring(a[i].length)+a[i];
    }
    return pre+a.join(",");
}

/* decodes state from local storage */
function storedQueriesDecode() {
    var lsq = store['queries'] || null;
    queries = [];
    if (lsq) {
        lsq = lsq.split(',');
        for (var j=0; j<lsq.length; j++) {
            var q = unesc(lsq[j]).split(':');
            if (q.length == 4) {
                queries.push({name:unesc(q[0]),query:unesc(q[1]),ops:unesc(q[2]),rops:unesc(q[3])});
            }
        }
    }
}

/* encodes state to local storage */
function storedQueriesEncode() {
    var qc = [];
    for (var i=0; i<queries.length; i++) {
        var q = queries[i];
        qc.push(esc([esc(q.name),esc(q.query),esc(q.ops),esc(q.rops),esc(q.tasks)].join(":")));
    }
    store['queries'] = qc;
}

/* sets or clears a cookie and re-encodes the lot */
function storeValue(c,v) {
    store[c] = v;
    storedQueriesEncode();
}

/* retrieves a cookie or returns a default if not set */
function fetchValue(c,dv) {
    return store[c] || dv;
}

/* create query object from input fields */
function fieldsToQuery() {
    return {
        query:$('query').value,
        ops:$('qops').value,
        rops:$('qrops').value,
        name:$('qname').value,
        tasks:$('qtasks').value
    };
}

/* populate input fields from query object */
function queryToFields(query) {
    $('query').value = query.query || '';
    $('qops').value = query.ops || '';
    $('qrops').value = query.rops || '';
    $('qname').value = query.name || '';
    $('qtasks').value = query.tasks || '';
}

/* transfer nav to query */
function navToQuery(src,exec) {
    if (src) {
        queryToFields({query:navp + ':+json'});
    } else {
        queryToFields({query:navp + '/('+maxnav+')+:+count,+nodes,+mem',rops:'gather=ksaa',ops:'gather=ksaau;sort=0:s:a;title=key,count,nodes,mem,merge'});
    }
    if (exec) {
        doFormQuery('json');
    }
    return false;
}

/* rpc callback : render raw node */
function navNodeRaw(rpc) {
    if (rpc.status == 200) {
        json=rpcDecode(rpc);
        json=eval("("+json[0][0]+")");
        console.log(json);
    } else  {
        console.log('failed rpc');
        console.log(rpc);
    }
}

/* rpc callback : render node child list */
function renderNavQuery(rpc) {
    if (rpc.status == 200) {
        var r = rpcDecode(rpc);
        var t = '<table id="table_nav"><tr><th>node</th><th>count</th><th>nodes</th><th>mem</th><th>merge</th></tr>';
        for (var i=0; i<r.length; i++) {
            var d = UTF8.decode(r[i][0]);
            var s = d.replace(/</g,'&lt;').replace(/>/g,'&gt;');
            var oc = 'QM.treeNavTo(\''+esc(d)+'\','+r[i][2]+');'
            var os = 'QM.navToQuery(true,true);return false;'
            t += '<tr><td><a href="#" onclick="'+oc+'">'+s+'</a></td><td class="num">'+fcsnum(r[i][1])+'</td><td class="num">'+fcsnum(r[i][2])+'</td><td class="num">'+fcsnum(r[i][3])+'</td><td class="num">'+fcsnum(r[i][4])+'</td></tr>';
        }
        t += '</table>';
        $('nodelist').innerHTML = t;
    } else {
        console.log('failed rpc');
        console.log(rpc);
        $('nodelist').innerHTML = "<b>server error: "+rpc.status+" : "+rpc.statusText+" : "+rpc.responseText+"</b>";
    }
}

/* get the real query url */
function queryRaw() {
    var query = fieldsToQuery();
    query.other = $('qother').value;
    var path = '/query/call?'+packQuery([query.other,['path',query.query],['ops',query.ops],['rops',query.rops],['format','json'],["job",jobid],['filename',query.name],["sender","spawn"],['tasks',query.tasks]]);
    alert(path);
    console.log(path);
    return false;
}

/* export current query as csv */
function queryCSV() {
    var query = fieldsToQuery();
    query.other = $('qother').value;
    window.open('/query/call?'+packQuery([query.other,['path',query.query],['ops',query.ops],['rops',query.rops],['format','csv'],["job",jobid],['filename',query.name],["sender","spawn"],['tasks',query.tasks]]));
    return false;
}

function queryGoogleDrive() {
    var q = fieldsToQuery();
    do {
       var filename = window.prompt("Enter filename with .csv extension: ","query.csv");
    } while(!/\.csv$/.test(filename))
    q.other = $('qother').value;
    q.name = filename
    q.format = 'gdrive';
    doQuery(q, renderFormQueryResults, true);
    return false;
}

/* save input fields as query */
function querySave() {
    queries.push(fieldsToQuery());
    storedQueriesShow();
    storedQueriesEncode();
    return false;
}

/* delete select query */
function queryDelete(i) {
    queries.splice(i,1);
    storedQueriesShow();
    storedQueriesEncode();
}

/* alter contents of select query */
function querySet(i,exec) {
    var q = queries[i];
    queryToFields(q);
    window.localStorage['lastQuery'] = packQuery([['path',q.query],['ops',q.ops],['rops',q.rops],['tasks',q.tasks],['format','json'],['filename',q.name],$('qother').value]);
    if (exec) doFormQuery('json');
    return false;
}

/* render queries into box */
function storedQueriesShow() {
    var txt = '<table id="table_queries">';
    for (var i=0; i<queries.length; i++) {
        txt += '<tr>';
        txt += '<th><a title="delete" href="#" onclick="QM.queryDelete('+i+');return false;">&times;</a></th>';
        txt += '<th><a title="query" href="#" onclick="QM.querySet('+i+',true);return false;">&raquo;</a></th>';
        txt += '<td width=95%><a title="load" href="#" onclick="return QM.querySet('+i+',false)">'+(queries[i].name || queries[i].query)+'</a></td>';
        txt += '</tr>';
    }
    txt += '</table>';
    $('saved').innerHTML = txt;
}

/* called by <return> in input field */
function submitQuery(val,event,json) {
    storeValue("qother",$('qother').value);
    // only trigger query on a return/enter keypress
    switch (window.event ? window.event.keyCode : event ? event.which : 0) {
        case 13: doFormQuery('json'); return false;
    }
}

/* called by <return> in input field */
function queryCodec(val,event,action) {
    switch (window.event ? window.event.keyCode : event ? event.which : 0) {
        case 13:
            switch (action) {
                case 'encode':
                    new Ajax.Request('/query/encode?path='+esc(val), { method: 'get', onComplete: function(rpc) {
                        $('o2q').value = rpc.responseText;
                    } });
                    break;
                case 'decode':
                    new Ajax.Request('/query/decode?path='+esc(val), { method: 'get', onComplete: function(rpc) {
                        $('q2o').value = rpc.responseText;
                    } });
                    break;
            }
            return false;
    }
}

/* sent rpc to get a list of live queries from QueryMaster */
function cacheRescan() {
    new Ajax.Request('/completed/list', { method: 'get', onComplete: function(rpc) { renderCompletedEntries(rpcDecode(rpc)); } });
}

/* sent rpc to get a list of live queries from QueryMaster */
function queriesRescan() {
    new Ajax.Request('/query/list', { method: 'get', onComplete: function(rpc) { renderLiveQueries(rpcDecode(rpc)); } });
    if($('runningstatus').style.display=="block" && $('sel_run_uuid').innerHTML!=""){
        queryHostsRescan($('sel_run_uuid').innerHTML, $('sel_run_job').innerHTML);
    }
    //setup polling for new live queries    
    if(liveQueryPolling==null){
        liveQueryPolling=setInterval(function(){                    
                    new Ajax.Request(
                        '/query/list', 
                        { 
                            method: 'get', 
                            onComplete: function(rpc) { 
                                renderLiveQueries(rpcDecode(rpc)); 
                            } 
                        });
                }, 5000);
    }
    else{
        liveQueryPolling.start();
    }
}

/* sent rpc to refresh from QueryMaster */
function monitorsRescan() {
    new Ajax.Request('/monitors.rescan', { method: 'get', onComplete: function(rpc) { alert(rpc.responseText); } });    
}

/* sent rpc to get a list of hosts for a query from QueryMaster */
function queryHostsRescan(uuid,job) {
    var tab=fetchValue('tab'); 
    var request= new Ajax.Request('/host/list', { 
        method: 'get', 
        parameters: {uuid: uuid},
        onComplete: function(rpc) { 
            renderQueryHosts(rpcDecode(rpc),tab); 
        } 
    });
    switch (tab) {
        case 'completedqueries':
            $('sel_compl_uuid').update(uuid);
            $('sel_compl_job').update(job);
            $('completedhosts').update("");            
            // $('sel_compl_progress').innerHTML ="-"; 
            break;
        case 'runningqueries':
            $('sel_run_uuid').update(uuid);
            $('sel_run_job').update(job);  
            $('runninghosts').update(""); 
            $('sel_run_progress').innerHTML ="-";         
            stopHostPolling();
            if(hostUpdater==null){
                hostUpdater=setInterval(function(){                    
                    new Ajax.Request('/host/list', { 
                        method: 'get', 
                        parameters: {uuid: $('sel_run_uuid').innerHTML},
                        onComplete: function(rpc) { 
                            renderQueryHosts(rpcDecode(rpc),'runningqueries'); 
                        } 
                    })
                }, 2000);
            }
            break;
    }    
}

function renderQueryHosts(hosts,tab){
    // console.log("unsorted:"+hosts);
    hosts = hosts.tasks;
    var html = '<table><tr><th>';
    html += ['task','lines','finished','host info'].join('</th><th>')+'</th></tr>';
    var finished=0;
    for (var i=0; i<hosts.length; i++) {
        var h = hosts[i];
        var options = h.options;
        var hostInfo = "";
        if (h.lines > 0) {
            var selected = "unknown";
            var inactive = "";
            for (var j = 0; j < options.length; j++) {
                var option = options[j];
                if (option.selected) {
                    selected = option.hostUuid;
                } else {
                    inactive += option.hostUuid + ", "
                }
            }
            hostInfo = "selected: " + selected + "   inactive: " + inactive;
        }
        if (h.lines == 0) {
            var active = "active: ";
            var inactive = "inactive: ";
            for (var j = 0; j < options.length; j++) {
                var option = options[j];
                if (option.active) {
                    active += option.hostUuid + ", ";
                } else {
                    inactive += option.hostUuid + ", ";
                }
            }
            hostInfo = active + "   " + inactive;
        }
        var row = [i, h.lines, (h.complete?"y":"n"), hostInfo];
        html += '<tr><td>'+row.join('</td><td>')+'</td></tr>';
        finished+=(h.complete?1:0);
    }
    html += '</table>';
    // var tab=fetchValue('tab');    
    switch (tab) {
        case 'completedqueries':             
            $('completedhosts').innerHTML = html;
            show('completedstatus');
            show('completedhosts');
            break;
        case 'runningqueries':            
            $('runninghosts').innerHTML = html; 
            var progress = (((finished/1.00)/hosts.length)*100.0);
            // $('sel_run_progress').innerHTML = (isNaN(progress) || (progress==0) )?"-":progress+"%";
             $('sel_run_progress').innerHTML = (hosts.length>0? finished+"/"+hosts.length:"-");
            show('runningstatus');
            show('runninghosts');
            break;
    }
    // $('completedstatus').style.display='block';
    // $('completedhosts').style.display='block';
}

function show(el){
    $(el).style.display='block';
}

function hide(el){
    $(el).style.display='none';
}

/* encode query arg array */
function packQuery(a) {
    var na = [];
    for (var i=0; i<a.length; i++) {
        if (!a[i]) {
            continue;
        }
        if (typeof(a[i]) == 'object') {
            na.push(packKV(a[i][0],a[i][1]));
        } else {
            na.push(a[i])
        }
    }
    return na.join('&');
}

/* encode query key/value pair */
function packKV(k,v) {
    return v && v != '' ? k+'='+esc(v) : '';
}

/* perform actual AJAX query */
function doQuery(query, callback, cacheBust) {
    var params = [query.other,['path',query.query],['ops',query.ops],['rops',query.rops],
        ['format',query.format],['job',jobid],['filename',query.name],
        ['sender','spawn'],['tasks',query.tasks]];
    if (cacheBust) {
        params.push(['nocache','1']);
    }
    if (query.format == 'gdrive') {
        window.open('/query/google/authorization?' + packQuery(params), 'Google Drive', 'width = 500, height = 500')
    } else {
        new Ajax.Request('/query/call', { method: 'get',  parameters: packQuery(params), onComplete: callback });
    }
    return false;
}

/* cancel running query */
function killLiveQuery(uuid) {
    new Ajax.Request('/query/cancel?uuid='+uuid, { method: 'get', onComplete: function(rpc) {
        alert(rpc.responseText); queriesRescan();
    }});
}

/* render queries live on QueryMaster */
function renderLiveQueries(live) {
    renderCacheList(live, 'queries', 'killLiveQuery');
}

/* render cache entries minus live */
function renderCompletedEntries(cache) {
    renderCacheList(cache, 'completed');
}

function limit(txt,chars) {
    return limitLines(txt, chars, 1);
}

function limitLines(txt,chars,lines) {
    if (!txt) { return txt; }
    var t = txt.toString();
    var output = "";
    while (t.length > 0 && lines > 0) {
        if (output.length > 0) {
            output += "<br>";
        }
        if (lines <= 1 && t.length > chars) {
            chars = chars - 3;
        }
        output += t.substring(0, chars);
        t = t.substring(chars);
        lines -= 1;
    }
    if (t.length > 0) {
        output += "...";
    }
    return output;
}

/* cache entry list render */
function renderCacheList(list,div,kill) {
    list.sort(function(a, b) {return b.uuid - a.uuid});
    var html = '<table><tr><th>';
    html += ['submit','uuid','state','job','path','ops','rops','run','lines','sent','kill'].join('</th><th>')+'</th></tr>';
    for (var i=0; i<list.length; i++) {
        var le = list[i];
        var expectedRows = [(le.paths[0] || "").length / 30, (le.ops[0] || "").length / 20, (le.ops[1] || "").length / 20].max();
        expectedRows = [expectedRows, 1].max();
        expectedRows = [expectedRows, 3].min();
        var row = [new Date(le.startTime).toString('HH:mm:ss'),
        "<a onclick='QM.queryHostsRescan(\""+le.uuid+"\",\""+le.job+"\")' href='#'>"+le.uuid+"</a>",
        le.state, limitLines(le.alias || le.job,12,expectedRows),
        "<a onclick='QM.queryHostsRescan(\""+le.uuid+"\",\""+le.job+"\")' href='#'>" +
        limitLines(le.paths[0],30,3) + "</a>",
        limitLines(le.ops[0],20,3), limitLines(le.ops[1],20,3),
        fcsnum(le.runTime), fcsnum(le.lines), fcsnum(le.sentLines),
        '<a href="#" onclick="QM.'+kill+'(\''+le.uuid+'\')">x</a>'];
        html += '<tr><td>'+row.join('</td><td>')+'</td></tr>';
    }
    html += '</table>';
    $(div).innerHTML = html;
}

/* do query with UI wrappings */
function doFormQuery(format) {
    $('queryinfo').innerHTML = busyimg;
    var q = fieldsToQuery();
    q.other = $('qother').value;
    q.format = format;
    doQuery(q, renderFormQueryResults, true);
    document.location.hash = '#'+esc(esc(Object.toQueryString(q)));
    return false;
}

/* handle AJAX query callback */
function renderFormQueryResults(rpc) {
    var src = '';
    if (rpc.status != 200) {
        $('queryinfo').innerHTML = "<b>server error: "+rpc.status+" : "+rpc.statusText+" : "+rpc.responseText+"</b>";
        return;
    }
    if (render != 0) {
        src = '<table id="table_results">';
        var table = rpcDecode(rpc);
        for (var i=0; i<table.length; i++) {
            var row = table[i];
            src += '<tr>';
            for (var j=0; j<row.length; j++) {
                src += renderQueryValue(row[j]);
            }
            src += '</tr>';
        }
        src += '</table>';
    } else {
        src = rpc.responseText;
    }
    $('queryinfo').innerHTML = src;
}

/* try to determine numbers and non-numbers */
function renderQueryValue(v) {
    if (v == null) v = '';
    if (typeof(v) !== 'number') {
        v = v.toString();
        if (v.match(/^[1-9][0-9]*$/) != null) {
            v = parseInt(v);
        } else {
            var str = UTF8.decode(v).replace(/</g,'&lt;').replace(/>/g,'&gt;');
            if (v.match(/{.*}/)) {
                str = prettyPrintOne(str,"js");
            }
            return '<td>'+str+'</td>';
        }
    }
    if (v % 1 !== 0) {
        return '<td class="num">'+v+'</td>';
    }
    return '<td class="num">'+fcsnum(v)+'</td>';
}

/* render nav stack */
function treeNavStack() {
    var navt = '';
    for (var i=0; i<navstack.length; i++) {
        var txt = i > 0 ? navstack[i] : '...';
        if (txt.length > 20) {
            txt = txt.substring(0,17)+"...";
        }
        navt += '<a href="..." onclick="QM.treeNavUp('+(i+1)+');return false;">'+unescape(txt)+'</a> / ';
    }
    $('treenav').innerHTML = navt;
    navp = navstack.length > 1 ? navstack.slice(1).join("/") : "";
    $('nodelist').innerHTML = busyimg;
    doQuery( {query : navp + '/('+maxnav+')+:+count,+nodes,+mem',
              ops : 'gather=ksaau;sort=0:s:a',
              other : $('qother').value,
              format : 'json'}, renderNavQuery, true);
    if (navstack.length > 0 && fetchValue('raw') == '1') doQuery({ query : navp+':+json',
        other : $('qother').value, format : 'json' }, navNodeRaw, true);
}

/* pop nav stack */
function treeNavUp(idx) {
    while (navstack.length > idx) {
        navstack.pop();
    }
    treeNavStack();
}

/* push nav stac */
function treeNavTo(node,children) {
    if (children == 0) {
        $('nodelist').innerHTML = '';
    }
    navstack.push(node);
    treeNavStack();
}

function toggleGraphOptions() {
	if ($('graph_type_buttons').innerHTML == "") {
		    $('graph_type_buttons').innerHTML = '<button onclick="QM.chooseGraph(\'line\')">line graph</button>';
	} else {
		$('graph_type_buttons').innerHTML = '';
		$('graph_config').innerHTML = '';
		$('graph_display').innerHTML = '';
	}

}

function chooseGraph(type) {
    var config = '';
    config += '<table id="graph_config" cellspacing=1 cellpadding=1 border=0 width=100%>';
    config += '<tr><td>X Columns</td><td><input id="xcols" type="text" value="0"/></td></tr>';
    config += '<tr><td>Y Columns</td><td><input id="ycols" type="text" value="1"/></td></tr>';
    config += '</table>';
    config += '<button onclick="QM.graphIt(\'' + type + '\')">graph it</button>';
    $('graph_config').innerHTML = config;
}

function renderLineGraph(rpc) {
    table = rpcDecode(rpc);
    $('graph_display').innerHTML = '<div id="graph" style="width:100%;height:300px;"></div>';

    var _$ = jQuery.noConflict();
    var y_cols = $('ycols').value.split(',');
    var x_cols = $('xcols').value.split(',');
    var tcks = [];
    var data = [];
    for (var i=0; i<table.length; i++){
        var x_keys = [];
        for (var x_col_i=0; x_col_i < x_cols.length; x_col_i++) {
            var x = table[i][parseInt(x_cols[x_col_i])];
            x_keys.push(x);
        }
        var x_val = x_keys.join(' ');
        tcks.push([i, x_val]);
        for (var y_col_i = 0; y_col_i < y_cols.length; y_col_i++) {
            var col_data = y_col_i < data.length ? data[y_col_i].data : [];
            var index = parseInt(y_cols[y_col_i]);
            var y = table[i][index];
            if (isNaN(y)){
                continue;
            }
            col_data.push([i,y])
            var y_label = isNaN(table[0][index]) ? table[0][index] : index;
            data[y_col_i] = { data: col_data, label: y_label};
        }
    }
    var num_ticks = tcks.length;
    var max_ticks = 8;
	if (num_ticks > max_ticks) {
		var reduced_ticks = [];
		for (var i=0; i<max_ticks; i++) {
			reduced_ticks.push(tcks[Math.floor(i * num_ticks / max_ticks)]);
		}
		tcks = reduced_ticks;
	}
    var options = { xaxis : {ticks:tcks}};
    _$.plot(_$("#graph"), data , options);
}

function graphIt(type) {
    var q = fieldsToQuery();
    q.other = $('qother').value;
    q.format = 'json';
    if (type == 'line') {
        doQuery(q, renderLineGraph, true);
    }
}

function closeRunningHosts(){
    hide('runninghosts');
    hide('runningstatus');
    //stop hostUpdater
    stopHostPolling();
}

function stopHostPolling(){
    if(hostUpdater!=null){
        clearInterval(hostUpdater);
        hostUpdater=null;
    }
}

function stopLiveQueryPolling(){
    if(liveQueryPolling!=null){
    //     clearInterval(liveQueryPolling);
    //     liveQueryPolling=null;
    // }
    // else{
        clearInterval(liveQueryPolling);
        liveQueryPolling=null;
    }
}

function closeCompletedHosts(){
    hide('completedhosts');
    hide('completedstatus');
}


/* called on page load  */
function init() {
    storedQueriesDecode();
    
    // Populate query fields from URL (use hash, then query string)
    $('qname').value  = hkv.name   || '';
    $('query').value  = hkv.query  || '';
    $('qops').value   = hkv.ops    || '';
    $('qrops').value  = hkv.rops   || '';
    $('qtasks').value  = hkv.tasks || '';
    $('qother').value = hkv.qother || fetchValue('qother', '');
    
    if ($('query').value) {
        doFormQuery('json');
    }
    
    treeNavStack();
    storedQueriesShow();

    showTab(fetchValue('tab','runningqueries'));
    storeValue('job',jobid);
}

window.QM = {
    init : init,
    showTab : showTab,
    monitorsRescan : monitorsRescan,
    queryCodec : queryCodec,
    queryRaw : queryRaw,
    queryGoogleDrive : queryGoogleDrive,
    queryCSV : queryCSV,
    querySave : querySave,
    querySet : querySet,
    queryDelete : queryDelete,
    submitQuery : submitQuery,
    navToQuery : navToQuery,
    treeNavUp : treeNavUp,
    treeNavTo : treeNavTo,
    toggleGraphOptions : toggleGraphOptions,
    chooseGraph : chooseGraph,
    graphIt : graphIt,
    killLiveQuery : killLiveQuery,
    queryHostsRescan: queryHostsRescan,

    show:show,
    hide:hide,
    closeRunningHosts: closeRunningHosts,
    closeCompletedHosts: closeCompletedHosts
};

})();

