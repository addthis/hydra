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
define([
    "underscore",
    "jquery",
    "datatables.net",
    "datatables.net-dt/css/jquery.dataTables.css",
    "backbone"
],
function(_, $, dt){
    // Attach datatables plugin to jquery
    dt.call($);

    $.fn.dataTableExt.sErrMode = 'throw';
    $.fn.dataTableExt.oApi.fnStandingRedraw = function(oSettings) {
        if(oSettings.oFeatures.bServerSide === false){
            oSettings.oApi._fnReDraw(oSettings);
            // iDisplayStart has been reset to zero - so lets change it back
            oSettings.oApi._fnCalculateEnd(oSettings);
        }
        // draw the 'current' page
        oSettings.oApi._fnDraw(oSettings);
    };
    $.fn.dataTableExt.oApi.fnFilterCallback = function(oSettings){
    };
    $.fn.dataTableExt.oApi.fnFilterKVString = function(oSettings,kvStr){
        //Parse filter query string to a key value object (aka "filteringCriteria" object).
        //Example: "share creator:foo map status:disabled" -> { _all:"share map", creator:"foo", status:"disabled"}
        var filterCriteria = DataTable.parseFilter(kvStr);
        //Use the _all attribute which is a string that contains all words that are not KV's (creator:andres status:disabled)
        //to filter all searchable columns
        this.fnFilterSearchableColumnsByString(filterCriteria._all);
        //Check if there's an attribute specific filter other than _all.
        var keys = _.keys(_.omit(filterCriteria,"_all"));
        if(keys.length>0){
            //If there is, then let's kv filter
            this.fnFilterColumnsByKV(filterCriteria);
        }
        else{
            //If there is none, then let's make sure the per column filters are cleared.
            this.fnClearAllColumnFilters();
        }
        //Let's find the X that is next to the search input and show it if the input has something, and hide it if it's empty
        if (this.fnSettings().aanFeatures.f) {
            var anControl = $('input', this.fnSettings().aanFeatures.f);
            if(!_.isEmpty(kvStr)){
                anControl.next().show();
            }
            else{
                anControl.next().hide();
            }
            //Let's make sure that the input field has the stringified version of the filter criteria
            anControl.val(DataTable.stringifyFilter(filterCriteria));
        }
    };
    $.fn.dataTableExt.oApi.fnClearAllColumnFilters = function(oSettings){
        //Data tables keeps the per column search state in the oSettings object. If there are no attribute-based filtering (basicaly if the
        //filteringCriteria is an object with only a _all attribute) then we have to loop through all columns and "clear" the state for each
        //column by making sure the have an empty sSearch value.
        for(var idx=0;idx<oSettings.aoPreSearchCols.length;idx++){
            $.extend( oSettings.aoPreSearchCols[ idx ], {
                "sSearch": "",
                "bRegex": false,
                "bSmart": false,
                "bCaseInsensitive": true
            });
        }
        //Since we just changed the state for each column, then we need to trigger a fnFilterComplete
        this._fnFilterComplete( oSettings.oPreviousSearch, 1 );
    };
    $.fn.dataTableExt.oApi.fnFilterSearchableColumnsByString = function(oSettings,filterString){
        //Filter first using the _all key which contains all the non KV string values,
        //this filter will filter every column tagged as searchable
        this._fnFilterComplete({
            "sSearch":filterString,
            "bRegex": false,
            "bSmart": false,
            "bCaseInsensitive": true
        }, 1 );
    };
    $.fn.dataTableExt.oApi.fnFilterColumnsByKV = function(oSettings,filterCriteria){
        //Loop through each of the defined columns (aoColumns) and see if that column's mData or attribute name is defined in
        //the filteringCriteria object. If it is, then extract the filter value for that column and do a filter for a specific
        //column, if that column attribute has no filteringCriteria value then use the filtering criteria it had before. If the
        //column had no filtering criteria value, then it will show that the previous filtering value was empty string "".
        for(var idx=0;idx<oSettings.aoColumns.length;idx++){
            //Take column object from the settings by index
            var column = oSettings.aoColumns[idx];
            //Take the previous search query object this column had. Every column by default has a search criteria object. If it has never
            //been searched, then it will be a search boject with an empty string as "sSearch"
            var columnSearch = oSettings.aoPreSearchCols[ idx ];
            //Take the filtering query string if defined. If not defined, filterVal will be undefined.
            var filterVal  = filterCriteria[column.mData];
            //Assign the filter value and if not defined then make it an empty string ""
            var value = (!_.isNull(filterVal) && !_.isUndefined(filterVal)?filterVal:"");
            //Extend the column's filtering object which will overwrite the sSearch,bRegex,bSmart,bCaseInsensitive attributes
            $.extend( columnSearch, {
                "sSearch": value,
                "bRegex": false,
                "bSmart": false,
                "bCaseInsensitive": true
            });
        }
        //Invoke this internal data table oApi function to force a (from docs: "..filter [of] the table using both the global filter and column based filtering")
        //With the previous logic, we made sure that the table's global and column based filter values were in order.
        //Since we already changed the oPreviousSearch to reflect the desired search values, we can use this as the search information object that
        //fnFilterComplete wants.
        this._fnFilterComplete( oSettings.oPreviousSearch, 1 );
    };
    $.fn.dataTableExt.oApi.fnFilterOnReturn = function (oSettings) {
        var _that = this, oApi=oSettings.oApi;
        this.each(function (i) {
            $.fn.dataTableExt.iApiIndex = i;
            var $this = this;
            if (_that.fnSettings().aanFeatures.f) {
                var anControl = $('input', _that.fnSettings().aanFeatures.f);
                anControl.unbind('keyup').bind('keypress', function (e) {
                    if (e.which == 13) {
                        $.fn.dataTableExt.iApiIndex = i;
                        //Extract the filter query string from the input field
                        var val = anControl.val();
                        _that.fnFilterKVString(val);
                    }
                });
                anControl.unbind('blur').bind('blur',function(e){
                    $.fn.dataTableExt.iApiIndex = i;
                    //Extract the filter query string from the input field
                    var val = anControl.val();
                    _that.fnFilterKVString(val);
                });
            }
            return this;
        });
        return this;
    };
    $.fn.dataTableExt.oApi.fnAdjustWidthCustom = function(o){
        var oSettings=o;
        var
            nScrollHeadInner = o.nScrollHead.getElementsByTagName('div')[0],
            nScrollHeadTable = nScrollHeadInner.getElementsByTagName('table')[0],
            nScrollBody = o.nTable.parentNode,
            i, iLen, j, jLen, anHeadToSize, anHeadSizers, anFootSizers, anFootToSize, oStyle, iVis,
            nTheadSize, nTfootSize,
            iWidth, aApplied=[], aAppliedFooter=[], iSanityWidth,
            nScrollFootInner = (o.nTFoot !== null) ? o.nScrollFoot.getElementsByTagName('div')[0] : null,
            nScrollFootTable = (o.nTFoot !== null) ? nScrollFootInner.getElementsByTagName('table')[0] : null,
            ie67 = o.oBrowser.bScrollOversize,
            zeroOut = function(nSizer) {
                oStyle = nSizer.style;
                oStyle.paddingTop = "0";
                oStyle.paddingBottom = "0";
                oStyle.borderTopWidth = "0";
                oStyle.borderBottomWidth = "0";
                oStyle.height = 0;
            };

        /*
         * 1. Re-create the table inside the scrolling div
         */

        /* Remove the old minimised thead and tfoot elements in the inner table */
        $(o.nTable).children('thead, tfoot').remove();

        /* Clone the current header and footer elements and then place it into the inner table */
        nTheadSize = $(o.nTHead).clone()[0];
        o.nTable.insertBefore( nTheadSize, o.nTable.childNodes[0] );
        anHeadToSize = o.nTHead.getElementsByTagName('tr');
        anHeadSizers = nTheadSize.getElementsByTagName('tr');

        if ( o.nTFoot !== null )
        {
            nTfootSize = $(o.nTFoot).clone()[0];
            o.nTable.insertBefore( nTfootSize, o.nTable.childNodes[1] );
            anFootToSize = o.nTFoot.getElementsByTagName('tr');
            anFootSizers = nTfootSize.getElementsByTagName('tr');
        }

        /*
         * 2. Take live measurements from the DOM - do not alter the DOM itself!
         */

        /* Remove old sizing and apply the calculated column widths
         * Get the unique column headers in the newly created (cloned) header. We want to apply the
         * calculated sizes to this header
         */
        if ( o.oScroll.sX === "" )
        {
            nScrollBody.style.width = '100%';
            nScrollHeadInner.parentNode.style.width = '100%';
        }

        var nThs = oSettings.oApi._fnGetUniqueThs( o, nTheadSize );
        for ( i=0, iLen=nThs.length ; i<iLen ; i++ )
        {
            iVis = oSettings.oApi._fnVisibleToColumnIndex( o, i );
            nThs[i].style.width = o.aoColumns[iVis].sWidth;
        }

        if ( o.nTFoot !== null )
        {
            oSettings.oApi._fnApplyToChildren( function(n) {
                n.style.width = "";
            }, anFootSizers );
        }

        // If scroll collapse is enabled, when we put the headers back into the body for sizing, we
        // will end up forcing the scrollbar to appear, making our measurements wrong for when we
        // then hide it (end of this function), so add the header height to the body scroller.
        if ( o.oScroll.bCollapse && o.oScroll.sY !== "" )
        {
            nScrollBody.style.height = (nScrollBody.offsetHeight + o.nTHead.offsetHeight)+"px";
        }

        /* Size the table as a whole */
        iSanityWidth = $(o.nTable).outerWidth();
        if ( o.oScroll.sX === "" )
        {
            /* No x scrolling */
            o.nTable.style.width = "100%";

            /* I know this is rubbish - but IE7 will make the width of the table when 100% include
             * the scrollbar - which is shouldn't. When there is a scrollbar we need to take this
             * into account.
             */
            if ( ie67 && ($('tbody', nScrollBody).height() > nScrollBody.offsetHeight ||
                $(nScrollBody).css('overflow-y') == "scroll")  )
            {
                o.nTable.style.width = oSettings.oApi._fnStringToCss( $(o.nTable).outerWidth() - o.oScroll.iBarWidth);
            }
        }
        else
        {
            if ( o.oScroll.sXInner !== "" )
            {
                /* x scroll inner has been given - use it */
                o.nTable.style.width = oSettings.oApi._fnStringToCss(o.oScroll.sXInner);
            }
            else if ( iSanityWidth == $(nScrollBody).width() &&
                $(nScrollBody).height() < $(o.nTable).height() )
            {
                /* There is y-scrolling - try to take account of the y scroll bar */
                o.nTable.style.width = oSettings.oApi._fnStringToCss( iSanityWidth-o.oScroll.iBarWidth );
                if ( $(o.nTable).outerWidth() > iSanityWidth-o.oScroll.iBarWidth )
                {
                    /* Not possible to take account of it */
                    o.nTable.style.width = oSettings.oApi._fnStringToCss( iSanityWidth );
                }
            }
            else
            {
                /* All else fails */
                o.nTable.style.width = oSettings.oApi._fnStringToCss( iSanityWidth );
            }
        }

        /* Recalculate the sanity width - now that we've applied the required width, before it was
         * a temporary variable. This is required because the column width calculation is done
         * before this table DOM is created.
         */
        iSanityWidth = $(o.nTable).outerWidth();

        /* We want the hidden header to have zero height, so remove padding and borders. Then
         * set the width based on the real headers
         */

        // Apply all styles in one pass. Invalidates layout only once because we don't read any
        // DOM properties.
        oSettings.oApi._fnApplyToChildren( zeroOut, anHeadSizers );

        // Read all widths in next pass. Forces layout only once because we do not change
        // any DOM properties.
        oSettings.oApi._fnApplyToChildren( function(nSizer) {
            aApplied.push( oSettings.oApi._fnStringToCss( $(nSizer).width() ) );
        }, anHeadSizers );

        // Apply all widths in final pass. Invalidates layout only once because we do not
        // read any DOM properties.
        oSettings.oApi._fnApplyToChildren( function(nToSize, i) {
            nToSize.style.width = aApplied[i];
        }, anHeadToSize );

        $(anHeadSizers).height(0);

        /* Same again with the footer if we have one */
        if ( o.nTFoot !== null )
        {
            oSettings.oApi._fnApplyToChildren( zeroOut, anFootSizers );

            oSettings.oApi._fnApplyToChildren( function(nSizer) {
                aAppliedFooter.push( oSettings.oApi._fnStringToCss( $(nSizer).width() ) );
            }, anFootSizers );

            oSettings.oApi._fnApplyToChildren( function(nToSize, i) {
                nToSize.style.width = aAppliedFooter[i];
            }, anFootToSize );

            $(anFootSizers).height(0);
        }

        /*
         * 3. Apply the measurements
         */

        /* "Hide" the header and footer that we used for the sizing. We want to also fix their width
         * to what they currently are
         */
        oSettings.oApi._fnApplyToChildren( function(nSizer, i) {
            nSizer.innerHTML = "";
            nSizer.style.width = aApplied[i];
        }, anHeadSizers );

        if ( o.nTFoot !== null )
        {
            oSettings.oApi._fnApplyToChildren( function(nSizer, i) {
                nSizer.innerHTML = "";
                nSizer.style.width = aAppliedFooter[i];
            }, anFootSizers );
        }

        /* Sanity check that the table is of a sensible width. If not then we are going to get
         * misalignment - try to prevent this by not allowing the table to shrink below its min width
         */
        if ( $(o.nTable).outerWidth() < iSanityWidth )
        {
            /* The min width depends upon if we have a vertical scrollbar visible or not */
            var iCorrection = ((nScrollBody.scrollHeight > nScrollBody.offsetHeight ||
                $(nScrollBody).css('overflow-y') == "scroll")) ?
                iSanityWidth+o.oScroll.iBarWidth : iSanityWidth;

            /* IE6/7 are a law unto themselves... */
            if ( ie67 && (nScrollBody.scrollHeight >
                nScrollBody.offsetHeight || $(nScrollBody).css('overflow-y') == "scroll")  )
            {
                o.nTable.style.width = oSettings.oApi._fnStringToCss( iCorrection-o.oScroll.iBarWidth );
            }

            /* Apply the calculated minimum width to the table wrappers */
            nScrollBody.style.width = oSettings.oApi._fnStringToCss( iCorrection );
            o.nScrollHead.style.width = oSettings.oApi._fnStringToCss( iCorrection );

            if ( o.nTFoot !== null )
            {
                o.nScrollFoot.style.width = oSettings.oApi._fnStringToCss( iCorrection );
            }

            /* And give the user a warning that we've stopped the table getting too small */
            if ( o.oScroll.sX === "" )
            {
                oSettings.oApi._fnLog( o, 1, "The table cannot fit into the current element which will cause column"+
                    " misalignment. The table has been drawn at its minimum possible width." );
            }
            else if ( o.oScroll.sXInner !== "" )
            {
                oSettings.oApi._fnLog( o, 1, "The table cannot fit into the current element which will cause column"+
                    " misalignment. Increase the sScrollXInner value or remove it to allow automatic"+
                    " calculation" );
            }
        }
        else
        {
            nScrollBody.style.width = oSettings.oApi._fnStringToCss( '100%' );
            o.nScrollHead.style.width = oSettings.oApi._fnStringToCss( '100%' );

            if ( o.nTFoot !== null )
            {
                o.nScrollFoot.style.width = oSettings.oApi._fnStringToCss( '100%' );
            }
        }


        /*
         * 4. Clean up
         */
        if ( o.oScroll.sY === "" )
        {
            /* IE7< puts a vertical scrollbar in place (when it shouldn't be) due to subtracting
             * the scrollbar height from the visible display, rather than adding it on. We need to
             * set the height in order to sort this. Don't want to do it in any other browsers.
             */
            if ( ie67 )
            {
                nScrollBody.style.height = oSettings.oApi._fnStringToCss( o.nTable.offsetHeight+o.oScroll.iBarWidth );
            }
        }

        if ( o.oScroll.sY !== "" && o.oScroll.bCollapse )
        {
            nScrollBody.style.height = oSettings.oApi._fnStringToCss( o.oScroll.sY );

            var iExtra = (o.oScroll.sX !== "" && o.nTable.offsetWidth > nScrollBody.offsetWidth) ?
                o.oScroll.iBarWidth : 0;
            if ( o.nTable.offsetHeight < nScrollBody.offsetHeight )
            {
                nScrollBody.style.height = oSettings.oApi._fnStringToCss( o.nTable.offsetHeight+iExtra );
            }
        }

        /* Finally set the width's of the header and footer tables */
        var iOuterWidth = $(o.nTable).outerWidth();
        nScrollHeadTable.style.width = oSettings.oApi._fnStringToCss( iOuterWidth );
        nScrollHeadInner.style.width = oSettings.oApi._fnStringToCss( iOuterWidth );

        // Figure out if there are scrollbar present - if so then we need a the header and footer to
        // provide a bit more space to allow "overflow" scrolling (i.e. past the scrollbar)
        var bScrolling = $(o.nTable).height() > nScrollBody.clientHeight || $(nScrollBody).css('overflow-y') == "scroll";
        nScrollHeadInner.style.paddingRight = bScrolling ? o.oScroll.iBarWidth+"px" : "0px";

        if ( o.nTFoot !== null )
        {
            nScrollFootTable.style.width = oSettings.oApi._fnStringToCss( iOuterWidth );
            nScrollFootInner.style.width = oSettings.oApi._fnStringToCss( iOuterWidth );
            nScrollFootInner.style.paddingRight = bScrolling ? o.oScroll.iBarWidth+"px" : "0px";
        }
        $(anHeadToSize).children("th").height("0px");
    };
    $.fn.dataTableExt.oApi.fnAdjustWidth = function (oSettings){
        var o=oSettings;
        var
            nScrollHeadInner = o.nScrollHead.getElementsByTagName('div')[0],
            nScrollHeadTable = nScrollHeadInner.getElementsByTagName('table')[0],
            nScrollBody = o.nTable.parentNode,
            i, iLen, j, jLen, anHeadToSize, anHeadSizers, anFootSizers, anFootToSize, oStyle, iVis,
            nTheadSize, nTfootSize,
            iWidth, aApplied=[], aAppliedFooter=[], iSanityWidth,
            nScrollFootInner = (o.nTFoot !== null) ? o.nScrollFoot.getElementsByTagName('div')[0] : null,
            nScrollFootTable = (o.nTFoot !== null) ? nScrollFootInner.getElementsByTagName('table')[0] : null,
            ie67 = o.oBrowser.bScrollOversize,
            zeroOut = function(nSizer) {
                oStyle = nSizer.style;
                oStyle.paddingTop = "0";
                oStyle.paddingBottom = "0";
                oStyle.borderTopWidth = "0";
                oStyle.borderBottomWidth = "0";
                oStyle.height = 0;
            };
        var table = $(o.nTable), invisibleCount=0;
        for ( var iColumn=0, iColumns=o.aoColumns.length ; iColumn<iColumns ; iColumn++ ){
            var oCol= o.aoColumns[iColumn];
            if(oCol.bVisible){
                table.find("tr td:nth-child("+(iColumn-invisibleCount+1)+")").width(oCol.sWidth);
            }
            else{
                invisibleCount++;
            }
        }
    };
    $.fn.dataTableExt.oApi.fnDataUpdate = function (oSettings, dataRow) {
        try {
            //console.log(nRowObject);
            //var nRow = oSettings.aoData[iRowIndex].nTr;
            //var dataRow = oSettings.aoData[iRowIndex]._aData;
            $('td').each(function(i) {
                dataRow[i] = $(this).html();
            });

            this.oApi._fnDraw(oSettings, true);
        }
        catch(e) {
            console.log("[fnDataUpdate] threw exception: " + e);
        }
    };
    $.fn.dataTableExt.oApi.fnSetFilteringDelay = function ( oSettings, iDelay ) {
        var _that = this;

        if ( iDelay === undefined ) {
            iDelay = 300;
        }

        this.each( function ( i ) {
            $.fn.dataTableExt.iApiIndex = i;
            var
                $this = this,
                oTimerId = null,
                sPreviousSearch = null,
                anControl = $( 'input', _that.fnSettings().aanFeatures.f );

            if (_that.fnSettings().aanFeatures.f) {
                anControl.unbind( 'keyup' ).bind( 'keyup', function() {
                    var $$this = $this;

                    if (sPreviousSearch === null || sPreviousSearch != anControl.val()) {
                        window.clearTimeout(oTimerId);
                        sPreviousSearch = anControl.val();
                        oTimerId = window.setTimeout(function() {
                            $.fn.dataTableExt.iApiIndex = i;
                            _that.fnFilter( anControl.val() );
                            _that.trigger("filtered",[]);
                        }, iDelay);
                    }
                });
            }
            return this;
        } );
        return this;
    };
    var DataTable = {};
    DataTable.parseFilter = function(str){
        if(_.isEmpty(str)){
            return {_all:""};
        }
        else{
            var toks = str.split(" ");
            var byCol = {};
            var all =[];
            _.each(toks,function(tok){
                if(_.contains(tok,":")){
                    var tuple = tok.split(":");
                    var key = _.first(tuple);
                    var values = _.rest(tuple);
                    var value = values.join("");
                    byCol[key]=value;
                }else{
                    all.push(tok);
                }
            });
            byCol._all= $.trim(all.join(" "));
            return byCol;
        }
    };
    DataTable.findColumnIndices=function(columns,attributes){
        var map={};
        _.each(columns,function(column,index){
            if(!_.isNumber(column.mData) && !_.isEmpty(column.mData) && _.contains(attributes,column.mData)){
                map[column.mData]=index;
            }
        });
        return map;
    };
    DataTable.stringifyFilter = function(criteria){
        var keys = _.keys(_.omit(criteria,"_all"));
        var value = criteria._all;
        _.each(keys,function(key){
            value+=" "+key+":"+criteria[key];
        });
        return $.trim(value);
    };
    DataTable.View = Backbone.View.extend({
        tagName:"table",
        className:"table table-bordered table-collapsed table-condensed",
        attributes:{
            cellPadding:0,
            cellSpacing:0,
            border:0
        },
        events:{
            "click tr":"handleSelect",
            "filtered":"handleFiltered"
        },
        handleFiltered:function(){
            var rows = this.views.table._('tr.row_selected', {"filter":"applied"});
            this.selectedIds={};
            var self=this;
            _.each(rows,function(row){
                self.selectedIds[row[self.idAttribute]]=true;
            });
            this.saveSelectedState();
        },
        initialize:function(options){
            _.bindAll(this,'cacheColumnIndices','handleFiltered','handleCheckboxButtonClick','handleCheckboxPartialClick','handleCheckboxNoneClick','handleCheckboxAllClick','checkDirty','resort','handleScroll','resize','handleReset','handleRemove','handleAdd','handleChange','handleFilter','resetAllFilters');
            options = options || {};
            this.views={};
            this.height=0;
            this.width=0;
            this.initialized=false;
            this.collapsible = (_.isUndefined(options.collapsible)?false:true);
            this.heightBuffer=options.heightBuffer || 89;
            this.columns = options.columns || this.columns ||  [];
            this.filterTemplate = options.filterTemplate || "";
            this.selectableTemplate = options.selectableTemplate || "";
            this.columnFilterIndex = localStorage[this.id+"_filterIndex"];
            this.columnIndexMap={};
            this.emptyMessage = (_.has(options,"emptyMessage")?options.emptyMessage:"No data available");
            this.enableSearch = (_.has(options,"enableSearch")?options.enableSearch:true);
            this.id=options.id || "";
            this.dirty=true;
            this.drawCallback=options.drawCallback;
            this.dirtyInterval = setInterval(this.checkDirty,2000);
            this.scrollTop=0;
            this.changeAttrs=options.changeAttrs || [];
            this.listenTo(this.collection,"reset",this.handleReset);
            this.listenTo(this.collection,"remove",this.handleRemove);
            this.listenTo(this.collection,"add",this.handleAdd);
            this.listenTo(this.collection,"change",this.handleChange);
            this.rowCount=0;
            this.rowById={};
            this.sortCols=[];
            this.idAttribute=options.idAttribute || "DT_RowId";
            this.loadSelectedState();
            this.cacheColumnIndices();
        },
        cacheColumnIndices:function(){
            var map={};
            _.each(this.columns,function(column,index){
                if(!_.isNumber(column.mData) && !_.isEmpty(column.mData)){
                    map[column.mData]=index;
                }
            });
            this.columnIndexMap = map;
        },
        checkDirty:function(){
            var oSettings=(!_.isUndefined(this.views.table)?this.views.table.fnSettings():{});
            if(this.dirty && !_.isEmpty(oSettings) && !oSettings.bDrawing){
                oSettings.iInitDisplayStart=this.scrollTop;
                this.resort();
                this.dirty=false;
            }
        },
        handleScroll:function(event){
            var body = this.views.body;
            this.scrollTop=body.scrollTop();
        },
        render:function(){
            var height = this.$el.parent().height()-this.heightBuffer;
            var self=this;
            this.views.parent=this.$el.parent();
            if(!this.initialized){
                var prefix = "";
                var id = this.id;
                this.views.table =this.$el.dataTable({
                    "aaData":this.collection.toJSON(),
                    "aoColumns": this.columns,
                    "aaSorting":[],
                    //"bScrollInfinite":false,
                    //"bAutoWidth": true,
                    //"sScrollX": "100%",
                    //"sScrollXInner": "100%",
                    "oSearch": {"bRegex":false,"bSmart": false},
                    "bPaginate":false,
                    "sScrollY":height+"px",
                    "bProcessing":false,
                    "bStateSave": true,
                    "bServerSide":false,
                    "bCaseInsensitive":true,
                    "bDeferRender": true,
                    //"bScrollCollapse":self.collapsible,
                    "sInfoEmpty":this.emptyMessage,
                    //"bScrollAutoCss": true,
                    "oLanguage":{
                        "sEmptyTable":self.emptyMessage,
                        "sSearch":""
                    },
                    "sCookiePrefix":prefix,
                    "iCookieDuration":7*60*60*24,//1 week
                    "fnStateSave": function (oSettings, oData) {
                        var state=JSON.stringify(oData);
                        state.columnFilterIndex=self.columnFilterIndex;
                        localStorage[id]=state;
                        self.sortCols=oData.aaSorting;
                        localStorage[id+"_filterIndex"]=self.columnFilterIndex;
                        return true;
                    },
                    "fnStateLoad":function(oSettings){
                        var state = localStorage[id];
                        state=(_.isUndefined(state)?null:state);
                        state=JSON.parse(state);
                        if(!_.isNull(oSettings.oLoadedState)){
                            state= _.extend(oSettings.oLoadedState,state);
                            self.sortCols=state.aaSorting;
                        }
                        var filterIndex = localStorage[id+"_filterIndex"];
                        if(!_.isUndefined(filterIndex)){
                            self.columnFilterIndex=parseInt(filterIndex,10);
                        }
                        return state;
                    },
                    "fnStateLoadParams":function (oSettings, oData) {
                        return true;
                    },
                    "fnCookieCallback":function(sName, oData, sExpires, sPath){
                        return sName + "="+JSON.stringify(oData)+"; expires=" + sExpires +"; path=" + sPath+id;
                    },
                    "fnDrawCallback" : function(oSettings) {
                        if(!_.isUndefined(self.drawCallback)){
                            self.drawCallback(oSettings);
                        }
                        self.dirty=false;
                    },
                    "fnPreDrawCallback":function(){
                        return true;
                    },
                    "fnInitComplete": function(oSettings, json){
                        var anControl = $("div#"+id+"_filter label input[type='text']");
                        var clearButton = $("<span class='icon_clear'>X</span>");
                        clearButton.bind('click',function(){
                            $(this).hide();
                            self.resetAllFilters();
                            self.views.table.fnFilter("");
                        });
                        if(_.isEmpty(anControl.val())){
                            clearButton.hide();
                        }
                        else{
                            clearButton.show();
                        }
                        clearButton.insertAfter(anControl);

                        var search = oSettings.oPreviousSearch.sSearch;
                        var columns = self.columns;
                        _.each(columns,function(column,idx){
                            var colSearch = oSettings.aoPreSearchCols[idx];
                            if(!_.isNumber(colSearch.sSearch) && !_.isEmpty(colSearch.sSearch)){
                                search+=" "+column.mData+":"+colSearch.sSearch;
                            }
                        });
                        anControl.val($.trim(search));

                    },
                    "fnCreatedRow":function(nRow, aData, iDataIndex){
                        self.rowCount++;
                        var $row=$(nRow),rowId=aData[self.idAttribute];
                        self.rowById[rowId]=nRow;
                        if(!_.isUndefined(self.selectedIds[rowId])){
                            $row.addClass("row_selected");
                            $row.find("td input.row_selectable").prop("checked",true);
                        }
                        $row.attr("id",rowId);
                    },
                    bFilter:this.enableSearch,
                    "sDom": '<"dataTables_header"<"selectable_action">'+(this.enableSearch?'f':'')+'<"filter_links">>lrt<"dataTables_footer"i<"summary_info">>'
                });
                //filter template
                this.views.selectable = $(this.selectableTemplate);
                this.views.filter = $(this.filterTemplate);
                this.views.parent.find("div.selectable_action").append(this.views.selectable);
                this.views.parent.find("div.filter_links").append(this.views.filter);
                this.views.parent.find("div.dataTables_filter input").attr("placeholder","Search");
                this.initialized=true;
                $(window).resize(this.resize);
                this.views.selectable.bind('click button.checkbox-button',this.handleCheckboxButtonClick);
                this.views.filter.bind('click li a',this.handleFilter);
                $(this.views.table.fnSettings().oInstance).on('filter',this.handleFiltered);
                this.views.body = this.$el.closest('div.dataTables_scrollBody');
                this.$el=$(this.views.table.fnSettings().nTableWrapper);
                this.adjustWidth();
                this.saveSelectedState();
                this.views.table.fnFilterOnReturn();
            }
            else{
                this.resize();
                this.adjustWidth();
                this.redrawTable();
            }
            return this;
        },
        checkFilterState:function(settings){
        },
        resetAllFilters:function(settings){
            var oSettings;
            if(_.isUndefined(settings)){
                oSettings = this.views.table.fnSettings();
            }else{
                oSettings=settings;
            }
            for(iCol = 0; iCol < oSettings.aoPreSearchCols.length; iCol++) {
                oSettings.aoPreSearchCols[ iCol ].sSearch = '';
            }
            this.columnFilterIndex=-1;
            // fnDraw?
        },
        adjustColumnSizing:function(){
            try{
                this.views.table.fnAdjustColumnSizing();
            }
            catch(err){
                console.log("Error resizing columns.");
            }
        },
        adjustWidth:function(){
            try{
                this.views.table.fnAdjustWidthCustom();
            }
            catch(err){
                console.log("Error adjusting width.");
            }
        },
        resize:function(event){
            app.log("Resizing table: "+this.id);
            var parent = this.views.parent;
            var height = parent.height() - this.heightBuffer;
            var width = parent.width();
            if(!_.isEqual(this.height,height)){
                var table = this.views.table;
                var settings = table.fnSettings();
                if(!_.isNull(settings)){
                    settings.oScroll.sY = height + "px";
                    this.views.parent.find('div.dataTables_scrollBody').height(height+'px');
                    this.redrawTable();
                    this.height=height;
                }
            }
            if(!_.isEqual(width,this.width)){
                this.adjustColumnSizing();
                this.adjustWidth();
                this.width=width;
            }
        },
        redrawTable:function(){
            this.views.table.fnDraw(true);
        },
        resort:function(){
            if(this.sortCols.length>0){
                this.views.table.fnSort(this.sortCols);
            }
        },
        handleReset:function(){
            this.dirty=true;
            this.views.table.fnClearTable();
            this.views.table.fnAddData(this.collection.toJSON());
            this.redrawTable();
            this.saveSelectedState();
        },
        handleSelect:function(event){
            var currentTarget = $(event.currentTarget);
            var target = $(event.target);
            if(!target.is("a") && !target.is("button")){
                var row = currentTarget;
                if(row.hasClass("row_selected")){
                    delete this.selectedIds[row.attr("id")];
                    row.removeClass("row_selected");
                    row.find("td input.row_selectable").prop("checked",false);
                }
                else{
                    row.addClass("row_selected");
                    this.selectedIds[row.attr("id")]=true;
                    row.find("td input.row_selectable").prop("checked",true);
                }
                this.saveSelectedState();
            }
        },
        handleRemove:function(model){
            var tr = $(this.rowById[model.id]);
            if(tr.length===1){
                delete this.selectedIds[tr.attr("id")];
                delete this.rowById[model.id];
                var index = this.views.table.fnGetPosition(tr.get(0));
                this.dirty=true;
                this.views.table.fnDeleteRow(index,null,false);
                this.redrawTable();
            }
            this.saveSelectedState();
        },
        handleAdd:function(model){
            this.dirty=true;
            this.views.table.fnAddData(model.toJSON(),true);
        },
        handleChange:function(model){
            var needUpdate=true;
            if(!_.isEmpty(this.changeAttrs)){
                for(var i=0;i<this.changeAttrs.length;i++){
                    var attr=this.changeAttrs[i];
                    if(_.has(model.changed,attr)){
                        needUpdate=true;
                        break;
                    }
                    else{
                        needUpdate=false;
                    }
                }
            }
            if(needUpdate){
                var tr;
                if(!_.isUndefined(this.rowById[model.id])){
                    tr = $(this.rowById[model.id]);
                }
                else{
                    tr = this.views.table.$("#"+model.id);
                }
                if(tr.length===1){
                    this.dirty=true;
                    try{
                        this.views.table.fnUpdate(model.toJSON(),tr.get(0),undefined,false,false);
                    }catch(er){
                        console.log("Couldn't find data for row: "+model.id);
                        var tr2 = this.views.table.$("#"+model.id);
                        if(tr2.length===1){
                            this.views.table.fnUpdate(model.toJSON(),tr2.get(0),undefined,false,false);
                        }
                    }
                }
            }
        },
        handleFilter:function(event){
            var link = $(event.target);
            var oSettings = this.views.table.fnSettings();
            if (oSettings.aanFeatures.f) {
                var anControl = $( 'input', oSettings.aanFeatures.f );
                var search = anControl.val();
                var index = parseInt(link.data("index"),10);
                var value = link.data("value");
                var col = this.columns[index];
                if(!_.isNull(col) && !_.isUndefined(col)){
                    var key = this.columns[index].mData;
                    if(!_.isNumber(key) && !_.isEmpty(key)){
                        search+=" "+key+":"+value;
                    }
                }
                anControl.val($.trim(search));
                anControl.trigger("blur");
                event.preventDefault();
                event.stopImmediatePropagation();
            }
        },
        remove:function(){
            this.$el.detach();
        },
        getSelectedIds:function(){
            var ids = _.keys(this.selectedIds);
            return ids;
        },
        saveSelectedState:function(){
            localStorage[this.id+"_selected"]=JSON.stringify(this.selectedIds);
            var keys= _.keys(this.selectedIds), checkbox=this.views.selectable.find("span.checkbox");
            this.views.parent.find("#selectedCount").html("["+keys.length+"]");
            if(_.isEqual(keys.length,this.collection.length)){
                checkbox.attr("class","checkbox checked-all");
                checkbox.parent().data("state","all");
                this.views.selectable.find("button.btn-hide-zero").removeClass("disabled");
            }
            else if(keys.length>0){
                checkbox.attr("class","checkbox checked-partial");
                checkbox.parent().data("state","partial");
                this.views.selectable.find("button.btn-hide-zero").removeClass("disabled");
            }
            else{
                checkbox.attr("class","checkbox");
                checkbox.parent().data("state","none");
                this.views.selectable.find("button.btn-hide-zero").addClass("disabled");
            }
        },
        loadSelectedState:function(){
            if(!_.isUndefined(localStorage[this.id+"_selected"])){
                this.selectedIds=JSON.parse(localStorage[this.id+"_selected"]);
            }
            else{
                this.selectedIds={};
            }
        },
        handleCheckboxButtonClick:function(event){
            if(!$(event.currentTarget).is("button.checkbox-button")){
                return;
            }
            var button = this.views.parent.find("button.checkbox-button");
            if(_.isEqual(button.data("state"),"partial")){
                this.handleCheckboxPartialClick(event);
            }else if(_.isEqual(button.data("state"),"none")){
                this.handleCheckboxNoneClick(event);
            }else if(_.isEqual(button.data("state"),"all")){
                this.handleCheckboxAllClick(event);
            }
        },
        handleCheckboxPartialClick:function(event){
            this.selectedIds={};
            var rows = this.views.table.$("tr.row_selected");
            rows.removeClass("row_selected");
            rows.find("td input.row_selectable").prop("checked",false);
            this.saveSelectedState();
        },
        handleCheckboxNoneClick:function(event){
            var trs = this.views.table.$("tbody tr");
            this.selectedIds={};
            var self=this;
            _.each(trs,function(tr){
                var row=$(tr);
                row.addClass("row_selected");
                row.find("td input.row_selectable").prop("checked",true);
                self.selectedIds[row.attr("id")]=true;
            });
            this.saveSelectedState();
        },
        handleCheckboxAllClick:function(event){
            this.selectedIds={};
            var rows = this.views.table.$("tr.row_selected");
            rows.removeClass("row_selected");
            rows.find("td input.row_selectable").prop("checked",false);
            this.saveSelectedState();
        }
    });
    return DataTable;
});

