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
package com.addthis.hydra.data.query;

import com.addthis.hydra.data.query.op.OpChangePoints;
import com.addthis.hydra.data.query.op.OpCompare;
import com.addthis.hydra.data.query.op.OpContains;
import com.addthis.hydra.data.query.op.OpDateFormat;
import com.addthis.hydra.data.query.op.OpDePivot;
import com.addthis.hydra.data.query.op.OpDiff;
import com.addthis.hydra.data.query.op.OpDiskSort;
import com.addthis.hydra.data.query.op.OpDisorder;
import com.addthis.hydra.data.query.op.OpFill;
import com.addthis.hydra.data.query.op.OpFold;
import com.addthis.hydra.data.query.op.OpFrequencyTable;
import com.addthis.hydra.data.query.op.OpGather;
import com.addthis.hydra.data.query.op.OpHistogram;
import com.addthis.hydra.data.query.op.OpHistogramExplicit;
import com.addthis.hydra.data.query.op.OpHoltWinters;
import com.addthis.hydra.data.query.op.OpLimit;
import com.addthis.hydra.data.query.op.OpMap;
import com.addthis.hydra.data.query.op.OpMedian;
import com.addthis.hydra.data.query.op.OpMerge;
import com.addthis.hydra.data.query.op.OpNoDup;
import com.addthis.hydra.data.query.op.OpNumber;
import com.addthis.hydra.data.query.op.OpOrder;
import com.addthis.hydra.data.query.op.OpOrderMap;
import com.addthis.hydra.data.query.op.OpPercentileDistribution;
import com.addthis.hydra.data.query.op.OpPercentileRank;
import com.addthis.hydra.data.query.op.OpPivot;
import com.addthis.hydra.data.query.op.OpRMap;
import com.addthis.hydra.data.query.op.OpRandomFail;
import com.addthis.hydra.data.query.op.OpRange;
import com.addthis.hydra.data.query.op.OpRemoveSingletons;
import com.addthis.hydra.data.query.op.OpReverse;
import com.addthis.hydra.data.query.op.OpRoll;
import com.addthis.hydra.data.query.op.OpSeen;
import com.addthis.hydra.data.query.op.OpSkip;
import com.addthis.hydra.data.query.op.OpSleep;
import com.addthis.hydra.data.query.op.OpString;
import com.addthis.hydra.data.query.op.OpTitle;
import com.addthis.hydra.data.query.op.OpTranspose;

import io.netty.channel.ChannelProgressivePromise;

enum Op {

    AVG {
        @Override
        QueryOp build(QueryOpProcessor processor,
                      String args,
                      ChannelProgressivePromise opPromise) {
            return new OpRoll.AvgOpRoll(args, opPromise);
        }
    },
    CHANGEPOINTS {
        @Override
        QueryOp build(QueryOpProcessor processor,
                      String args,
                      ChannelProgressivePromise opPromise) {
            return new OpChangePoints(processor.tableFactory(), args, opPromise);
        }
    },
    COMPARE {
        @Override
        QueryOp build(QueryOpProcessor processor,
                      String args,
                      ChannelProgressivePromise opPromise) {
            return new OpCompare(args, opPromise);
        }
    },
    CONTAINS {
        @Override
        QueryOp build(QueryOpProcessor processor,
                      String args,
                      ChannelProgressivePromise opPromise) {
            return new OpContains(args, opPromise);
        }
    },
    DATEF {
        @Override
        QueryOp build(QueryOpProcessor processor,
                      String args,
                      ChannelProgressivePromise opPromise) {
            return new OpDateFormat(args, opPromise);
        }
    },
    DELTA {
        @Override
        QueryOp build(QueryOpProcessor processor,
                      String args,
                      ChannelProgressivePromise opPromise) {
            return new OpRoll.DeltaOpRoll(args, opPromise);
        }
    },
    DEPIVOT {
        @Override
        QueryOp build(QueryOpProcessor processor,
                      String args,
                      ChannelProgressivePromise opPromise) {
            return new OpDePivot(args, opPromise);
        }
    },
    DIFF {
        @Override
        QueryOp build(QueryOpProcessor processor,
                      String args,
                      ChannelProgressivePromise opPromise) {
            return new OpDiff(processor.tableFactory(), args, opPromise);
        }
    },
    DISORDER {
        @Override
        QueryOp build(QueryOpProcessor processor,
                      String args,
                      ChannelProgressivePromise opPromise) {
            return new OpDisorder(processor.tableFactory(), args, opPromise);
        }
    },
    DSORT {
        @Override
        QueryOp build(QueryOpProcessor processor,
                      String args,
                      ChannelProgressivePromise opPromise) {
            return new OpDiskSort(args, processor.tempDir(), opPromise);
        }
    },
    FILL {
        @Override
        QueryOp build(QueryOpProcessor processor,
                      String args,
                      ChannelProgressivePromise opPromise) {
            return new OpFill(args, opPromise);
        }
    },
    FOLD {
        @Override
        QueryOp build(QueryOpProcessor processor,
                      String args,
                      ChannelProgressivePromise opPromise) {
            return new OpFold(args, opPromise);
        }
    },
    FTABLE {
        @Override
        QueryOp build(QueryOpProcessor processor,
                      String args,
                      ChannelProgressivePromise opPromise) {
            return new OpFrequencyTable(processor.tableFactory(), args, opPromise);
        }
    },
    GATHER {
        @Override
        QueryOp build(QueryOpProcessor processor,
                      String args,
                      ChannelProgressivePromise opPromise) {
            return new OpGather(args, processor.memTip(), processor.rowTip(),
                                processor.tempDir().toString(), opPromise);
        }
    },
    HISTO {
        @Override
        QueryOp build(QueryOpProcessor processor,
                      String args,
                      ChannelProgressivePromise opPromise) {
            return new OpHistogram(args, opPromise);
        }
    },
    HISTO2 {
        @Override
        QueryOp build(QueryOpProcessor processor,
                      String args,
                      ChannelProgressivePromise opPromise) {
            return new OpHistogramExplicit(args, opPromise);
        }
    },
    HOLTWINTERS {
        @Override
        QueryOp build(QueryOpProcessor processor,
                      String args,
                      ChannelProgressivePromise opPromise) {
            return new OpHoltWinters(processor.tableFactory(), args, opPromise);
        }
    },
    DISTRIBUTION {
        @Override
        QueryOp build(QueryOpProcessor processor,
                      String args,
                      ChannelProgressivePromise opPromise) {
            return new OpPercentileDistribution(processor.tableFactory(), args, opPromise);
        }
    },
    LIMIT {
        @Override
        QueryOp build(QueryOpProcessor processor,
                      String args,
                      ChannelProgressivePromise opPromise) {
            return new OpLimit(args, opPromise);
        }
    },
    MAP {
        @Override
        QueryOp build(QueryOpProcessor processor,
                      String args,
                      ChannelProgressivePromise opPromise) {
            return new OpMap(args, opPromise);
        }
    },
    RMAP {
        @Override
        QueryOp build(QueryOpProcessor processor,
                      String args,
                      ChannelProgressivePromise opPromise) {
            return new OpRMap(args, opPromise);
        }
    },
    MAX {
        @Override
        QueryOp build(QueryOpProcessor processor,
                      String args,
                      ChannelProgressivePromise opPromise) {
            return new OpRoll.MaxOpRoll(args, opPromise);
        }
    },
    MEDIAN {
        @Override
        QueryOp build(QueryOpProcessor processor,
                      String args,
                      ChannelProgressivePromise opPromise) {
            return new OpMedian(processor.tableFactory(), opPromise);
        }
    },
    MERGE {
        @Override
        QueryOp build(QueryOpProcessor processor,
                      String args,
                      ChannelProgressivePromise opPromise) {
            return new OpMerge(args, opPromise);
        }
    },
    MIN {
        @Override
        QueryOp build(QueryOpProcessor processor,
                      String args,
                      ChannelProgressivePromise opPromise) {
            return new OpRoll.MinOpRoll(args, opPromise);
        }
    },
    NUM {
        @Override
        QueryOp build(QueryOpProcessor processor,
                      String args,
                      ChannelProgressivePromise opPromise) {
            return new OpNumber(args, opPromise);
        }
    },
    MATH {
        @Override
        QueryOp build(QueryOpProcessor processor,
                String args,
                ChannelProgressivePromise opPromise) {
            return NUM.build(processor, args, opPromise);
        }
    },
    NODUP {
        @Override
        QueryOp build(QueryOpProcessor processor,
                      String args,
                      ChannelProgressivePromise opPromise) {
            return new OpNoDup(opPromise);
        }
    },
    ORDER {
        @Override
        QueryOp build(QueryOpProcessor processor,
                      String args,
                      ChannelProgressivePromise opPromise) {
            return new OpOrder(args, opPromise);
        }
    },
    ORDERMAP {
        @Override
        QueryOp build(QueryOpProcessor processor,
                      String args,
                      ChannelProgressivePromise opPromise) {
            return new OpOrderMap(args, opPromise);
        }
    },
    PAD {
        @Override
        QueryOp build(QueryOpProcessor processor,
                      String args,
                      ChannelProgressivePromise opPromise) {
            return new OpFill(args, true, opPromise);
        }
    },
    PERCENTRANK {
        @Override
        QueryOp build(QueryOpProcessor processor,
                      String args,
                      ChannelProgressivePromise opPromise) {
            return new OpPercentileRank(processor.tableFactory(), args, opPromise);
        }
    },
    PIVOT {
        @Override
        QueryOp build(QueryOpProcessor processor,
                      String args,
                      ChannelProgressivePromise opPromise) {
            return new OpPivot(processor.tableFactory(), args, opPromise);
        }
    },
    RANGE {
        @Override
        QueryOp build(QueryOpProcessor processor,
                      String args,
                      ChannelProgressivePromise opPromise) {
            return new OpRange(processor.tableFactory(), args, opPromise);
        }
    },
    REVERSE {
        @Override
        QueryOp build(QueryOpProcessor processor,
                      String args,
                      ChannelProgressivePromise opPromise) {
            return new OpReverse(processor.tableFactory(), opPromise);
        }
    },
    RMSING {
        @Override
        QueryOp build(QueryOpProcessor processor,
                      String args,
                      ChannelProgressivePromise opPromise) {
            return new OpRemoveSingletons(processor.tableFactory(), args, opPromise);
        }
    },
    RNDFAIL {
        @Override
        QueryOp build(QueryOpProcessor processor,
                      String args,
                      ChannelProgressivePromise opPromise) {
            return new OpRandomFail(args, opPromise);
        }
    },
    SEEN {
        @Override
        QueryOp build(QueryOpProcessor processor,
                      String args,
                      ChannelProgressivePromise opPromise) {
            return new OpSeen(processor.tableFactory(), args, opPromise);
        }
    },
    SKIP {
        @Override
        QueryOp build(QueryOpProcessor processor,
                      String args,
                      ChannelProgressivePromise opPromise) {
            return new OpSkip(args, opPromise);
        }
    },
    SLEEP {
        @Override
        QueryOp build(QueryOpProcessor processor,
                      String args,
                      ChannelProgressivePromise opPromise) {
            return new OpSleep(args, opPromise);
        }
    },
    SORT {
        @Override
        QueryOp build(QueryOpProcessor processor,
                      String args,
                      ChannelProgressivePromise opPromise) {
            return new OpDiskSort(args, processor.tempDir(), opPromise);
        }
    },
    STR {
        @Override
        QueryOp build(QueryOpProcessor processor,
                      String args,
                      ChannelProgressivePromise opPromise) {
            return new OpString(args, opPromise);
        }
    },
    SUM {
        @Override
        QueryOp build(QueryOpProcessor processor,
                      String args,
                      ChannelProgressivePromise opPromise) {
            return new OpRoll.SumOpRoll(args, opPromise);
        }
    },
    TOP {
        @Override
        QueryOp build(QueryOpProcessor processor,
                      String args,
                      ChannelProgressivePromise opPromise) {
            return GATHER.build(processor, args, opPromise);
        }
    },
    TITLE {
        @Override
        QueryOp build(QueryOpProcessor processor,
                      String args,
                      ChannelProgressivePromise opPromise) {
            return new OpTitle(args, opPromise);
        }
    },
    TRANS {
        @Override
        QueryOp build(QueryOpProcessor processor,
                      String args,
                      ChannelProgressivePromise opPromise) {
            return new OpTranspose(processor.tableFactory(), opPromise);
        }
    },
    T {
        @Override
        QueryOp build(QueryOpProcessor processor,
                      String args,
                      ChannelProgressivePromise opPromise) {
            return TRANS.build(processor, args, opPromise);
        }
    };

    QueryOp build(QueryOpProcessor processor, String args) {
        return build(processor, args, processor.opPromise());
    }

    abstract QueryOp build(QueryOpProcessor processor,
                           String args,
                           ChannelProgressivePromise opPromise);
}
