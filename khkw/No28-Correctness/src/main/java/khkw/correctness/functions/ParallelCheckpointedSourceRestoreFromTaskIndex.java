package khkw.correctness.functions;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * 项目名称: Apache Flink 知其然，知其所以然 - khkw.correctness.functions
 * <p>
 * 作者： 孙金城
 * 日期： 2020/7/13
 */
public class ParallelCheckpointedSourceRestoreFromTaskIndex
        extends RichParallelSourceFunction<Tuple3<String, Long, Long>>
        implements CheckpointedFunction {
    private static final long serialVersionUID = 1L;
    protected static final Logger LOG =
            LoggerFactory.getLogger(ParallelCheckpointedSourceRestoreFromTaskIndex.class);

    // 标示数据源一直在取数据
    private volatile boolean running = true;

    // 当前任务实例的编号
    private transient int indexOfThisTask;

    // 数据源的消费offset
    private transient long offset;

    // offsetState
    private transient ListState<Map<Integer, Long>> offsetState;

    // offsetState 名字
    private static final String OFFSETS_STATE_NAME = "offset-states";

    @Override
    public void run(SourceContext<Tuple3<String, Long, Long>> ctx) throws Exception {
        while (running) {
            ctx.collect(new Tuple3<>("key", ++offset, System.currentTimeMillis()));
            Thread.sleep(100 * (indexOfThisTask + 1));
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
        if (!running) {
            LOG.error("snapshotState() called on closed source");
        } else {

            //获取历史状态值
            Map<Integer, Long> state = new HashMap<>();
            state.put(indexOfThisTask, offset);
            // 清除上次的state
            offsetState.clear();
            // 持久化最新的offset
            offsetState.add(state);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext ctx) throws Exception {
        indexOfThisTask = getRuntimeContext().getIndexOfThisSubtask();

        offsetState = ctx
                .getOperatorStateStore()
                .getUnionListState(new ListStateDescriptor<>(OFFSETS_STATE_NAME, new MapTypeInfo(Types.INT, Types.LONG)));
        //getUnionListState 通过广播的形式发送状态，即每个task都可以拿到全部的在维护的状态
        //这里的实现方式是维护了Map类型的ListState，每个task可以获取到全部状态，遍历map，根据indexOfTask遍历获取对应的状态
        //适合于failover后，需要task继续从上次消费的partition和上次消费的offset继续消费的应用场景。
        //思考了下，这种应用场景使用于某种特殊侧partition策略，例如id对10取余，分成10个partition。第0个task绑定到第0个partition。
        //如果其他的应用场景，task所在计算节点发生变化，并不能保证之前的task继续消费之前的状态

        int size = 0;
        for (Map<Integer, Long> allOffset : offsetState.get()) {
            size ++;
            if (allOffset.containsKey(indexOfThisTask)) {
                offset = allOffset.get(indexOfThisTask);
                // 跳过10和20的循环失败
                if (offset == 9 || offset == 19) {
                    offset += 1;
                }
                // user error, just for test
                LOG.error(String.format("Current Task index[%d], Restore from offset [%d]", indexOfThisTask, offset));
            }
        }
        System.err.println(String.format("offsetState Size = [%d]",size));
    }

    @Override
    public void cancel() {
    }
}
