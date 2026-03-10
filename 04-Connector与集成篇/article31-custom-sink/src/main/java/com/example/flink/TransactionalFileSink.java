package com.example.flink;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

/**
 * 基于两阶段提交（2PC）的事务文件 Sink。
 *
 * <p>核心流程：
 * <ol>
 *   <li>beginTransaction: 创建 .tmp 临时文件，打开写入流</li>
 *   <li>invoke: 将每条数据追加写入临时文件</li>
 *   <li>preCommit: flush + close，确保数据持久化到磁盘</li>
 *   <li>commit: 原子重命名 .tmp → .data，数据对外可见</li>
 *   <li>abort: 关闭流并删除临时文件，回滚事务</li>
 * </ol>
 *
 * <p>Exactly-Once 保证：
 * <ul>
 *   <li>事务句柄（文件路径）保存在 Checkpoint 中</li>
 *   <li>故障恢复后，对已预提交的事务重新执行 commit</li>
 *   <li>commit 操作幂等：临时文件不存在则跳过</li>
 * </ul>
 */
public class TransactionalFileSink
        extends TwoPhaseCommitSinkFunction<String, FileTransaction, Void> {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionalFileSink.class);

    /** 输出目录路径 */
    private final String outputDir;

    /**
     * 构造方法。
     *
     * @param outputDir 输出目录路径（如 /tmp/flink-sink-output）
     */
    public TransactionalFileSink(String outputDir) {
        // 使用 KryoSerializer 序列化 FileTransaction 事务句柄
        // VoidSerializer 用于 Context（本示例不需要全局上下文）
        super(new KryoSerializer<>(FileTransaction.class, new ExecutionConfig()),
              VoidSerializer.INSTANCE);
        this.outputDir = outputDir;
    }

    /**
     * 开启新事务：创建临时文件并打开写入流。
     * 文件名包含子任务索引和时间戳，确保唯一性。
     */
    @Override
    protected FileTransaction beginTransaction() throws Exception {
        int subtaskIdx = getRuntimeContext().getIndexOfThisSubtask();
        long timestamp = System.currentTimeMillis();

        // 生成唯一的临时文件和目标文件路径
        String tmpPath = outputDir + "/tmp-" + subtaskIdx + "-" + timestamp + ".tmp";
        String targetPath = outputDir + "/data-" + subtaskIdx + "-" + timestamp + ".data";

        // 确保输出目录存在
        Files.createDirectories(Paths.get(outputDir));

        // 创建事务句柄并打开写入流
        FileTransaction txn = new FileTransaction(tmpPath, targetPath);
        txn.setWriter(new BufferedWriter(new FileWriter(tmpPath, true)));

        LOG.info("Begin transaction: subtask={}, tmpFile={}", subtaskIdx, tmpPath);
        return txn;
    }

    /**
     * 处理每条数据：将数据追加写入当前事务的临时文件。
     */
    @Override
    protected void invoke(FileTransaction txn, String value, Context context)
            throws Exception {
        if (txn.getWriter() != null) {
            txn.getWriter().write(value);
            txn.getWriter().newLine();
        }
    }

    /**
     * 预提交：flush 并关闭写入流，确保数据已持久化到磁盘。
     * 此方法在 Checkpoint Barrier 到达时调用。
     *
     * <p>重要：preCommit 之后数据必须已经落盘，
     * 不能只留在操作系统的写缓冲区中。
     */
    @Override
    protected void preCommit(FileTransaction txn) throws Exception {
        if (txn.getWriter() != null) {
            txn.getWriter().flush();
            txn.getWriter().close();
            txn.setWriter(null);
        }
        LOG.info("Pre-commit transaction: {}", txn.getTmpFilePath());
    }

    /**
     * 正式提交：原子重命名 .tmp → .data，数据对外可见。
     *
     * <p>幂等设计：如果临时文件不存在（说明已经提交过），直接跳过。
     * 这保证了故障恢复时多次调用 commit 不会出错。
     */
    @Override
    protected void commit(FileTransaction txn) {
        try {
            Path tmpPath = Paths.get(txn.getTmpFilePath());
            Path targetPath = Paths.get(txn.getTargetFilePath());

            if (Files.exists(tmpPath)) {
                // 原子重命名：要么成功，要么失败，不会出现中间状态
                Files.move(tmpPath, targetPath, StandardCopyOption.ATOMIC_MOVE);
                LOG.info("Committed transaction: {} -> {}", tmpPath, targetPath);
            } else {
                // 幂等：临时文件不存在说明已经提交过
                LOG.info("Transaction already committed (idempotent): {}", txn.getTmpFilePath());
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to commit transaction: " + txn, e);
        }
    }

    /**
     * 回滚事务：关闭写入流并删除临时文件。
     * 在 Checkpoint 失败或作业取消时调用。
     */
    @Override
    protected void abort(FileTransaction txn) {
        try {
            // 关闭写入流（如果还未关闭）
            if (txn.getWriter() != null) {
                txn.getWriter().close();
                txn.setWriter(null);
            }
            // 删除临时文件
            Path tmpPath = Paths.get(txn.getTmpFilePath());
            if (Files.deleteIfExists(tmpPath)) {
                LOG.info("Aborted transaction, deleted: {}", tmpPath);
            }
        } catch (IOException e) {
            LOG.warn("Failed to abort transaction: {}", txn.getTmpFilePath(), e);
        }
    }
}
