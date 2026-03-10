package com.example.flink;

import java.io.BufferedWriter;
import java.io.Serializable;

/**
 * 事务句柄：封装两阶段提交中的临时文件和目标文件路径信息。
 *
 * <p>关键设计：
 * <ul>
 *   <li>tmpFilePath: 写入阶段使用的临时文件路径（.tmp 后缀）</li>
 *   <li>targetFilePath: 提交后的正式文件路径（.data 后缀）</li>
 *   <li>writer: 标记为 transient，不参与序列化（IO 流无法序列化）</li>
 * </ul>
 *
 * <p>故障恢复时，Flink 通过反序列化恢复文件路径，
 * 然后根据路径重新执行 commit（重命名）或 abort（删除）操作。
 */
public class FileTransaction implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 临时文件路径（写入阶段使用，.tmp 后缀） */
    private final String tmpFilePath;

    /** 目标文件路径（提交后的正式路径，.data 后缀） */
    private final String targetFilePath;

    /**
     * 写入流（transient：不参与序列化）。
     * 故障恢复时此字段为 null，需要通过文件路径重建。
     */
    private transient BufferedWriter writer;

    public FileTransaction(String tmpFilePath, String targetFilePath) {
        this.tmpFilePath = tmpFilePath;
        this.targetFilePath = targetFilePath;
    }

    public String getTmpFilePath() {
        return tmpFilePath;
    }

    public String getTargetFilePath() {
        return targetFilePath;
    }

    public BufferedWriter getWriter() {
        return writer;
    }

    public void setWriter(BufferedWriter writer) {
        this.writer = writer;
    }

    @Override
    public String toString() {
        return "FileTransaction{" +
                "tmpFilePath='" + tmpFilePath + '\'' +
                ", targetFilePath='" + targetFilePath + '\'' +
                '}';
    }
}
