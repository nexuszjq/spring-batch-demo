package com.example.batchdemo.service;

import java.util.Locale;
import java.util.Objects;

/**
 * 将每个文件路径/模式封装成一个批处理请求对象，等同于“一个文件一个请求”的语义。
 * 接收的字符串格式：mode:input:output，例如 MMAP:/data/in.csv:/data/out.csv。
 */
public class FileJobRequest {

    public enum Mode {
        MMAP, SENDFILE
    }

    private final String inputPath;
    private final String outputPath;
    private final Mode mode;

    public FileJobRequest(String inputPath, String outputPath, Mode mode) {
        this.inputPath = Objects.requireNonNull(inputPath, "inputPath 不能为空");
        this.outputPath = Objects.requireNonNull(outputPath, "outputPath 不能为空");
        this.mode = Objects.requireNonNull(mode, "mode 不能为空");
    }

    public String getInputPath() {
        return inputPath;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public Mode getMode() {
        return mode;
    }

    /**
     * 解析形如 mode:input:output 的描述串。
     */
    public static FileJobRequest parse(String spec) {
        if (spec == null || spec.isBlank()) {
            throw new IllegalArgumentException("请求串不能为空");
        }
        String[] parts = spec.split(":", 3);
        if (parts.length != 3) {
            throw new IllegalArgumentException("格式需为 mode:input:output，例如 MMAP:/data/in.csv:/data/out.csv");
        }
        Mode mode = Mode.valueOf(parts[0].trim().toUpperCase(Locale.ROOT));
        return new FileJobRequest(parts[1].trim(), parts[2].trim(), mode);
    }
}
