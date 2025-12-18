package com.example.batchdemo.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 在程序启动时，将每个文件视为一次“请求”去触发对应的 Job，避免使用 REST。
 * 可以通过 --file.requests=MMAP:/data/in.csv:/data/out.csv 的形式传入多个请求。
 */
@Component
public class FileRequestCommandLineRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(FileRequestCommandLineRunner.class);

    @Value("${file.requests:}")
    private List<String> rawRequests = Collections.emptyList();

    private final FileJobLauncherService launcherService;

    public FileRequestCommandLineRunner(FileJobLauncherService launcherService) {
        this.launcherService = launcherService;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        List<FileJobRequest> requests = normalize(rawRequests);
        if (requests.isEmpty()) {
            log.info("未收到 file.requests，跳过批处理触发");
            return;
        }
        for (FileJobRequest request : requests) {
            log.info("启动批处理，请求模式: {}, 输入: {}, 输出: {}", request.getMode(), request.getInputPath(), request.getOutputPath());
            launcherService.launch(request);
        }
    }

    private List<FileJobRequest> normalize(List<String> specs) {
        if (specs == null || specs.isEmpty()) {
            return Collections.emptyList();
        }
        List<FileJobRequest> requests = new ArrayList<>();
        for (String spec : specs) {
            if (spec == null) {
                continue;
            }
            String trimmed = spec.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            requests.add(FileJobRequest.parse(trimmed));
        }
        return requests;
    }
}
