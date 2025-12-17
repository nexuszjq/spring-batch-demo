package com.example.batchdemo.web;

import com.example.batchdemo.service.FileJobLauncherService;
import com.example.batchdemo.service.RandomAccessHeaderFooterService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.nio.file.Path;

/**
 * 提供 REST API，让每个大文件处理以单独请求的方式触发。
 */
@RestController
@RequestMapping("/files")
public class FileProcessingController {

    private final FileJobLauncherService jobLauncherService;
    private final RandomAccessHeaderFooterService headerFooterService;

    public FileProcessingController(FileJobLauncherService jobLauncherService,
                                    RandomAccessHeaderFooterService headerFooterService) {
        this.jobLauncherService = jobLauncherService;
        this.headerFooterService = headerFooterService;
    }

    @PostMapping("/process")
    public ResponseEntity<String> process(@RequestBody FileProcessingRequest request) throws Exception {
        if (request.getMode() == FileProcessingRequest.Mode.MMAP) {
            jobLauncherService.launchMemoryMapped(request.getInputPath(), request.getOutputPath());
        } else {
            jobLauncherService.launchSendfile(request.getInputPath(), request.getOutputPath());
        }
        return ResponseEntity.ok("已触发批处理任务，输出: " + request.getOutputPath());
    }

    @PostMapping("/header")
    public ResponseEntity<String> reserveHeader(@RequestParam("path") String path,
                                                @RequestParam("header") String header) throws Exception {
        Path file = Path.of(path);
        headerFooterService.createFileWithReservedHeader(file);
        headerFooterService.writeHeader(file, header);
        return ResponseEntity.ok("已写入头部，不需要全量读取正文");
    }

    @PostMapping("/footer")
    public ResponseEntity<String> appendFooter(@RequestParam("path") String path,
                                               @RequestParam("footer") String footer) throws Exception {
        headerFooterService.appendFooter(Path.of(path), footer);
        return ResponseEntity.ok("已追加文件尾");
    }
}
