package com.example.RAG_chatbot.ingest;

import com.example.RAG_chatbot.ingest.gitlab.GitLabClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.springframework.ai.document.Document;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

@Profile("gitlab-ingest")
@Component
public class GitLabIngestRunner implements CommandLineRunner {

    private final VectorStore store;
    private final GitLabClient gitlab;
    private final TextNormalizer norm;
    private final ObjectMapper objectMapper;

    private final List<String> include;
    private final List<String> exclude;
    private final long maxBytesPerFile;
    private final String projectPath;
    private final String branch;
    private final String host;
    private final String onlyPrefix;

    public GitLabIngestRunner(VectorStore store,
                              GitLabClient gitlab,
                              TextNormalizer norm,
                              Environment env) {
        this.store = store;
        this.gitlab = gitlab;
        this.norm = norm;

        this.objectMapper = new ObjectMapper();
        this.objectMapper.enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);

        this.include = splitList(env.getProperty("app.ingest.gitlab.include"),
                "**/*.md,**/*.txt,**/*.html,**/*.pdf");
        this.exclude = splitList(env.getProperty("app.ingest.gitlab.exclude"),
                ".git/**,**/node_modules/**");
        this.maxBytesPerFile = Long.parseLong(env.getProperty("app.ingest.gitlab.maxBytesPerFile","2000000"));
        this.projectPath = env.getProperty("app.ingest.gitlab.projectPath");
        this.branch = env.getProperty("app.ingest.gitlab.branch","main");
        this.host = env.getProperty("app.ingest.gitlab.host","https://gitlab.com");
        this.onlyPrefix = env.getProperty("app.ingest.gitlab.onlyPathPrefix","").trim();
    }

    @Override
    public void run(String... args) {
        System.out.printf("[Ingest] host=%s path=%s branch=%s prefix=%s include=%s exclude=%s%n",
                host, projectPath, branch, onlyPrefix,
                String.join(",", include), String.join(",", exclude));

        try {
            // Çıktı klasörlerini hazırla
            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
            String baseOutputDir = "chunks_output";
            String sessionDir = String.format("%s/session_%s", baseOutputDir, timestamp);
            Path sessionPath = Paths.get(sessionDir);
            Files.createDirectories(sessionPath);

            // Her chunk için ayrı klasör
            String chunksDir = sessionDir + "/chunks";
            Path chunksPath = Paths.get(chunksDir);
            Files.createDirectories(chunksPath);

            // İstatistikler için
            Map<String, Integer> fileChunkCounts = new LinkedHashMap<>();
            Map<String, Integer> extensionCounts = new LinkedHashMap<>();
            List<Map<String, Object>> chunkIndex = new ArrayList<>(); // Tüm chunk'ların index'i

            System.out.printf("[Ingest] Çıktı klasörü: %s%n", sessionDir);

            List<GitLabClient.TreeItem> all = gitlab.listRepoTree().block();
            if (all == null) {
                System.out.println("GitLab tree boş döndü (null).");
                return;
            }

            System.out.printf("[Ingest] tree total=%d%n", all.size());

            Map<String, String> pathToSha = new HashMap<>();
            List<String> blobs = all.stream()
                    .filter(t -> "blob".equalsIgnoreCase(t.type()))
                    .peek(t -> pathToSha.put(t.path(), t.id()))
                    .map(GitLabClient.TreeItem::path)
                    .toList();

            // Filtreleme
            List<String> afterPrefix = blobs.stream()
                    .filter(p -> onlyPrefix.isEmpty()
                            || p.equalsIgnoreCase(onlyPrefix + ".md")
                            || p.startsWith(onlyPrefix + "/"))
                    .collect(Collectors.toList());

            List<String> noExcluded = afterPrefix.stream()
                    .filter(p -> !isExcluded(p))
                    .collect(Collectors.toList());

            List<String> paths = noExcluded.stream()
                    .filter(p -> onlyPrefix.isEmpty() ? isIncluded(p) : true)
                    .collect(Collectors.toList());

            System.out.printf("[Ingest] İşlenecek dosya sayısı: %d%n", paths.size());

            List<Document> allDocs = new ArrayList<>();
            int globalIdx = 0;

            for (String p : paths) {
                try {
                    byte[] raw;
                    try {
                        raw = gitlab.fetchRaw(p).block();
                    } catch (Exception pathErr) {
                        String sha = pathToSha.get(p);
                        if (sha == null) {
                            System.out.printf("ERR no blob sha for %s%n", p);
                            continue;
                        }
                        raw = gitlab.fetchBlobRaw(sha).block();
                    }

                    if (raw == null || raw.length == 0) {
                        System.out.printf("Skip (empty) %s%n", p);
                        continue;
                    }
                    if (raw.length > maxBytesPerFile) {
                        System.out.printf("Skip (too big) %s size=%d%n", p, raw.length);
                        continue;
                    }

                    String ext = extOf(p);
                    String text = switch (ext) {
                        case "md", "txt" -> norm.normalizePlain(new String(raw, StandardCharsets.UTF_8));
                        case "html", "htm" -> norm.htmlToText(raw);
                        case "pdf" -> pdfToText(raw);
                        default -> null;
                    };

                    if (text == null || text.isBlank()) {
                        System.out.printf("Skip (empty text) %s%n", p);
                        continue;
                    }

                    Map<String, String> documentMetadata = extractDocumentMetadata(text, p);

                    List<Document> chunks = chunkWithSections(
                            text, 2000, 200,
                            "gitlab://" + host + "/" + projectPath + "@" + branch,
                            null,
                            Map.of("repo_path", p, "repo_branch", branch),
                            documentMetadata
                    );

                    int fileChunkCount = 0;
                    for (Document d : chunks) {
                        d.getMetadata().put("g_index", String.valueOf(globalIdx));

                        // Her chunk için veri hazırla
                        Map<String, Object> chunkData = new LinkedHashMap<>();

                        // Temel bilgiler
                        chunkData.put("id", d.getMetadata().get("id"));
                        chunkData.put("source", d.getMetadata().get("source"));
                        chunkData.put("repo_path", d.getMetadata().get("repo_path"));
                        chunkData.put("repo_branch", d.getMetadata().get("repo_branch"));

                        // Section ve breadcrumb bilgileri
                        if (d.getMetadata().containsKey("breadcrumbs")) {
                            chunkData.put("breadcrumbs", d.getMetadata().get("breadcrumbs"));
                        }
                        if (d.getMetadata().containsKey("section_id")) {
                            chunkData.put("section_id", d.getMetadata().get("section_id"));
                        }

                        // Front matter bilgileri
                        d.getMetadata().entrySet().stream()
                                .filter(e -> e.getKey().startsWith("fm:"))
                                .forEach(e -> chunkData.put(e.getKey(), e.getValue()));

                        // Chunk pozisyon bilgileri
                        chunkData.put("chunk_index", String.valueOf(d.getMetadata().get("chunk_index")));
                        chunkData.put("offset", String.valueOf(d.getMetadata().get("offset")));
                        chunkData.put("length", String.valueOf(d.getMetadata().get("length")));

                        // İçerik
                        chunkData.put("content", d.getText());

                        // HER CHUNK İÇİN AYRI JSONL DOSYASI OLUŞTUR
                        String chunkFileName = String.format("%s/chunk_%06d.jsonl", chunksDir, globalIdx);
                        try (BufferedWriter chunkWriter = new BufferedWriter(new FileWriter(chunkFileName))) {
                            chunkWriter.write(objectMapper.writeValueAsString(chunkData));
                            chunkWriter.newLine();
                        }

                        // Index'e ekle (özet için)
                        Map<String, Object> indexEntry = new LinkedHashMap<>();
                        indexEntry.put("chunk_id", globalIdx);
                        indexEntry.put("file", String.format("chunks/chunk_%06d.jsonl", globalIdx));
                        indexEntry.put("source_file", p);
                        indexEntry.put("section", d.getMetadata().get("breadcrumbs"));
                        indexEntry.put("size", String.valueOf(d.getMetadata().get("length")));
                        chunkIndex.add(indexEntry);

                        globalIdx++;
                        fileChunkCount++;
                    }

                    allDocs.addAll(chunks);
                    fileChunkCounts.put(p, fileChunkCount);
                    extensionCounts.merge(ext, fileChunkCount, Integer::sum);

                    System.out.printf("✓ %s -> %d chunk (dosyalar: chunk_%06d - chunk_%06d)%n",
                            p, fileChunkCount, globalIdx - fileChunkCount, globalIdx - 1);

                } catch (Exception ex) {
                    System.out.printf("✗ ERR %s : %s%n", p, ex.getMessage());
                }
            }

            // Ana index dosyası oluştur (tüm chunk'ların listesi)
            String indexFileName = sessionDir + "/chunks_index.json";
            Map<String, Object> indexData = new LinkedHashMap<>();
            indexData.put("timestamp", timestamp);
            indexData.put("total_chunks", allDocs.size());
            indexData.put("chunks_directory", "chunks/");
            indexData.put("chunks", chunkIndex);

            Files.write(Paths.get(indexFileName),
                    objectMapper.writerWithDefaultPrettyPrinter()
                            .writeValueAsString(indexData)
                            .getBytes(StandardCharsets.UTF_8));

            // Detaylı özet dosyası
            String summaryFileName = sessionDir + "/summary.json";
            Map<String, Object> summary = new LinkedHashMap<>();
            summary.put("timestamp", timestamp);
            summary.put("session_directory", sessionDir);
            summary.put("total_chunks", allDocs.size());
            summary.put("total_files", fileChunkCounts.size());
            summary.put("project", Map.of(
                    "path", projectPath,
                    "branch", branch,
                    "host", host,
                    "prefix", onlyPrefix
            ));
            summary.put("statistics", Map.of(
                    "by_extension", extensionCounts,
                    "average_chunks_per_file", fileChunkCounts.isEmpty() ? 0 :
                            allDocs.size() / fileChunkCounts.size(),
                    "total_size_bytes", allDocs.stream()
                            .mapToInt(d -> Integer.parseInt(String.valueOf(d.getMetadata().get("length"))))
                            .sum()
            ));
            summary.put("files_processed", fileChunkCounts);

            Files.write(Paths.get(summaryFileName),
                    objectMapper.writerWithDefaultPrettyPrinter()
                            .writeValueAsString(summary)
                            .getBytes(StandardCharsets.UTF_8));

            if (!allDocs.isEmpty()) {
                store.add(allDocs);
                System.out.println("\n" + "=".repeat(70));
                System.out.println("✅ GitLab ingest tamamlandı!");
                System.out.println("📊 Toplam: " + allDocs.size() + " chunk, " + fileChunkCounts.size() + " dosya");
                System.out.println("📁 Session klasörü: " + sessionDir);
                System.out.println("📄 Her chunk ayrı dosyada: " + chunksDir);
                System.out.println("📋 Index dosyası: " + indexFileName);
                System.out.println("📈 Özet dosyası: " + summaryFileName);
                System.out.println("=".repeat(70));
            } else {
                System.out.println("⚠️ GitLab ingest: eklenecek chunk bulunamadı.");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Markdown'dan front matter ve section bilgilerini çıkar
    private Map<String, String> extractDocumentMetadata(String text, String path) {
        Map<String, String> metadata = new LinkedHashMap<>();

        if (text.startsWith("---")) {
            int endIndex = text.indexOf("---", 3);
            if (endIndex > 0) {
                String frontMatter = text.substring(3, endIndex);
                String[] lines = frontMatter.split("\n");
                for (String line : lines) {
                    if (line.contains(":")) {
                        String[] parts = line.split(":", 2);
                        if (parts.length == 2) {
                            String key = parts[0].trim();
                            String value = parts[1].trim().replaceAll("^['\"]|['\"]$", "");
                            metadata.put("fm:" + key, value);
                        }
                    }
                }
            }
        }

        return metadata;
    }

    // Section-aware chunking
    private List<Document> chunkWithSections(String text, int size, int overlap,
                                             String source, Integer page,
                                             Map<String,String> extraMeta,
                                             Map<String,String> docMeta) {
        List<Document> result = new ArrayList<>();
        if (text == null) return result;

        String currentSection = "";
        String currentSectionId = "";

        int start = 0, idx = 0;
        while (start < text.length()) {
            int end = Math.min(text.length(), start + size);
            String part = text.substring(start, end);

            Pattern sectionPattern = Pattern.compile("^#+\\s+(.+)$", Pattern.MULTILINE);
            Matcher matcher = sectionPattern.matcher(part);
            if (matcher.find()) {
                currentSection = matcher.group(1).trim();
                currentSectionId = currentSection.toLowerCase()
                        .replaceAll("[^a-z0-9]+", "-")
                        .replaceAll("^-|-$", "");
            }

            Document d = new Document(part);
            d.getMetadata().put("source", source);
            if (page != null) d.getMetadata().put("page", String.valueOf(page));
            d.getMetadata().put("chunk_index", String.valueOf(idx));
            d.getMetadata().put("offset", String.valueOf(start));
            d.getMetadata().put("length", String.valueOf(part.length()));

            if (!currentSection.isEmpty()) {
                d.getMetadata().put("breadcrumbs", currentSection);
                d.getMetadata().put("section_id", currentSectionId);
            }

            if (extraMeta != null) d.getMetadata().putAll(extraMeta);
            if (docMeta != null) d.getMetadata().putAll(docMeta);

            String idStr = String.format("%s|sec=%s|i=%d|o=%d",
                    source,
                    currentSectionId.isEmpty() ? "main" : currentSectionId,
                    idx,
                    start
            );
            d.getMetadata().put("id", idStr);

            result.add(d);

            if (end == text.length()) break;
            start = Math.max(0, end - overlap);
            idx++;
        }
        return result;
    }

    // Helper metodlar aynı kalıyor...
    private static List<String> splitList(String raw, String fallback) {
        String src = (raw == null || raw.isBlank()) ? fallback : raw;
        String[] parts = src.split(",");
        List<String> out = new ArrayList<>();
        for (String p : parts) {
            String s = p.trim();
            if (!s.isEmpty()) out.add(s);
        }
        return out;
    }

    private boolean isIncluded(String path) {
        return micromatch(path, include, true);
    }

    private boolean isExcluded(String path) {
        return micromatch(path, exclude, false);
    }

    private boolean micromatch(String path, List<String> globs, boolean defaultIfEmpty) {
        if (globs == null || globs.isEmpty()) return defaultIfEmpty;
        for (String g : globs) {
            String regex = globToRegex(g.trim());
            if (Pattern.compile(regex, Pattern.CASE_INSENSITIVE).matcher(path).matches()) {
                return true;
            }
        }
        return false;
    }

    private String globToRegex(String glob) {
        String r = glob.replace(".", "\\.")
                .replace("**/", "(.*/)?")
                .replace("**", ".*")
                .replace("*", "[^/]*");
        return "^" + r + "$";
    }

    private String extOf(String p) {
        int i = p.lastIndexOf('.');
        return i < 0 ? "" : p.substring(i + 1).toLowerCase(Locale.ROOT);
    }

    private String pdfToText(byte[] raw) throws Exception {
        try (PDDocument pdf = PDDocument.load(new ByteArrayInputStream(raw))) {
            PDFTextStripper stripper = new PDFTextStripper();
            String text = stripper.getText(pdf);
            return norm.normalizePlain(text);
        }
    }
}
