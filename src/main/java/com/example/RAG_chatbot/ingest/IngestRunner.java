package com.example.RAG_chatbot.ingest;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;
import org.springframework.util.StreamUtils;
import org.springframework.ai.document.Document;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.context.annotation.Profile;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

@Profile("file-ingest")
@Component
public class IngestRunner implements CommandLineRunner {

    private final VectorStore vectorStore;
    private final ResourceLoader resourceLoader;

    @Value("${app.ingest.path:classpath:data/wiki.txt}")
    private String ingestPath;

    public IngestRunner(VectorStore vectorStore, ResourceLoader resourceLoader) {
        this.vectorStore = vectorStore;
        this.resourceLoader = resourceLoader;
    }

    @Override
    public void run(String... args) throws Exception {
        Resource res = resourceLoader.getResource(ingestPath);
        String lower = ingestPath.toLowerCase();

        List<Document> chunks;
        if (lower.endsWith(".pdf")) {
            chunks = readPdfAsChunks(res);
        } else {
            chunks = readTxtAsChunks(res);
        }

        // Global sıra numarası ekle (isteğe bağlı ama debug için faydalı)
        for (int i = 0; i < chunks.size(); i++) {
            chunks.get(i).getMetadata().put("g_index", String.valueOf(i));
        }

        vectorStore.add(chunks);
        System.out.println("Ingest ok: " + chunks.size() + " chunk yüklendi.");
    }

    // ---- TXT ----
    private List<Document> readTxtAsChunks(Resource res) throws Exception {
        String content = StreamUtils.copyToString(res.getInputStream(), StandardCharsets.UTF_8);
        // TXT için page = null; chunk_index bu çağrı içinde 0'dan başlayacak
        return chunk(content, 2000, 200, "txt://" + ingestPath, null);
    }

    // ---- PDF (per page) ----
    private List<Document> readPdfAsChunks(Resource res) throws Exception {
        List<Document> out = new ArrayList<>();
        try (InputStream is = res.getInputStream();
             org.apache.pdfbox.pdmodel.PDDocument pdf = org.apache.pdfbox.pdmodel.PDDocument.load(is)) {

            org.apache.pdfbox.text.PDFTextStripper stripper = new org.apache.pdfbox.text.PDFTextStripper();
            int pageCount = pdf.getNumberOfPages();
            for (int p = 1; p <= pageCount; p++) {
                stripper.setStartPage(p);
                stripper.setEndPage(p);
                String pageText = stripper.getText(pdf);

                // Her sayfa için chunk_index 0'dan başlar (page + chunk_index kombinasyonu tekil kimlik gibi kullanılabilir)
                List<Document> pageChunks = chunk(pageText, 2000, 200, "pdf://" + ingestPath, p);
                out.addAll(pageChunks);
            }
        }
        return out;
    }

    // ---- Generic chunker (char-based; fine for POC) ----
    private List<Document> chunk(String text, int size, int overlap, String source, Integer page) {
        List<Document> result = new ArrayList<>();
        if (text == null) return result;

        int start = 0;
        int idx = 0; // chunk_index (TXT için dosya genelinde, PDF için sayfa bazında)
        while (start < text.length()) {
            int end = Math.min(text.length(), start + size);
            String part = text.substring(start, end);

            Document d = new Document(part);
            d.getMetadata().put("source", source);
            if (page != null) d.getMetadata().put("page", String.valueOf(page));
            d.getMetadata().put("length", String.valueOf(part.length()));
            d.getMetadata().put("offset", String.valueOf(start));
            d.getMetadata().put("chunk_index", String.valueOf(idx)); // <-- eklendi

            // İsterseniz stabil bir id gibi kullanılabilir:
            d.getMetadata().put("id", source +
                    (page != null ? "#p=" + page : "") +
                    "|o=" + start + "|l=" + part.length());

            result.add(d);

            if (end == text.length()) break;
            start = Math.max(0, end - overlap);
            idx++; // sıradaki chunk
        }
        return result;
    }
}
