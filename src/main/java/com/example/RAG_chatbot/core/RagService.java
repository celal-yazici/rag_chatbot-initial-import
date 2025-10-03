package com.example.RAG_chatbot.core;

import org.springframework.stereotype.Service;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.document.Document;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.ai.vectorstore.SearchRequest;

import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.stream.Collectors;

@Service
public class RagService {
    private final ChatClient chat;
    private final VectorStore store;

    public RagService(ChatClient.Builder builder, VectorStore store) {
        this.chat = builder.build();
        this.store = store;
    }

    /** Cevabı ve kullanılan chunk’ları birlikte döndürür + konsola loglar */
    public Map<String, Object> answerWithDebug(String userQuestion) {
        var req = SearchRequest.builder()
                .query(userQuestion)
                .topK(5)
                .similarityThreshold(0.5)
                .build();

        List<Document> hits = store.similaritySearch(req);

        // ---- Konsola okunur log ----
        System.out.println("=== Retrieved Chunks ===");
        for (int i = 0; i < hits.size(); i++) {
            Document d = hits.get(i);
            System.out.printf("#%d src=%s page=%s chunk=%s offset=%s len=%s%n",
                    i + 1,
                    d.getMetadata().get("source"),
                    d.getMetadata().get("page"),
                    d.getMetadata().get("chunk_index"),
                    d.getMetadata().get("offset"),
                    d.getMetadata().get("length")
            );
            String prev = safeText(d);
            if (!prev.isEmpty()) {
                prev = prev.replaceAll("\\s+", " ");
                System.out.println("Preview: " + prev.substring(0, prev.length()));
            }
            System.out.println("------------------------");
        }

        // ---- Boş sonuç guard ----
        if (hits == null || hits.isEmpty()) {
            return Map.of(
                    "answer", "Üzgünüm, ilgili içerik bulamadım.",
                    "usedChunks", List.of()
            );
        }

        // ---- Prompt hazırlığı ----
        String context = hits.stream()
                .map(d -> "- " + safeText(d).replaceAll("\\s+", " ").trim())
                .collect(Collectors.joining("\n"));

        String system = """
                You are Avsos wiki assistant.
                Answer ONLY using the CONTEXT below. If not enough, say you don't know.
                At the end, list short citations like: [source: <source> chunk:<idx>].
                """;

        String citations = hits.stream()
                .map(d -> "[source: " + meta(d, "source", "txt")
                        + " chunk:" + meta(d, "chunk_index", "?") + "]")
                .collect(Collectors.joining(" "));

        String prompt = "QUESTION:\n" + userQuestion + "\n\nCONTEXT:\n" + context + "\n\n" + citations;

        String answer = chat.prompt()
                .system(system)
                .user(prompt)
                .call()
                .content();

        // ---- HTTP response için usedChunks listesi (null-safe) ----
        List<Map<String, Object>> used = hits.stream().map(d -> {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("source",  meta(d, "source", ""));
            m.put("page",    meta(d, "page", ""));
            m.put("chunk",   meta(d, "chunk_index", "?"));
            m.put("offset",  meta(d, "offset", ""));
            m.put("length",  meta(d, "length", ""));
            m.put("preview", preview(safeText(d), 160));
            return m;
        }).toList();

        return Map.of("answer", answer, "usedChunks", used);
    }

    /** Eski/yeni Spring AI sürümleriyle uyum için: getText() içeriği güvenle al */
    private static String safeText(Document d) {
        String t = d.getText();
        return t == null ? "" : t;
    }

    private static String preview(String s, int n) {
        if (s == null) return "";
        s = s.replaceAll("\\s+", " ").trim();
        return s.substring(0, Math.min(n, s.length()));
    }

    /** Metadata’yı null-safe şekilde oku ve stringe çevir. */
    private static String meta(Document d, String key, String def) {
        Object v = d.getMetadata().get(key);
        return v == null ? def : Objects.toString(v, def);
    }
}
