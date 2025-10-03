package com.example.RAG_chatbot.ingest;

import org.jsoup.Jsoup;
import org.springframework.stereotype.Component;

@Component
public class TextNormalizer {

    /** Generic whitespace/controls cleanup for plain text. */
    public String normalizePlain(String s) {
        if (s == null) return "";
        String t = s.replace("\uFEFF", "")            // BOM
                .replaceAll("\\p{C}", " ")        // control chars
                .replaceAll("[ \\t\\x0B\\f\\r]+", " ")
                .replaceAll("\\n{3,}", "\n\n");
        return t.trim();
    }

    /** HTML -> visible text -> normalize. */
    public String htmlToText(byte[] bytes) {
        if (bytes == null || bytes.length == 0) return "";
        String html = new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        String text = Jsoup.parse(html).text();
        return normalizePlain(text);
    }
}
