package com.example.RAG_chatbot.web;

import com.example.RAG_chatbot.core.RagService;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping(value = "/api/chat", produces = MediaType.APPLICATION_JSON_VALUE)
@CrossOrigin(origins = { "http://localhost:8080", "http://localhost:63342" }) // dev use
public class ChatController {

    private final RagService rag;

    public ChatController(RagService rag) {
        this.rag = rag;
    }

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String, Object>> chat(@RequestBody Map<String, String> body) {
        String q = body.getOrDefault("message", "").trim();
        if (q.isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "message is required"
            ));
        }
        // Artık cevap + kullanılan chunk’lar geri dönüyor
        Map<String, Object> payload = rag.answerWithDebug(q);
        return ResponseEntity.ok(payload);
    }
}
