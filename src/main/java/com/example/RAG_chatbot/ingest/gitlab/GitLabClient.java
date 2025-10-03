package com.example.RAG_chatbot.ingest.gitlab;

import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import org.springframework.web.util.UriUtils;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Component
public class GitLabClient {

    private final WebClient http;
    private final String host;
    private final String projectPath;       // e.g. "avsos-wiki/riverx"
    private final String projectIdProp;     // if set, skip path->id lookup
    private final String branch;
    private final AtomicReference<String> cachedProjectId = new AtomicReference<>(null);

    public GitLabClient(Environment env, WebClient.Builder builder) {
        this.host = env.getProperty("app.ingest.gitlab.host", "https://gitlab.com");
        this.projectPath = env.getProperty("app.ingest.gitlab.projectPath");
        this.projectIdProp = env.getProperty("app.ingest.gitlab.projectId"); // optional
        this.branch = env.getProperty("app.ingest.gitlab.branch", "main");
        String token = env.getProperty("app.ingest.gitlab.token");           // may be blank

        WebClient.Builder b = builder.baseUrl(host);
        if (token != null && !token.isBlank()) b.defaultHeader("PRIVATE-TOKEN", token);
        this.http = b.build();

        if ((this.projectPath == null || this.projectPath.isBlank())
                && (this.projectIdProp == null || this.projectIdProp.isBlank())) {
            throw new IllegalArgumentException("Set app.ingest.gitlab.projectPath or app.ingest.gitlab.projectId");
        }
        String tokenMask = (token == null || token.isBlank())
                ? "(none)"
                : token.substring(0, Math.min(6, token.length())) + "...";
        System.out.printf("[GitLab] host=%s path=%s projectId=%s branch=%s token=%s%n",
                host, projectPath, projectIdProp, branch, tokenMask);
    }

    /** Resolve numeric project id (uses configured projectId if provided). */
    private Mono<String> getProjectId() {
        if (projectIdProp != null && !projectIdProp.isBlank()) {
            System.out.printf("[GitLab] using configured projectId=%s%n", projectIdProp);
            return Mono.just(projectIdProp);
        }
        String cached = cachedProjectId.get();
        if (cached != null) return Mono.just(cached);

        // Encode ONCE (slashes -> %2F) and use absolute URI to avoid double-encoding of '%'
        String encodedPath = UriUtils.encode(projectPath, StandardCharsets.UTF_8);
        String full = host + "/api/v4/projects/" + encodedPath;
        java.net.URI uri = java.net.URI.create(full);

        System.out.printf("[GitLab] resolve projectId uri=%s (path=%s)%n", uri, projectPath);

        return http.get()
                .uri(uri)
                .retrieve()
                .bodyToMono(Map.class)
                .map(map -> {
                    Object id = map.get("id");
                    if (id == null) throw new IllegalStateException("Project JSON’da id yok");
                    String pid = String.valueOf(id);
                    cachedProjectId.compareAndSet(null, pid);
                    System.out.printf("[GitLab] projectId=%s (name=%s, visibility=%s)%n",
                            pid, map.get("name_with_namespace"), map.get("visibility"));
                    return pid;
                })
                .onErrorMap(WebClientResponseException.class, e -> {
                    String msg = String.format("Proje çözülemedi (%d): %s. Path=%s",
                            e.getRawStatusCode(), e.getResponseBodyAsString(), projectPath);
                    return new IllegalStateException(msg, e);
                });
    }

    /** List repository tree (all pages, with ref). */
    public Mono<List<TreeItem>> listRepoTree() {
        return getProjectId().flatMap(this::listRepoTreeAllPages);
    }

    private Mono<List<TreeItem>> listRepoTreeAllPages(String pid) {
        List<TreeItem> acc = new ArrayList<>();
        return fetchTreePage(pid, 1, acc);
    }

    private Mono<List<TreeItem>> fetchTreePage(String pid, int page, List<TreeItem> acc) {
        return http.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/api/v4/projects/{pid}/repository/tree")
                        .queryParam("recursive", "true")
                        .queryParam("per_page", "100")
                        .queryParam("page", page)
                        .queryParam("ref", branch)          // include branch ref
                        .build(pid))
                .exchangeToMono(resp -> resp.bodyToFlux(TreeItem.class).collectList()
                        .flatMap(list -> {
                            acc.addAll(list);
                            String next = resp.headers().asHttpHeaders().getFirst("X-Next-Page");
                            System.out.printf("[GitLab] tree page=%d items=%d next=%s%n", page, list.size(), next);
                            if (next != null && !next.isBlank()) {
                                return fetchTreePage(pid, Integer.parseInt(next), acc);
                            }
                            return Mono.just(acc);
                        })
                );
    }

    /** Fetch raw file bytes by repository PATH (with ref). */
    public Mono<byte[]> fetchRaw(String filePath) {
        return getProjectId().flatMap(pid -> {
            String path = UriUtils.encode(filePath, StandardCharsets.UTF_8);
            String ref  = UriUtils.encode(this.branch, StandardCharsets.UTF_8);
            String url = String.format("/api/v4/projects/%s/repository/files/%s/raw?ref=%s", pid, path, ref);
            System.out.printf("[GitLab] raw url=%s%s%n", host, url);
            return http.get()
                    .uri(url)
                    .retrieve()
                    .onStatus(s -> s == HttpStatus.NOT_FOUND, r ->
                            r.bodyToMono(String.class).flatMap(body ->
                                    Mono.error(new IllegalStateException(
                                            "File 404 by path: " + filePath + " body=" + body))))
                    .bodyToMono(byte[].class);
        });
    }

    /** Fetch raw file bytes by BLOB SHA (path issues workaround). */
    public Mono<byte[]> fetchBlobRaw(String blobSha) {
        return getProjectId().flatMap(pid -> {
            String url = String.format("/api/v4/projects/%s/repository/blobs/%s/raw", pid, blobSha);
            System.out.printf("[GitLab] blob raw url=%s%s%n", host, url);
            return http.get()
                    .uri(url)
                    .retrieve()
                    .onStatus(s -> s == HttpStatus.NOT_FOUND, r ->
                            r.bodyToMono(String.class).flatMap(body ->
                                    Mono.error(new IllegalStateException(
                                            "File 404 by blob sha: " + blobSha + " body=" + body))))
                    .bodyToMono(byte[].class);
        });
    }

    /** GitLab tree item; id = blob sha for type=blob */
    public static record TreeItem(String id, String name, String type, String path, String mode) {}
}
