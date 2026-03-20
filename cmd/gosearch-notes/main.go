package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"gosearch/analysis"
	"gosearch/document"
	"gosearch/index"
	"gosearch/search"
	"gosearch/store"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "add":
		cmdAdd(os.Args[2:])
	case "search":
		cmdSearch(os.Args[2:])
	case "delete":
		cmdDelete(os.Args[2:])
	case "import":
		cmdImport(os.Args[2:])
	case "stats":
		cmdStats(os.Args[2:])
	case "serve":
		cmdServe(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintf(os.Stderr, `Usage: gosearch-notes <command> [options]

Commands:
  add       Add a note
  search    Search notes
  delete    Delete notes by field match
  import    Import markdown files from a directory
  stats     Show index statistics
  serve     Start HTTP server for note management
`)
}

// indexDir returns the index directory path.
func indexDir() string {
	if dir := os.Getenv("GOSEARCH_INDEX_DIR"); dir != "" {
		return dir
	}
	home, err := os.UserHomeDir()
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot determine home directory: %v\n", err)
		os.Exit(1)
	}
	return filepath.Join(home, ".gosearch-notes", "index")
}

func newAnalyzer() *analysis.Analyzer {
	return analysis.NewAnalyzer(
		analysis.NewWhitespaceTokenizer(),
		&analysis.LowerCaseFilter{},
	)
}

func openWriter(bufferSize int) (*index.IndexWriter, *store.FSDirectory) {
	dir, err := store.NewFSDirectory(indexDir())
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot open index directory: %v\n", err)
		os.Exit(1)
	}
	writer := index.NewIndexWriter(dir, newAnalyzer(), bufferSize)
	return writer, dir
}

func openReader() *index.IndexReader {
	dir, err := store.NewFSDirectory(indexDir())
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot open index directory: %v\n", err)
		os.Exit(1)
	}
	reader, err := index.OpenDirectoryReader(dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot open index: %v\n", err)
		os.Exit(1)
	}
	return reader
}

func createNoteDoc(title, body, tags, category string, priority int) *document.Document {
	doc := document.NewDocument()

	doc.AddField("title", title, document.FieldTypeText)
	doc.AddField("body", body, document.FieldTypeText)

	// Tags: each tag as a separate keyword field
	tagList := parseTags(tags)
	for _, tag := range tagList {
		doc.AddField("tag", strings.ToLower(strings.TrimSpace(tag)), document.FieldTypeKeyword)
	}
	doc.AddField("tags_display", strings.Join(tagList, ", "), document.FieldTypeStored)

	// Category
	doc.AddField("category", category, document.FieldTypeKeyword)
	doc.AddSortedDocValuesField("category", category)
	doc.AddField("category_display", category, document.FieldTypeStored)

	// Priority
	doc.AddNumericDocValuesField("priority", int64(priority))
	doc.AddField("priority_display", strconv.Itoa(priority), document.FieldTypeStored)

	// Created
	now := time.Now().Unix()
	doc.AddNumericDocValuesField("created", now)
	doc.AddField("created_display", time.Unix(now, 0).Format(time.RFC3339), document.FieldTypeStored)

	return doc
}

func parseTags(tags string) []string {
	if tags == "" {
		return nil
	}
	parts := strings.Split(tags, ",")
	var result []string
	for _, p := range parts {
		t := strings.TrimSpace(p)
		if t != "" {
			result = append(result, strings.ToLower(t))
		}
	}
	return result
}

// --- add command ---

func cmdAdd(args []string) {
	fs := flag.NewFlagSet("add", flag.ExitOnError)
	title := fs.String("title", "", "Note title")
	body := fs.String("body", "", "Note body")
	tags := fs.String("tags", "", "Comma-separated tags")
	category := fs.String("category", "uncategorized", "Category")
	priority := fs.Int("priority", 5, "Priority (1-10)")
	fs.Parse(args)

	if *title == "" {
		fmt.Fprintf(os.Stderr, "error: --title is required\n")
		os.Exit(1)
	}
	if *body == "" {
		fmt.Fprintln(os.Stderr, "error: --body is required")
		os.Exit(1)
	}

	writer, _ := openWriter(100)
	doc := createNoteDoc(*title, *body, *tags, *category, *priority)
	if err := writer.AddDocument(doc); err != nil {
		fmt.Fprintf(os.Stderr, "error adding document: %v\n", err)
		os.Exit(1)
	}
	if err := writer.Commit(); err != nil {
		fmt.Fprintf(os.Stderr, "error committing: %v\n", err)
		os.Exit(1)
	}
	writer.Close()
	fmt.Println("Note added successfully.")
}

// --- search command ---

func cmdSearch(args []string) {
	fs := flag.NewFlagSet("search", flag.ExitOnError)
	tag := fs.String("tag", "", "Filter by tag")
	category := fs.String("category", "", "Filter by category")
	not := fs.String("not", "", "Exclude terms (space-separated)")
	sortBy := fs.String("sort", "score", "Sort by: score, priority, created, category")
	desc := fs.Bool("desc", false, "Reverse sort order")
	limit := fs.Int("limit", 10, "Max results")
	phrase := fs.Bool("phrase", false, "Use phrase query")

	boolFlags := map[string]bool{"desc": true, "phrase": true}
	flagArgs, positional := splitFlagsAndArgs(args, boolFlags)
	fs.Parse(flagArgs)

	queryText := strings.Join(positional, " ")

	reader := openReader()
	defer reader.Close()

	q := buildQuery(queryText, *tag, *category, *not, *phrase)
	if q == nil {
		fmt.Fprintf(os.Stderr, "error: provide search terms, --tag, or --category\n")
		os.Exit(1)
	}

	collector := createCollector(*limit, *sortBy, *desc)
	searcher := search.NewIndexSearcher(reader)
	results := searcher.Search(q, collector)
	displayResults(results, *sortBy)
}

func buildQuery(queryText, tag, category, notTerms string, phrase bool) search.Query {
	bq := search.NewBooleanQuery()
	hasClauses := false

	// Text search
	if queryText != "" {
		if phrase {
			terms := strings.Fields(strings.ToLower(queryText))
			titlePhrase := search.NewPhraseQuery("title", terms...)
			bodyPhrase := search.NewPhraseQuery("body", terms...)
			phraseQ := search.NewBooleanQuery()
			phraseQ.Add(titlePhrase, search.OccurShould)
			phraseQ.Add(bodyPhrase, search.OccurShould)
			bq.Add(phraseQ, search.OccurMust)
		} else {
			terms := strings.Fields(strings.ToLower(queryText))
			termQ := search.NewBooleanQuery()
			for _, t := range terms {
				termQ.Add(search.NewTermQuery("title", t), search.OccurShould)
				termQ.Add(search.NewTermQuery("body", t), search.OccurShould)
			}
			bq.Add(termQ, search.OccurMust)
		}
		hasClauses = true
	}

	// Tag filter
	if tag != "" {
		bq.Add(search.NewTermQuery("tag", strings.ToLower(tag)), search.OccurMust)
		hasClauses = true
	}

	// Category filter
	if category != "" {
		bq.Add(search.NewTermQuery("category", category), search.OccurMust)
		hasClauses = true
	}

	// NOT terms
	if notTerms != "" {
		for _, t := range strings.Fields(strings.ToLower(notTerms)) {
			bq.Add(search.NewTermQuery("title", t), search.OccurMustNot)
			bq.Add(search.NewTermQuery("body", t), search.OccurMustNot)
		}
	}

	if !hasClauses {
		return nil
	}
	return bq
}

func createCollector(limit int, sortBy string, desc bool) search.Collector {
	switch sortBy {
	case "priority":
		reverse := true
		if desc {
			reverse = !reverse
		}
		return search.NewTopFieldCollector(limit, search.NewSort(
			search.SortField{Field: "priority", Type: search.SortFieldNumeric, Reverse: reverse},
		))
	case "created":
		reverse := true
		if desc {
			reverse = !reverse
		}
		return search.NewTopFieldCollector(limit, search.NewSort(
			search.SortField{Field: "created", Type: search.SortFieldNumeric, Reverse: reverse},
		))
	case "category":
		reverse := false
		if desc {
			reverse = true
		}
		return search.NewTopFieldCollector(limit, search.NewSort(
			search.SortField{Field: "category", Type: search.SortFieldString, Reverse: reverse},
		))
	default:
		return search.NewTopKCollector(limit)
	}
}

func displayResults(results []search.SearchResult, sortBy string) {
	fmt.Printf("=== Results (%d hits) ===\n", len(results))
	if len(results) == 0 {
		return
	}
	fmt.Println()

	for i, r := range results {
		title := r.Fields["title"]
		if title == "" {
			title = "(untitled)"
		}

		if sortBy == "score" {
			fmt.Printf("[%d] %-48s Score: %.3f\n", i+1, title, r.Score)
		} else {
			fmt.Printf("[%d] %s\n", i+1, title)
		}

		tags := r.Fields["tags_display"]
		cat := r.Fields["category_display"]
		pri := r.Fields["priority_display"]
		fmt.Printf("    Tags: %-16s Category: %-12s Priority: %s\n", tags, cat, pri)

		body := r.Fields["body"]
		fmt.Printf("    %s\n\n", truncate(body, 200))
	}
}

func truncate(s string, maxLen int) string {
	runes := []rune(s)
	if len(runes) <= maxLen {
		return s
	}
	return string(runes[:maxLen]) + "..."
}

// splitFlagsAndArgs separates flag arguments from positional arguments,
// allowing flags and positional args to be interspersed.
func splitFlagsAndArgs(args []string, boolFlags map[string]bool) (flagArgs, positional []string) {
	i := 0
	for i < len(args) {
		arg := args[i]
		if strings.HasPrefix(arg, "-") {
			name := strings.TrimLeft(arg, "-")
			if strings.Contains(name, "=") {
				flagArgs = append(flagArgs, arg)
			} else if boolFlags[name] {
				flagArgs = append(flagArgs, arg)
			} else if i+1 < len(args) {
				flagArgs = append(flagArgs, arg, args[i+1])
				i++
			} else {
				flagArgs = append(flagArgs, arg)
			}
		} else {
			positional = append(positional, arg)
		}
		i++
	}
	return
}

// --- delete command ---

func cmdDelete(args []string) {
	fs := flag.NewFlagSet("delete", flag.ExitOnError)
	tag := fs.String("tag", "", "Delete by tag")
	title := fs.String("title", "", "Delete by title term")
	fs.Parse(args)

	if *tag == "" && *title == "" {
		fmt.Fprintf(os.Stderr, "error: specify --tag or --title\n")
		os.Exit(1)
	}

	var field, term string
	if *tag != "" {
		field = "tag"
		term = strings.ToLower(*tag)
	} else {
		field = "title"
		term = strings.ToLower(*title)
	}

	fmt.Printf("Delete all documents where %s = %q? (y/n): ", field, term)
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Scan()
	if strings.ToLower(strings.TrimSpace(scanner.Text())) != "y" {
		fmt.Println("Cancelled.")
		return
	}

	writer, _ := openWriter(100)
	if err := writer.DeleteDocuments(field, term); err != nil {
		fmt.Fprintf(os.Stderr, "error deleting: %v\n", err)
		os.Exit(1)
	}
	if err := writer.Commit(); err != nil {
		fmt.Fprintf(os.Stderr, "error committing: %v\n", err)
		os.Exit(1)
	}
	writer.Close()
	fmt.Println("Documents deleted successfully.")
}

// --- import command ---

func cmdImport(args []string) {
	if len(args) < 1 {
		fmt.Fprintf(os.Stderr, "usage: gosearch-notes import <directory>\n")
		os.Exit(1)
	}
	dir := args[0]

	// Collect markdown files
	var files []string
	filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		ext := strings.ToLower(filepath.Ext(path))
		if ext == ".md" || ext == ".markdown" {
			files = append(files, path)
		}
		return nil
	})

	if len(files) == 0 {
		fmt.Println("No markdown files found.")
		return
	}

	writer, _ := openWriter(1000)

	type noteData struct {
		doc *document.Document
		err error
	}

	ch := make(chan noteData, len(files))
	var wg sync.WaitGroup

	for _, f := range files {
		wg.Add(1)
		go func(path string) {
			defer wg.Done()
			data, err := os.ReadFile(path)
			if err != nil {
				ch <- noteData{err: fmt.Errorf("read %s: %w", path, err)}
				return
			}
			content := string(data)

			title, body, tags, category, priority := parseFrontmatter(content, path)
			doc := createNoteDoc(title, body, tags, category, priority)
			ch <- noteData{doc: doc}
		}(f)
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	count := 0
	for nd := range ch {
		if nd.err != nil {
			fmt.Fprintf(os.Stderr, "warning: %v\n", nd.err)
			continue
		}
		if err := writer.AddDocument(nd.doc); err != nil {
			fmt.Fprintf(os.Stderr, "error adding document: %v\n", err)
			continue
		}
		count++
	}

	if err := writer.Commit(); err != nil {
		fmt.Fprintf(os.Stderr, "error committing: %v\n", err)
		os.Exit(1)
	}
	writer.Close()
	fmt.Printf("Imported %d notes from %d files.\n", count, len(files))
}

func parseFrontmatter(content, path string) (title, body, tags, category string, priority int) {
	priority = 5
	category = "uncategorized"

	lines := strings.SplitAfter(content, "\n")
	// Check for frontmatter
	if len(lines) > 0 && strings.TrimSpace(strings.TrimRight(lines[0], "\n")) == "---" {
		endIdx := -1
		for i := 1; i < len(lines); i++ {
			if strings.TrimSpace(strings.TrimRight(lines[i], "\n")) == "---" {
				endIdx = i
				break
			}
		}
		if endIdx > 0 {
			// Parse YAML-like key: value pairs
			for _, line := range lines[1:endIdx] {
				line = strings.TrimSpace(line)
				if idx := strings.Index(line, ":"); idx > 0 {
					key := strings.TrimSpace(line[:idx])
					val := strings.TrimSpace(line[idx+1:])
					switch key {
					case "title":
						title = val
					case "tags":
						tags = val
					case "category":
						category = val
					case "priority":
						if p, err := strconv.Atoi(val); err == nil {
							priority = p
						}
					}
				}
			}
			// Body is everything after the closing ---
			var bodyParts []string
			for _, l := range lines[endIdx+1:] {
				bodyParts = append(bodyParts, l)
			}
			body = strings.TrimSpace(strings.Join(bodyParts, ""))
		}
	}

	if title == "" {
		// Use filename as title
		base := filepath.Base(path)
		title = strings.TrimSuffix(base, filepath.Ext(base))
	}
	if body == "" {
		// If no frontmatter was found, use entire content as body.
		// If frontmatter was found but body is empty, use title as body
		// to avoid indexing a completely empty text field.
		if category == "uncategorized" && tags == "" {
			body = strings.TrimSpace(content)
		} else {
			body = title
		}
	}

	return
}

// --- stats command ---

func cmdStats(_ []string) {
	reader := openReader()
	defer reader.Close()

	fmt.Printf("Total documents:   %d\n", reader.TotalDocCount())
	fmt.Printf("Live documents:    %d\n", reader.LiveDocCount())
	fmt.Printf("Segments:          %d\n", len(reader.Leaves()))
}

// --- serve command ---

type server struct {
	writer          *index.IndexWriter
	dir             *store.FSDirectory
	docsSinceCommit atomic.Int64
	commitInterval  time.Duration
}

func cmdServe(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	port := fs.Int("port", 8080, "HTTP port")
	commitInterval := fs.Duration("commit-interval", 5*time.Second, "Commit interval")
	bufferSize := fs.Int("buffer-size", 1000, "Index buffer size")
	fs.Parse(args)

	writer, dir := openWriter(*bufferSize)
	writer.SetMergePolicy(index.NewTieredMergePolicy())

	s := &server{
		writer:         writer,
		dir:            dir,
		commitInterval: *commitInterval,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /notes", s.handleAddNote)
	mux.HandleFunc("GET /search", s.handleSearch)
	mux.HandleFunc("GET /stats", s.handleStats)
	mux.HandleFunc("POST /merge", s.handleForceMerge)

	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", *port),
		Handler: mux,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var commitWg sync.WaitGroup
	commitWg.Add(1)
	go func() {
		defer commitWg.Done()
		s.backgroundCommitLoop(ctx)
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		fmt.Println("\nShutting down...")
		cancel()
		httpServer.Shutdown(context.Background())
	}()

	fmt.Printf("Serving on http://localhost:%d\n", *port)
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		fmt.Fprintf(os.Stderr, "server error: %v\n", err)
		os.Exit(1)
	}

	commitWg.Wait()

	// Final commit
	if s.docsSinceCommit.Load() > 0 {
		if err := writer.Commit(); err != nil {
			fmt.Fprintf(os.Stderr, "final commit error: %v\n", err)
		}
	}
	writer.Close()
	fmt.Println("Server stopped.")
}

func (s *server) backgroundCommitLoop(ctx context.Context) {
	ticker := time.NewTicker(s.commitInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if s.docsSinceCommit.Load() > 0 {
				if err := s.writer.Commit(); err != nil {
					fmt.Fprintf(os.Stderr, "commit error: %v\n", err)
				} else {
					s.docsSinceCommit.Store(0)
				}
			}
		}
	}
}

func (s *server) handleAddNote(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Title    string `json:"title"`
		Body     string `json:"body"`
		Tags     string `json:"tags"`
		Category string `json:"category"`
		Priority int    `json:"priority"`
	}
	req.Category = "uncategorized"
	req.Priority = 5

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.Title == "" || req.Body == "" {
		jsonError(w, "title and body are required", http.StatusBadRequest)
		return
	}

	doc := createNoteDoc(req.Title, req.Body, req.Tags, req.Category, req.Priority)
	if err := s.writer.AddDocument(doc); err != nil {
		jsonError(w, "failed to add document: "+err.Error(), http.StatusInternalServerError)
		return
	}
	s.docsSinceCommit.Add(1)
	jsonResponse(w, http.StatusCreated, map[string]string{"status": "created"})
}

func (s *server) handleSearch(w http.ResponseWriter, r *http.Request) {
	queryText := r.URL.Query().Get("q")
	tag := r.URL.Query().Get("tag")
	category := r.URL.Query().Get("category")
	notTerms := r.URL.Query().Get("not")
	phrase := r.URL.Query().Get("phrase") == "true"
	sortBy := r.URL.Query().Get("sort")
	if sortBy == "" {
		sortBy = "score"
	}
	desc := r.URL.Query().Get("desc") == "true"
	limit := 10
	if l, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && l > 0 {
		limit = l
	}

	q := buildQuery(queryText, tag, category, notTerms, phrase)
	if q == nil {
		jsonError(w, "provide q, tag, or category parameter", http.StatusBadRequest)
		return
	}

	reader, err := index.OpenNRTReader(s.writer)
	if err != nil {
		jsonError(w, "failed to open reader: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer reader.Close()

	collector := createCollector(limit, sortBy, desc)
	searcher := search.NewIndexSearcher(reader)
	results := searcher.Search(q, collector)

	type resultJSON struct {
		Title    string  `json:"title"`
		Body     string  `json:"body"`
		Tags     string  `json:"tags"`
		Category string  `json:"category"`
		Priority string  `json:"priority"`
		Created  string  `json:"created"`
		Score    float64 `json:"score"`
	}

	out := make([]resultJSON, 0, len(results))
	for _, res := range results {
		out = append(out, resultJSON{
			Title:    res.Fields["title"],
			Body:     res.Fields["body"],
			Tags:     res.Fields["tags_display"],
			Category: res.Fields["category_display"],
			Priority: res.Fields["priority_display"],
			Created:  res.Fields["created_display"],
			Score:    res.Score,
		})
	}
	jsonResponse(w, http.StatusOK, out)
}

func (s *server) handleStats(w http.ResponseWriter, r *http.Request) {
	reader, err := index.OpenNRTReader(s.writer)
	if err != nil {
		jsonError(w, "failed to open reader: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer reader.Close()

	jsonResponse(w, http.StatusOK, map[string]int{
		"total_documents": reader.TotalDocCount(),
		"live_documents":  reader.LiveDocCount(),
		"segments":        len(reader.Leaves()),
	})
}

func (s *server) handleForceMerge(w http.ResponseWriter, r *http.Request) {
	maxSegments := 1
	if n, err := strconv.Atoi(r.URL.Query().Get("max_segments")); err == nil && n > 0 {
		maxSegments = n
	}

	if err := s.writer.ForceMerge(maxSegments); err != nil {
		jsonError(w, "merge failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if err := s.writer.Commit(); err != nil {
		jsonError(w, "commit after merge failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	s.docsSinceCommit.Store(0)
	jsonResponse(w, http.StatusOK, map[string]string{"status": "merged"})
}

func jsonResponse(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func jsonError(w http.ResponseWriter, msg string, status int) {
	jsonResponse(w, status, map[string]string{"error": msg})
}
