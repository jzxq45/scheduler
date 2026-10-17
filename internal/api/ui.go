package api

import (
	"embed"
	"html/template"
	"net/http"
)

//go:embed templates
var templateFS embed.FS

var uiTemplate = template.Must(template.ParseFS(templateFS, "templates/*.html"))

// ui serves the single-page dashboard.
func (s *Server) ui(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := uiTemplate.ExecuteTemplate(w, "index.html", nil); err != nil {
		s.log.Error("render ui", "err", err)
		http.Error(w, "ui render failed", http.StatusInternalServerError)
	}
}
