package inspect

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/x4b1/messenger"
)

const defaultLimit = 25

// Pagination defines a page and limit to get paginated messages.
type Pagination struct {
	Page  int
	Limit int
}

// Query defines the filters to request messages.
type Query struct {
	Pagination
}

// Result defines the paginated messages.
type Result struct {
	Total int
	Msgs  []*messenger.GenericMessage
}

// Store knows how to retrieve messages.
type Store interface {
	Find(ctx context.Context, q *Query) (*Result, error)
	Republish(ctx context.Context, msgID ...string) error
}

// NewInspector returns a new instance of the Inspector.
func NewInspector(s Store) *Inspector {
	return &Inspector{s}
}

var _ http.Handler = (*Inspector)(nil)

// Inspector implements an http handler that serves a UI that shows stored messages.
type Inspector struct {
	s Store
}

// ServeHTTP is a httpHandler that renders the index.tmpl with the messages stored.
func (i *Inspector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/republish") {
		i.handleRepublish(w, r)
		return
	}
	i.handleIndex(w, r)
}

func (i *Inspector) handleIndex(w http.ResponseWriter, r *http.Request) {
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page == 0 {
		page = 1
	}

	res, err := i.s.Find(r.Context(), &Query{
		Pagination: Pagination{
			Page:  page,
			Limit: defaultLimit,
		},
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))
		return
	}

	if err := indexTemplate.Execute(w, struct {
		*Result
		Page int
	}{
		Result: res,
		Page:   page,
	}); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))
	}
}

type republishRequest struct {
	MessageIDs []string `json:"message_ids,omitempty"`
}

func (i *Inspector) handleRepublish(w http.ResponseWriter, r *http.Request) {
	var req republishRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(err.Error()))
		return
	}
	defer r.Body.Close()

	if err := i.s.Republish(r.Context(), req.MessageIDs...); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))
		return
	}

	w.WriteHeader(http.StatusOK)
}
