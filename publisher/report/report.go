package report

import "time"

type stats struct {
	Time    time.Time `json:"time"`
	Total   int       `json:"total"`
	Success int       `json:"success"`
	Fail    int       `json:"fail"`
}

type report struct {
	stats stats
}

func (r *report) Init(total int) {
	r.stats = stats{
		Time:  time.Now(),
		Total: total,
	}
}
