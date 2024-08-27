package rules

import (
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/prometheus/prometheus/promql/parser"
)

type HttpLoader struct{}

func (HttpLoader) Load(identifier string) (*rulefmt.RuleGroups, []error) {

	client := &http.Client{
		Timeout: time.Second * 5,
	}
	req, err := http.NewRequest(http.MethodGet, identifier, nil)
	if err != nil {
		return nil, []error{fmt.Errorf("%s: %w", identifier, err)}
	}

	res, err := client.Do(req)
	if err != nil {
		return nil, []error{fmt.Errorf("%s: %w", identifier, err)}
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, []error{fmt.Errorf("%s: %w", identifier, err)}
	}

	return rulefmt.Parse(body)
}

func (HttpLoader) Parse(query string) (parser.Expr, error) { return parser.ParseExpr(query) }
