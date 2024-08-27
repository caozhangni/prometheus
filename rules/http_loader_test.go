package rules

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

// type httpServer struct {
// 	server *httptest.Server
// }

type httpServer struct {
	*httptest.Server
}

func (h *httpServer) start() {
	h.Start()
}

func (h *httpServer) close() {
	h.Close()
}

func newHttpServer(rgs *rulefmt.RuleGroups) *httpServer {
	rgsb, _ := yaml.Marshal(rgs)
	server := httptest.NewUnstartedServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// w.Header().Set("Content-Type", `text/plain; version=0.0.4`)
			w.Write(rgsb)
		}),
	)
	return &httpServer{
		Server: server,
	}
}

func TestHttpLoader_Load(t *testing.T) {

	testCases := []struct {
		name    string
		testUrl string
		rgs     *rulefmt.RuleGroups

		wantRgs  *rulefmt.RuleGroups
		wantErrs []error
	}{
		{
			name: "load成功",
			rgs: &rulefmt.RuleGroups{
				Groups: []rulefmt.RuleGroup{
					{
						Name:     "规则1",
						Interval: model.Duration(time.Second),
						// QueryOffset: model.Duration(time.Second),
						Rules: []rulefmt.RuleNode{},
					},
				},
			},
		},
		// {
		// 	name:    "load超时",
		// 	testUrl: "http://aaa.com.cn",
		// 	wantErrs: []error{
		// 		errors.New("http://aaa.com.cn: Get \"http://aaa.com.cn\": context deadline exceeded (Client.Timeout exceeded while awaiting headers)"),
		// 	},
		// },
		// {
		// 	name:    "load url错误",
		// 	testUrl: "htt://aaa.com.cn",
		// },
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			server := newHttpServer(tc.rgs)
			server.start()
			defer server.close()
			testUrl := tc.testUrl
			if len(tc.testUrl) == 0 {
				serverURL, err := url.Parse(server.URL)
				if err != nil {
					panic(err)
				}
				testUrl = fmt.Sprintf("%s://%s", serverURL.Scheme, serverURL.Host)
			}
			hl := HttpLoader{}
			res, errs := hl.Load(testUrl)
			assert.Equal(t, tc.wantErrs, errs)
			assert.Equal(t, tc.rgs, res)
		})
	}
}
