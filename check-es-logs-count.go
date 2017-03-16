package main

import (
	"fmt"
	"time"
	"strings"
	"text/template"
	"bytes"
	"encoding/json"

	"github.com/parnurzeal/gorequest"
	"gopkg.in/alecthomas/kingpin.v1"
	"github.com/olorin/nagiosplugin"
)

const (
	ver string = "0.10"
)

var (
	esURL = kingpin.Flag("url", "elasticsearch URL").Default("http://localhost:9200").Short('u').String()
	timeout = kingpin.Flag("timeout", "timeout for HTTP requests in seconds").Default("20").Int()
	timePeriod = kingpin.Flag("time-period", "check last X minutes until now").Default("5").Short('t').Int()
	indexPattern = kingpin.Flag("index-pattern", "index pattern, eg.: logstash-mediawiki").Default("logstash-*").Short('i').String()
	esQuery = kingpin.Flag("query", "elasticsearch query").Default("*").Short('q').String()
	countThreshold = kingpin.Flag("threshold", "threshold for logs count").Short('T').Required().Int()
	compareOperator = kingpin.Flag("compare-operator", "operator to compare returned value with threshold, 'lt' or 'gt'").Short('o').Default("gt").String()
)

// TemplateESQuery : struct containts elasticsearch query data
type TemplateESQuery struct {
	TimeFrom int64
	Query string
}

// QueryResult : struct containts elasticsearch query result
type QueryResult struct {
	Hits struct {
		Total int `json:"total"`
	} `json:"hits"`
}

// Msg : struct containts channel message content
type Msg struct {
	Count int
	Err error
}

var (
	templateSource = `
	{
		"size": 0,
		"query": {
			"bool": {
				"must": [
					{
						"query_string": {
							"analyze_wildcard": true,
							"query": "{{ .Query }}"
						}
					},
					{
						"range": {
							"@timestamp": {
								"lte": "now",
								"gte": {{ .TimeFrom }},
								"format": "epoch_millis"
							}
						}
					}
				],
				"must_not": []
			}
		},
		"_source": {
			"excludes": []
		},
		"aggs": {
			"3": {
				"date_histogram": {
					"field": "@timestamp",
					"interval": "1h",
					"time_zone": "UTC",
					"min_doc_count": 1
				}
			}
		}
	}
	`
)

func getRenderedTemplate(templateSource, query string, timeFrom int64) (string, error) {
	t := TemplateESQuery{
		timeFrom * 1000,
		query,
	}

	tmpl, err := template.New("TemplateESQuery").Parse(templateSource)
	if err != nil {
		return "", err
	}

	var tpl bytes.Buffer
	err = tmpl.Execute(&tpl, t)
	if err != nil {
		return "", err
	}

	return tpl.String(), nil
}

func esQueryPost(url, content string) (string, error) {
	request := gorequest.New()
	resp, body, errs := request.Post(url).Send(content).End()

	if errs != nil {
		var errsStr []string
		for _, e := range errs {
			errsStr = append(errsStr, fmt.Sprintf("%s", e))
		}
		return "", fmt.Errorf("%s", strings.Join(errsStr, ", "))
	}
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("HTTP response code: %s", resp.Status)
	}
	return body, nil
}

func getQueryResultCount(url, indexPattern, templateSource, query string, timeFrom int64, c chan Msg) {
	var msg Msg
	tmpl, err := getRenderedTemplate(templateSource, query, timeFrom)
	if err != nil {
		msg.Err = err
		c <- msg
		return
	}

	currentTime := time.Now().Local()
	url = url + "/" + indexPattern + "-" + currentTime.Format("2006.01.02") + "/_search"

	data, err := esQueryPost(url, tmpl)
	if err != nil {
		msg.Err = err
		c <- msg
		return
	}

	result, err := parseResult(data)
	if err != nil {
		msg.Err = err
		c <- msg
		return
	}

	msg.Count = result.Hits.Total
	msg.Err = nil
	c <- msg
}

func parseResult(data string) (QueryResult, error) {
	var result QueryResult
	err := json.Unmarshal([]byte(data), &result)
	if err != nil {
		return result, fmt.Errorf("JSON parse failed")
	}
	return result, nil
}

func normalizeEsQuery(str string) string {
	return strings.Replace(str, `"`, `\"`, -1)
}

func main() {
	kingpin.Version(ver)
	kingpin.Parse()

	check := nagiosplugin.NewCheck()
	defer check.Finish()

	if *compareOperator != "lt" && *compareOperator != "gt" {
		check.AddResult(nagiosplugin.UNKNOWN, "compare-operator parameter should be 'lt' or 'gt'")
		return
	}

	if *countThreshold == 0 {
		check.AddResult(nagiosplugin.UNKNOWN, "threshold cannot be equal to 0")
		return
	}

	c := make(chan Msg)
	go getQueryResultCount(
		*esURL,
		*indexPattern,
		templateSource,
		normalizeEsQuery(*esQuery),
		time.Now().Unix() - int64(60) * int64(*timePeriod),
		c,
	)

	var msg Msg

	select {
	case msg = <-c:
		if msg.Err == nil {
			perc := float64(msg.Count) / float64(*countThreshold) * 100
			if (*compareOperator == "gt" && msg.Count >= *countThreshold) || (*compareOperator == "lt" && msg.Count <= *countThreshold) {
				check.AddResult(nagiosplugin.OK, fmt.Sprintf("%d entries of '%s' (%.2f%%) found in the past %d minutes", msg.Count, *esQuery, perc, *timePeriod))
			} else if (*compareOperator == "gt" && msg.Count < *countThreshold) || (*compareOperator == "lt" && msg.Count > *countThreshold) {
				check.AddResult(nagiosplugin.CRITICAL, fmt.Sprintf("%d entries of '%s' (%.2f%%) found in the past %d minutes", msg.Count, *esQuery, perc, *timePeriod))
			}
		} else {
			check.AddResult(nagiosplugin.UNKNOWN, fmt.Sprintf("%v", msg.Err))
		}
	case <-time.After(time.Second * time.Duration(*timeout)):
		check.AddResult(nagiosplugin.UNKNOWN, "connection timeout")
	}
}
