package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"html"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
)

type sample struct {
	scenario       string
	implementation string
	x              float64
	xLabel         string
	throughput     float64
	p99            float64
	duplicates     float64
}

type aggregate struct {
	count                       int
	throughput, p99, duplicates float64
}

func main() {
	input := flag.String("in", "bench-results.csv", "benchmark CSV input")
	output := flag.String("out", "bench-results.svg", "SVG output")
	xColumn := flag.String("x", "concurrency", "numeric CSV column used for the x-axis")
	title := flag.String("title", "Durable RPC benchmark comparison", "chart title")
	flag.Parse()
	file, err := os.Open(*input)
	if err != nil {
		log.Fatal(err)
	}
	samples, err := readSamples(file, *xColumn)
	file.Close()
	if err != nil {
		log.Fatal(err)
	}
	if len(samples) == 0 {
		log.Fatal("input contains no benchmark rows")
	}
	out, err := os.Create(*output)
	if err != nil {
		log.Fatal(err)
	}
	if err := renderSVG(out, samples, *xColumn, *title); err != nil {
		out.Close()
		log.Fatal(err)
	}
	if err := out.Close(); err != nil {
		log.Fatal(err)
	}
	log.Printf("wrote %s", *output)
}

func readSamples(reader io.Reader, xColumn string) ([]sample, error) {
	rows, err := csv.NewReader(reader).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("read CSV: %w", err)
	}
	if len(rows) < 2 {
		return nil, nil
	}
	columns := make(map[string]int)
	for index, name := range rows[0] {
		columns[name] = index
	}
	required := []string{"scenario", "implementation", xColumn, "throughput_ops_s", "p99_ms", "duplicate_effects"}
	for _, name := range required {
		if _, ok := columns[name]; !ok {
			return nil, fmt.Errorf("CSV is missing %q", name)
		}
	}
	var samples []sample
	for line, row := range rows[1:] {
		// Accept files formed by concatenating complete CSV outputs.
		if row[columns["implementation"]] == "implementation" {
			continue
		}
		x, err := strconv.ParseFloat(row[columns[xColumn]], 64)
		if err != nil {
			return nil, fmt.Errorf("line %d %s: %w", line+2, xColumn, err)
		}
		throughput, err := strconv.ParseFloat(row[columns["throughput_ops_s"]], 64)
		if err != nil {
			return nil, fmt.Errorf("line %d throughput: %w", line+2, err)
		}
		p99, err := strconv.ParseFloat(row[columns["p99_ms"]], 64)
		if err != nil {
			return nil, fmt.Errorf("line %d p99: %w", line+2, err)
		}
		duplicates, err := strconv.ParseFloat(row[columns["duplicate_effects"]], 64)
		if err != nil {
			return nil, fmt.Errorf("line %d duplicate effects: %w", line+2, err)
		}
		samples = append(samples, sample{
			scenario:       row[columns["scenario"]],
			implementation: row[columns["implementation"]],
			x:              x,
			xLabel:         strconv.FormatFloat(x, 'g', -1, 64),
			throughput:     throughput,
			p99:            p99,
			duplicates:     duplicates,
		})
	}
	return samples, nil
}

func renderSVG(writer io.Writer, samples []sample, xName, chartTitle string) error {
	type groupKey struct {
		scenario string
		x        float64
		xLabel   string
	}
	groups := make(map[groupKey]map[string]*aggregate)
	for _, value := range samples {
		key := groupKey{value.scenario, value.x, value.xLabel}
		if groups[key] == nil {
			groups[key] = make(map[string]*aggregate)
		}
		current := groups[key][value.implementation]
		if current == nil {
			current = &aggregate{}
			groups[key][value.implementation] = current
		}
		current.count++
		current.throughput += value.throughput
		current.p99 += value.p99
		current.duplicates += value.duplicates
	}
	keys := make([]groupKey, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].scenario == keys[j].scenario {
			return keys[i].x < keys[j].x
		}
		return keys[i].scenario < keys[j].scenario
	})
	width := 240 + len(keys)*110
	if width < 900 {
		width = 900
	}
	const height = 850
	if _, err := fmt.Fprintf(writer, `<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" viewBox="0 0 %d %d">
<rect width="100%%" height="100%%" fill="#ffffff"/>
<style>text{font-family:system-ui,sans-serif;fill:#172033}.title{font-size:22px;font-weight:700}.axis{font-size:12px}.label{font-size:11px}.grid{stroke:#d9deea;stroke-width:1}.protected{fill:#2563eb}.baseline{fill:#e05252}</style>
<text x="70" y="35" class="title">%s</text>
<rect x="%d" y="18" width="14" height="14" class="protected"/><text x="%d" y="30" class="axis">protected</text>
<rect x="%d" y="18" width="14" height="14" class="baseline"/><text x="%d" y="30" class="axis">baseline</text>
`, width, height, width, height, html.EscapeString(chartTitle), width-280, width-260, width-160, width-140); err != nil {
		return err
	}
	panels := []struct {
		title string
		unit  string
		value func(*aggregate) float64
	}{
		{"Throughput", "ops/s", func(a *aggregate) float64 { return a.throughput / float64(a.count) }},
		{"p99 logical-request latency", "ms", func(a *aggregate) float64 { return a.p99 / float64(a.count) }},
		{"Duplicate side effects", "extra counter changes", func(a *aggregate) float64 { return a.duplicates / float64(a.count) }},
	}
	for panelIndex, panel := range panels {
		top := 75 + panelIndex*250
		plotTop, plotBottom := top+30, top+190
		maxValue := 0.0
		for _, key := range keys {
			for _, implementation := range []string{"protected", "baseline"} {
				if value := groups[key][implementation]; value != nil && panel.value(value) > maxValue {
					maxValue = panel.value(value)
				}
			}
		}
		if maxValue == 0 {
			maxValue = 1
		}
		fmt.Fprintf(writer, `<text x="70" y="%d" class="title">%s <tspan class="axis">(%s)</tspan></text>
<line x1="70" y1="%d" x2="%d" y2="%d" class="grid"/><line x1="70" y1="%d" x2="%d" y2="%d" class="grid"/>
<text x="62" y="%d" text-anchor="end" class="axis">%.2f</text><text x="62" y="%d" text-anchor="end" class="axis">0</text>
`, top+15, html.EscapeString(panel.title), html.EscapeString(panel.unit), plotTop, width-35, plotTop, plotBottom, width-35, plotBottom, plotTop+4, maxValue, plotBottom+4)
		for index, key := range keys {
			center := 115 + index*110
			for implementationIndex, implementation := range []string{"protected", "baseline"} {
				value := groups[key][implementation]
				if value == nil {
					continue
				}
				height := panel.value(value) / maxValue * float64(plotBottom-plotTop)
				x := center - 24 + implementationIndex*28
				y := float64(plotBottom) - height
				fmt.Fprintf(writer, `<rect x="%d" y="%.2f" width="22" height="%.2f" class="%s"><title>%s %s=%s %s: %.3f</title></rect>
`, x, y, height, implementation, html.EscapeString(key.scenario), html.EscapeString(xName), html.EscapeString(key.xLabel), implementation, panel.value(value))
			}
			fmt.Fprintf(writer, `<text x="%d" y="%d" text-anchor="middle" class="label">%s</text><text x="%d" y="%d" text-anchor="middle" class="label">%s=%s</text>
`, center, plotBottom+15, html.EscapeString(key.scenario), center, plotBottom+29, html.EscapeString(xName), html.EscapeString(key.xLabel))
		}
	}
	_, err := io.WriteString(writer, "</svg>\n")
	return err
}
