package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"

	_ "net/http/pprof"

	"github.com/elastic/go-elasticsearch/v7"
)

func main() {
	startTime := time.Now()
	// Основной код
	cfg := elasticsearch.Config{
		Addresses: []string{
			"http://ipev2-phd-access-es-01p.data.corp:9200/",
		},
	}

	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("Error creating the Elasticsearch client: %s", err)
	}

	query := map[string]interface{}{
		"size": 10000, // Количество записей, которое мы хотим получить (допустим, нужно еще 1000)
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []map[string]interface{}{
					{
						"match": map[string]interface{}{
							"metadata.actual": "true",
						},
					},
					{
						"match": map[string]interface{}{
							"metadata.recordType": "right_record",
						},
					},
				},
			},
		},
	}

	scroll := "2m"
	scrollDuration, err := time.ParseDuration(scroll)
	if err != nil {
		log.Fatalf("Error parsing scroll duration: %s", err)
	}

	var wg sync.WaitGroup
	resultChan := make(chan int, 10)
	totalSize := 0
	count := 0
	elapsedTime := time.Since(startTime)
	go func() {
		for size := range resultChan {
			totalSize += size
			count += 1
			elapsedTime = time.Since(startTime)
			if count%10000 == 0 {
				print("Обработано: ", count, "\n", "Вес составляет: ", totalSize, "\n")
				fmt.Printf("Время работы программы: %s\n\n", elapsedTime)
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := performScrollSearch(es, query, scrollDuration, resultChan)
		if err != nil {
			log.Fatalf("Error during scroll search: %s", err)
		}
	}()

	wg.Wait()
	close(resultChan)

	// Преобразование totalSize в гигабайты (GB)
	gb := float64(totalSize) / (1024 * 1024 * 1024)

	// Печать с двумя знаками после запятой
	fmt.Printf("right_record\nВес записей: %d bytes (%.2f GB)\nКоличество записей = %d\n", totalSize, gb, count)

	elapsedTime = time.Since(startTime)
	fmt.Printf("Время работы программы: %s\n", elapsedTime)
}

func performScrollSearch(es *elasticsearch.Client, query map[string]interface{}, scroll time.Duration, resultChan chan<- int) error {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return fmt.Errorf("error encoding query: %s", err)
	}

	res, err := es.Search(
		es.Search.WithContext(context.Background()),
		es.Search.WithIndex("registry_records_search_alias"),
		es.Search.WithBody(&buf),
		es.Search.WithScroll(scroll),
	)
	if err != nil {
		return fmt.Errorf("error initiating scroll search: %s", err)
	}
	defer res.Body.Close()

	for {
		if res.StatusCode != 200 {
			return fmt.Errorf("error in search response: %s", res.Status())
		}

		var r map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
			return fmt.Errorf("error parsing response body: %s", err)
		}

		hits := r["hits"].(map[string]interface{})["hits"].([]interface{})
		if len(hits) == 0 {
			break
		}

		for _, hit := range hits {
			doc := hit.(map[string]interface{})
			source := doc["_source"].(map[string]interface{})
			content := source["content"].(string)
			contentSize := len(content)
			resultChan <- contentSize
		}

		runtime.GC()

		scrollID := r["_scroll_id"].(string)

		res, err = es.Scroll(
			es.Scroll.WithContext(context.Background()),
			es.Scroll.WithScrollID(scrollID),
			es.Scroll.WithScroll(scroll),
		)
		if err != nil {
			return fmt.Errorf("error continuing scroll search: %s", err)
		}
	}

	return nil
}
