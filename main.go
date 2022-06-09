package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"reflect"
	"strconv"

	"github.com/olivere/elastic/v7"
	"github.com/pborman/uuid"
)

type Location struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

type Post struct {
	User     string   `json:"user"`
	Message  string   `json:"message"`
	Location Location `json:"location"`
}

const (
	INDEX    = "around"
	TYPE     = "post"
	DISTANCE = "200km"
	ES_URL   = "http://34.125.206.199:9200"
)

func main() {
	// Create a client
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		panic(err)
	}

	// Use the IndexExists service to check if a specified index exists.
	exists, err := client.IndexExists(INDEX).Do(context.Background())
	if err != nil {
		panic(err)
	}
	if !exists {
		// Create a new index.
		mapping := `{
				"mappings":{
					
					
						"properties":{
							"location":{
								"type":"geo_point"
							}
						}
					
					
				}
			}`
		_, err := client.CreateIndex(INDEX).Body(mapping).Do(context.Background())
		if err != nil {
			// Handle error
			panic(err)
		}
	}
	fmt.Println("Start service")
	http.HandleFunc("/post", handlePost)
	http.HandleFunc("/search", handleSearch)
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func handlePost(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received one post request.")

	decoder := json.NewDecoder(r.Body)
	var p Post
	if err := decoder.Decode(&p); err != nil {
		panic(err)
	}

	fmt.Fprintf(w, "Post received: %s\n", p.Message)

	id := uuid.New()
	saveToES(&p, id)
}

func saveToES(p *Post, id string) {
	es_client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		panic(err)
	}

	_, err = es_client.Index().Index(INDEX).Id(id).BodyJson(p).Refresh("true").Do(context.Background())
	if err != nil {
		panic(err)
	}

	fmt.Printf("Post is saved to index : %s\n", p.Message)
}

func handleSearch(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received one request for search")

	lat, err := strconv.ParseFloat(r.URL.Query().Get("lat"), 64)
	if err != nil {
		panic(err)
	}

	fmt.Println("first")

	lon, err := strconv.ParseFloat(r.URL.Query().Get("lon"), 64)
	if err != nil {
		panic(err)
	}
	fmt.Println("second")
	ran := DISTANCE
	if val := r.URL.Query().Get("range"); val != "" {
		ran = val + "km"
	}

	//create client
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		panic(err)
	}

	q := elastic.NewGeoDistanceQuery("location")
	q = q.Point(lat, lon).Distance(ran)
	// termQuery := elastic.NewTermQuery("user", "1111")
	fmt.Println("third")
	searchResult, err := client.Search().Index(INDEX).Query(q).Pretty(true).Do(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Println("fourth")
	fmt.Printf("Query took %d milliseconds\n", searchResult.TookInMillis)
	fmt.Printf("Found total of %d posts\n", searchResult.TotalHits())

	var tye Post
	var ps []Post
	for _, item := range searchResult.Each(reflect.TypeOf(tye)) {
		p := item.(Post)
		fmt.Printf("Post by %s: %s at lat %v and lon: %v\n", p.User, p.Message, p.Location.Lat, p.Location.Lon)
		ps = append(ps, p)
	}

	js, err := json.Marshal(ps)
	if err != nil {
		panic(err)
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Write(js)

	fmt.Fprintf(w, "Search received: %f %f %s", lat, lon, ran)
}
