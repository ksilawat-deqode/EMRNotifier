package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	_ "github.com/lib/pq"
)

type JobDetail struct {
	Id          string `json:"id"`
	JobId       string `json:"jobId"`
	JobStatus   string `json:"jobStatus"`
	RequestId   string `json:"requestId"`
	Query       string `json:"query"`
	Destination string `json:"destination"`
}

var db *sql.DB

func init() {
	host := os.Getenv("DB_HOST")
	port := os.Getenv("DB_PORT")
	user := os.Getenv("DB_USER")
	password := os.Getenv("DB_PASSWORD")
	databaseName := os.Getenv("DB_NAME")

	connection := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host,
		port,
		user,
		password,
		databaseName,
	)
	db, _ = sql.Open("postgres", connection)
}

func main() {
	lambda.Start(HandleEvent)
}

func GetJobDetail(jobId string) (JobDetail, error) {
	log.Printf("Initiating GetJobDetail with jobId:%v", jobId)

	statement := `SELECT id, jobid, jobstatus, requestid, query, destination FROM emr_job_details WHERE jobid=$1`
	var jobDetail JobDetail

	record := db.QueryRow(statement, jobId)

	switch err := record.Scan(
		&jobDetail.Id,
		&jobDetail.JobId,
		&jobDetail.JobStatus,
		&jobDetail.RequestId,
		&jobDetail.Query,
		&jobDetail.Destination,
	); err {
	case sql.ErrNoRows:
		return jobDetail, sql.ErrNoRows
	case nil:
		return jobDetail, nil
	default:
		return jobDetail, err
	}
}

func UpdateJob(jobDetail JobDetail, updatedJobStatus string) {
	log.Printf("%v-> Initiating UpdateJob", jobDetail.Id)

	statement := `UPDATE emr_job_details SET jobstatus = $1 WHERE jobid = $2;`

	log.Printf("%v-> Updating record for jobId: %v\n", jobDetail.Id, jobDetail.JobId)

	_, err := db.Exec(statement, updatedJobStatus, jobDetail.JobId)

	if err != nil {
		log.Printf("%v-> Failed to update record for jobId: %v with error: %v\n", jobDetail.Id, jobDetail.JobId, err.Error())
		return
	}

	log.Printf("%v-> Successfully Updated jobStatus: %v for jobId: %v\n", jobDetail.Id, updatedJobStatus, jobDetail.JobId)
}

func HandleEvent(event events.CloudWatchEvent) {
	if event.Source == "aws.emr-serverless" {

		log.Printf("Initiating EMR Notifier\n")

		var eventContext map[string]interface{}
		if err := json.Unmarshal(event.Detail, &eventContext); err != nil {
			log.Printf("Failed to serializer data with error: %v\n", err.Error())
			return
		}
		jobId := fmt.Sprint(eventContext["jobRunId"])
		updatedJobStatus := fmt.Sprint(eventContext["state"])

		jobDetail, err := GetJobDetail(jobId)
		if err != nil {
			log.Printf("Failed to get job details for jobId: %v with error: %v\n", jobId, err.Error())
			return
		}
		if updatedJobStatus == "SUCCESS"{
			updatedJobStatus = "DATA_TRANSFER"
		}

		UpdateJob(jobDetail, updatedJobStatus)
	}
}
