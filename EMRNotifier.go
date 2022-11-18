package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/emrserverless"
	_ "github.com/lib/pq"
	log "github.com/sirupsen/logrus"
)

type JobDetail struct {
	Id          string `json:"id"`
	JobId       string `json:"jobId"`
	JobStatus   string `json:"jobStatus"`
	RequestId   string `json:"requestId"`
	Query       string `json:"query"`
	Destination string `json:"destination"`
	Jti         string `json:"jti"`
	Region      string `json:"cross_bucket_region"`
	ClientIp    string `json:"client_ip"`
}

var db *sql.DB
var logger *log.Entry
var source string
var service *emrserverless.EMRServerless

func init() {
	log.SetFormatter(&log.JSONFormatter{})

	host := os.Getenv("DB_HOST")
	port := os.Getenv("DB_PORT")
	user := os.Getenv("DB_USER")
	password := os.Getenv("DB_PASSWORD")
	databaseName := os.Getenv("DB_NAME")
	region := os.Getenv("REGION")

	connection := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host,
		port,
		user,
		password,
		databaseName,
	)
	db, _ = sql.Open("postgres", connection)

	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String(region),
	})
	service = emrserverless.New(sess)

	source = "EMRNotifier"
}

func main() {
	lambda.Start(HandleEvent)
}

func HandleEvent(event events.CloudWatchEvent) {
	if event.Source == "aws.emr-serverless" {

		logger = log.WithFields(log.Fields{
			"source": source,
		})
		logger.Info("Initiating EMR Notifier")

		var eventContext map[string]interface{}
		if err := json.Unmarshal(event.Detail, &eventContext); err != nil {
			logger.Error(fmt.Sprintf("Failed to serializer data with error: %v", err.Error()))
			return
		}
		jobId := fmt.Sprint(eventContext["jobRunId"])
		applicationId := fmt.Sprint(eventContext["applicationId"])
		updatedJobStatus := fmt.Sprint(eventContext["state"])

		jobDetail, err := GetJobDetail(jobId)
		if err != nil {
			logger.Error(fmt.Sprintf("Failed to get job details for jobId: %v with error: %v", jobId, err.Error()))
			return
		}
		if updatedJobStatus == "SUCCESS" {
			updatedJobStatus = "DATA_TRANSFER"
		}

		logger = logger.WithFields(log.Fields{
			"queryId":           jobDetail.Id,
			"jti":               jobDetail.Jti,
			"clientIp":          jobDetail.ClientIp,
			"skyflowRequestId":  jobDetail.RequestId,
			"region":            jobDetail.Region,
			"query":             jobDetail.Query,
			"destinationBucket": jobDetail.Destination,
		})

		UpdateJob(jobDetail, updatedJobStatus)

		if updatedJobStatus == "FAILED" {
			params := &emrserverless.GetJobRunInput{
				ApplicationId: &applicationId,
				JobRunId:      &jobId,
			}

			jobRunOutput, err := service.GetJobRun(params)
			if err != nil {
				logger.Error(fmt.Sprintf("Failed to get EMR Job details with error: %v", err.Error()))
				return
			}

			logger.Error(fmt.Sprintf("EMR job failed with details: %v", jobRunOutput))
		}
	}
}

func GetJobDetail(jobId string) (JobDetail, error) {
	logger.Info(fmt.Sprintf("Initiating GetJobDetail with jobId:%v", jobId))

	statement := `SELECT id, jobid, jobstatus, requestid, query, destination, jti, cross_bucket_region, client_ip FROM emr_job_details WHERE jobid=$1`
	var jobDetail JobDetail

	record := db.QueryRow(statement, jobId)

	switch err := record.Scan(
		&jobDetail.Id,
		&jobDetail.JobId,
		&jobDetail.JobStatus,
		&jobDetail.RequestId,
		&jobDetail.Query,
		&jobDetail.Destination,
		&jobDetail.Jti,
		&jobDetail.Region,
		&jobDetail.ClientIp,
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
	logger.Info("Initiating UpdateJob")

	statement := `UPDATE emr_job_details SET jobstatus = $1 WHERE jobid = $2;`

	logger.Info(fmt.Sprintf("Updating record for jobId: %v", jobDetail.JobId))

	_, err := db.Exec(statement, updatedJobStatus, jobDetail.JobId)

	if err != nil {
		logger.Error(fmt.Sprintf("Failed to update record for jobId: %v with error: %v", jobDetail.JobId, err.Error()))
		return
	}

	logger.Info(fmt.Sprintf("Successfully Updated jobStatus: %v for jobId: %v", updatedJobStatus, jobDetail.JobId))
}
