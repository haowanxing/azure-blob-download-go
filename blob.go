package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

var (
	accountName   string
	containerName string
	accountKey    string
)

// Azure Storage Quickstart Sample - Demonstrate how to upload, list, download, and delete blobs.
//
// Documentation References:
// - What is a Storage Account - https://docs.microsoft.com/azure/storage/common/storage-create-storage-account
// - Blob Service Concepts - https://docs.microsoft.com/rest/api/storageservices/Blob-Service-Concepts
// - Blob Service Go SDK API - https://godoc.org/github.com/Azure/azure-storage-blob-go
// - Blob Service REST API - https://docs.microsoft.com/rest/api/storageservices/Blob-Service-REST-API
// - Scalability and performance targets - https://docs.microsoft.com/azure/storage/common/storage-scalability-targets
// - Azure Storage Performance and Scalability checklist https://docs.microsoft.com/azure/storage/common/storage-performance-checklist
// - Storage Emulator - https://docs.microsoft.com/azure/storage/common/storage-use-emulator

func randomString() string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return strconv.Itoa(r.Int())
}

func handleErrors(err error) {
	if err != nil {
		if serr, ok := err.(azblob.StorageError); ok { // This error is a Service-specific
			switch serr.ServiceCode() { // Compare serviceCode to ServiceCodeXxx constants
			case azblob.ServiceCodeContainerAlreadyExists:
				fmt.Println("Received 409. Container already exists")
				return
			}
		}
		log.Fatal(err)
	}
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	c := make(chan bool, 32)

	fmt.Printf("####Azure Blob Storage Quick Download Tool####\n")

	// From the Azure portal, get your storage account name and key and set environment variables.
	accountName, accountKey = os.Getenv("AZURE_STORAGE_ACCOUNT"), os.Getenv("AZURE_STORAGE_ACCESS_KEY")
	if len(accountName) == 0 || len(accountKey) == 0 {
		tip := `
Either the AZURE_STORAGE_ACCOUNT or AZURE_STORAGE_ACCESS_KEY environment variable is not set
### Environment variable to set

export AZURE_STORAGE_ACCOUNT="<youraccountname>"

export AZURE_STORAGE_ACCESS_KEY="<youraccountkey>"`
		fmt.Println(tip)
		os.Exit(1)
	}

	// Create a default request pipeline using your storage account name and account key.
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		log.Fatal("Invalid credentials with error: " + err.Error())
	}
	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	// Create a random string for the quick start container
	//containerName := fmt.Sprintf("quickstart-%s", randomString())
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Please input the blobName: ")
	input, _, _ := reader.ReadLine()
	containerName = fmt.Sprintf("%s", input)
	fmt.Printf("You choosed the blob: %#v \n", containerName)

	fmt.Print("Please input the localDir to save: ")
	input, _, _ = reader.ReadLine()
	localDir := fmt.Sprintf("%s", input)
	fmt.Printf("%#v has been choosed \n", localDir)
	// From the Azure portal, get your storage account blob service URL endpoint.
	URL, _ := url.Parse(
		fmt.Sprintf("https://%s.blob.core.chinacloudapi.cn/%s", accountName, containerName))

	// Create a ContainerURL object that wraps the container URL and a request
	// pipeline to make requests.
	containerURL := azblob.NewContainerURL(*URL, p)

	ctx := context.Background() // This example uses a never-expiring context

	fmt.Printf("Please choose one func: \n1.save object to local.\n2.save URL list to local.\ninput:")
	input, _, _ = reader.ReadLine()
	if string(input) == "2" {
		os.MkdirAll(localDir, 0777)
		path := filepath.Join(localDir, "bloblist.txt")
		file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0766)
		if err != nil {
			log.Fatalln(err)
		}
		SaveList(ctx, containerURL, file)
		log.Printf("save URL list to %s finished.", path)
		os.Exit(0)
	}

	// List the container that we have created above
	fmt.Println("Listing the blobs in the container:")

	count := 0
	for marker := (azblob.Marker{}); marker.NotDone(); {
		// Get a result segment starting with the blob indicated by the current Marker.
		listBlob, err := containerURL.ListBlobsFlatSegment(ctx, marker, azblob.ListBlobsSegmentOptions{})
		handleErrors(err)

		// ListBlobs returns the start of the next segment; you MUST use this to get
		// the next segment (after processing the current result segment).
		marker = listBlob.NextMarker

		// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
		count += len(listBlob.Segment.BlobItems)
		for _, blobInfo := range listBlob.Segment.BlobItems {
			c <- true
			go func(bName string, c chan bool) {
				outTemplate := "	Blob name: %s - %s"
				uRl, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.chinacloudapi.cn/%s/%s", accountName, containerName, bName))
				fileRealPath := localDir + "/" + bName
				pathTmp := filepath.Dir(fileRealPath)
				os.MkdirAll(pathTmp, 0777)
				f, err := os.OpenFile(fileRealPath, os.O_CREATE|os.O_RDWR, 0766)
				if err != nil {
					fmt.Println("open file err: ", err)
					fmt.Println(fmt.Sprintf(outTemplate, bName, "failed"))
				} else {
					err = azblob.DownloadBlobToFile(ctx, azblob.NewBlobURL(*uRl, p), 0, azblob.CountToEnd, f, azblob.DownloadFromBlobOptions{})
					if err != nil {
						fmt.Println("downloading err", err)
						fmt.Println(fmt.Sprintf(outTemplate, bName, "failed"))
					} else {
						fmt.Println(fmt.Sprintf(outTemplate, bName, "success"))
					}
					f.Close()
				}
				<-c
			}(blobInfo.Name, c)
		}
		fmt.Println("Now search items count to: ", count)
	}
}

func SaveList(ctx context.Context, containerURL azblob.ContainerURL, writeTo io.Writer) {
	for marker := (azblob.Marker{}); marker.NotDone(); {
		// Get a result segment starting with the blob indicated by the current Marker.
		listBlob, err := containerURL.ListBlobsFlatSegment(ctx, marker, azblob.ListBlobsSegmentOptions{})
		handleErrors(err)

		// ListBlobs returns the start of the next segment; you MUST use this to get
		// the next segment (after processing the current result segment).
		marker = listBlob.NextMarker

		// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, blobInfo := range listBlob.Segment.BlobItems {
			url := fmt.Sprintf("https://%s.blob.core.chinacloudapi.cn/%s/%s", accountName, containerName, blobInfo.Name)
			n, err := writeTo.Write([]byte(url + "\n"))
			log.Println(url, n, err)
		}
	}
}
