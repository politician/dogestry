package remote

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/MSOpenTech/azure-sdk-for-go/storage"
	"dogestry/config"
	"dogestry/utils"
	docker "github.com/fsouza/go-dockerclient"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func NewAzureRemote(config config.Config) (*AzureRemote, error) {
	return &AzureRemote{config: config}, nil
}

type AzureRemote struct {
	config config.Config
}

func (remote *AzureRemote) Push(image, imageRoot string) error {
	var err error

	keysToPush, err := remote.localKeys(imageRoot)
	if err != nil {
		return fmt.Errorf("error calculating keys to push: %v", err)
	}

	if len(keysToPush) == 0 {
		log.Println("There are no files to push")
		return nil
	}

	type putFileResult struct {
		host string
		err  error
	}

	putFileErrChan := make(chan putFileResult)
	putFilesChan := remote.makeAzFilesChan(keysToPush)

	defer close(putFileErrChan)

	numGoroutines := 25
	goroutineQuitChans := make([]chan bool, numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		goroutineQuitChans[i] = make(chan bool)
	}

	println("Pushing files to Azure remote:")
	for i := 0; i < numGoroutines; i++ {
		go func(i int) {
			select {
			case <-goroutineQuitChans[i]:
				return
			default:
				for putFile := range putFilesChan {
					putFileErr := remote.putFile(putFile.KeyDef.fullPath, &putFile.KeyDef)

					if (putFileErr != nil) && ((putFileErr != io.EOF) && (!strings.Contains(putFileErr.Error(), "EOF"))) {
						putFileErrChan <- putFileResult{putFile.Key, putFileErr}
						return
					}

					putFileErrChan <- putFileResult{}
				}
			}
		}(i)
	}

	for i := 0; i < len(keysToPush); i++ {
		p := <-putFileErrChan
		if p.err != nil {
			// Close all running goroutines
			for i := 0; i < numGoroutines; i++ {
				select {
				case goroutineQuitChans[i] <- true:
				default:
				}
			}

			log.Printf("error when uploading to Azure: %v", p.err)
			return fmt.Errorf("Error when uploading to Azure: %v", p.err)
		}
	}

	return nil
}

// pull a single image from the remote
func (remote *AzureRemote) PullImageId(id ID, dst string) error {
	rootKey := "images/" + string(id)
	imageKeys, err := remote.repoKeys("/" + rootKey)
	if err != nil {
		return err
	}

	return remote.getFiles(dst, rootKey, imageKeys)
}

// map repo:tag to id (like git rev-parse)
func (remote *AzureRemote) ParseTag(repo, tag string) (ID, error) {
	svc, err := remote.azureBlobClient()
	if err != nil {
		return "", err
	}

	path := remote.tagFilePath(repo, tag)

	exists, err := svc.BlobExists(remote.config.Azure.Blob.Container, path)
	if err != nil {
		return "", err
	}

	if exists {
		// Read the ID from the blob
		s, err := remote.getAsString(svc, remote.config.Azure.Blob.Container, path)
		if err != nil && err != io.EOF {
			return "", err
		}

		return ID(s), nil
	}

	return "", nil
}

// map a ref-like to id. "ref-like" could be a ref or an id.
func (remote *AzureRemote) ResolveImageNameToId(image string) (ID, error) {
	return ResolveImageNameToId(remote, image)
}

func (remote *AzureRemote) ImageFullId(id ID) (ID, error) {
	remoteKeys, err := remote.repoKeys("/images")
	if err != nil {
		return "", err
	}

	for key, _ := range remoteKeys {
		key = strings.TrimPrefix(key, "images/")
		parts := strings.Split(key, "/")
		if strings.HasPrefix(parts[0], string(id)) {
			return ID(parts[0]), nil
		}
	}

	return "", ErrNoSuchImage
}

// Download the json file at images/{id}/json
func (remote *AzureRemote) ImageMetadata(id ID) (docker.Image, error) {
	blob := remote.config.Azure.Blob

	path := filepath.Join("images", string(id), "json")

	if blob.PathPresent {
		path = filepath.Join(blob.Path, path)
	}

	image := docker.Image{}

	svc, err := remote.azureBlobClient()
	if err != nil {
		return image, err
	}

	s, err := remote.getAsString(svc, blob.Container, path)
	if err != nil {
		return image, err
	}

	if err := json.Unmarshal([]byte(s), &image); err != nil {
		return image, err
	}

	return image, nil
}

// return repo, tag from a file path (or S3 key)
func (remote *AzureRemote) ParseImagePath(path string, prefix string) (repo, tag string) {
	if remote.config.Azure.Blob.PathPresent {
		prefix = filepath.Join(remote.config.Azure.Blob.Path, prefix)
	}

	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	return ParseImagePath(path, prefix)
}

// walk the image history on the remote, starting at id
func (remote *AzureRemote) WalkImages(id ID, walker ImageWalkFn) error {
	return WalkImages(remote, id, walker)
}

// checks the config and connectivity of the remote
func (remote *AzureRemote) Validate() error {
	_, err := remote.azureBlobClient()

	if err != nil {
		return err
	}

	return nil
}

// describe the remote
func (remote *AzureRemote) Desc() string {
	return fmt.Sprintf("Azure blob storage(%s)", remote.config.Azure.Blob.String())
}

// List images on the remote
func (remote *AzureRemote) List() (images []Image, err error) {
	keys, err := remote.repoKeys("repositories")

	if err != nil {
		return nil, err
	}

	for _, v := range keys {
		repo, tag := remote.ParseImagePath(v.remotePath, "repositories/")
		if err != nil {
			log.Printf("error splitting Azure key: repositories/")
			return images, err
		}

		image := Image{repo, tag}
		images = append(images, image)
	}

	return images, nil
}

type azKeyDef struct {
	key    string
	sumKey string

	sum      string
	fullPath string

	remotePath string

	remote *AzureRemote
}

// keys represents either local or remote files
type azKeys map[string]*azKeyDef

// gets a key, creating the underlying keyDef if required
// we need to S3Remote for getting the sum, so add it here
func (k azKeys) Get(key string, remote *AzureRemote) *azKeyDef {
	if existing, ok := k[key]; ok {
		return existing
	} else {
		k[key] = &azKeyDef{key: key, remote: remote}
	}

	return k[key]
}

type azPutFileTuple struct {
	Key    string
	KeyDef azKeyDef
}

func (remote *AzureRemote) makeAzFilesChan(keysToPush azKeys) <-chan azPutFileTuple {
	putFilesChan := make(chan azPutFileTuple, len(keysToPush))
	go func() {
		defer close(putFilesChan)
		for key, localKey := range keysToPush {
			keyDefClone := *localKey
			putFilesChan <- azPutFileTuple{key, keyDefClone}
		}
	}()
	return putFilesChan
}

func (remote *AzureRemote) azureBlobClient() (*storage.BlobStorageClient, error) {
	acctName := remote.config.Azure.AccountName
	acctKey := remote.config.Azure.AccountKey

	client, err := storage.NewBasicClient(acctName, acctKey)

	if err != nil {
		return nil, err
	}

	svc := client.GetBlobService()

	return &svc, nil
}

func (remote *AzureRemote) getAsString(service *storage.BlobStorageClient, container, path string) (string, error) {
	f, err := service.GetBlob(container, path)
	if err != nil {
		return "", err
	}

	defer f.Close()

	buf := bufio.NewReader(f)

	// Read until null terminator
	b, err := buf.ReadBytes(0)
	if err != nil && err != io.EOF {
		return "", err
	}

	if err == io.EOF {
		return string(b), nil
	}

	// Don't return the null terminator
	return string(b[:len(b)-1]), nil
}

// Get repository keys from the local work dir.
// Returned as a map of azKeyDef's for ease of comparison.
func (remote *AzureRemote) localKeys(root string) (azKeys, error) {
	localKeys := make(azKeys)

	if root[len(root)-1] != '/' {
		root = root + "/"
	}

	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}

		sum, err := utils.Sha1File(path)
		if err != nil {
			return err
		}

		key := strings.TrimPrefix(path, root)

		// note that we pre-populate the sum here
		localKeys[key] = &azKeyDef{
			key:      key,
			sum:      sum,
			fullPath: path,
		}

		return nil
	})

	if err != nil {
		return localKeys, err
	}

	return localKeys, nil
}

// put a file with key from imageRoot to the s3 bucket
func (remote *AzureRemote) putFile(src string, key *azKeyDef) error {
	dstKey := key.key

	blob := remote.config.Azure.Blob

	if blob.PathPresent {
		dstKey = fmt.Sprintf("%s/%s", blob.Path, dstKey)
	}

	f, err := os.Open(src)
	if err != nil {
		return err
	}

	defer f.Close()

	service, err := remote.azureBlobClient()
	if err != nil {
		return err
	}

	// Create the block, if it doesn't exist
	// Copy to Azure Blob Storage
	blocks, err := remote.putAzureBlocks(service, f, blob, dstKey)
	if err != nil {
		return err
	}

	if len(blocks) > 0 {
		err = service.PutBlockList(blob.Container, dstKey, blocks)
		if err != nil {
			return err
		}
	}

	return nil
}

const maxBlockSize int64 = 4000000

func (remote *AzureRemote) putAzureBlocks(svc *storage.BlobStorageClient, f *os.File, blob *config.BlobSpec, dst string) ([]storage.Block, error) {
	arr := make([]byte, maxBlockSize)

	firstId, nBlocks := remote.firstBlockId(f)
	id := firstId
	blocks := make([]storage.Block, nBlocks)

	var err error = nil

	for n, e := f.Read(arr); n > 0 && (e == nil || e == io.EOF); n, e = f.Read(arr) {
		strId := base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(id)))

		putErr := svc.PutBlock(blob.Container, dst, strId, arr[:n])

		if putErr != nil {
			// We could re-try the block under certain conditions
			// Note that, because we haven't committed any blocks,
			// we don't have to worry about partial uploads
			return nil, putErr
		}

		blocks[id-firstId] = storage.Block{strId, storage.BlockStatusUncommitted}
		err = e

		id++
	}

	if err != nil && err != io.EOF {
		return nil, err
	}

	if nBlocks == 1 && id == 10 {
		// Create an empty one and break
		if createErr := svc.CreateBlockBlob(blob.Container, dst); createErr != nil {
			return nil, createErr
		}

		return blocks, nil
	}

	return blocks, nil
}

// Return a number n with 1 in the greatest place value, such that
// numBlocks < n < 10 * numBlocks
// This allows us to use IDs in the range [n, n+numBlocks)
// without the number of digits changing.  This is a requirement
// for azure block blob uploads
func (remote *AzureRemote) firstBlockId(f *os.File) (firstId, numBlocks int) {
	fi, _ := f.Stat()

	numBlocks = int((fi.Size() / maxBlockSize) + 1)

	s := strconv.Itoa(numBlocks * 10)

	var buf bytes.Buffer

	buf.WriteString("1")

	for i := 1; i < len(s); i++ {
		buf.WriteString("0")
	}

	id, _ := strconv.ParseInt(buf.String(), 10, 0)

	firstId = int(id)
	return
}

// get repository keys from azure
func (remote *AzureRemote) repoKeys(prefix string) (azKeys, error) {
	repoKeys := make(azKeys)

	prefix = strings.Trim(prefix, "/")

	svc, err := remote.azureBlobClient()
	if err != nil {
		return repoKeys, err
	}

	blob := remote.config.Azure.Blob

	if blob.PathPresent {
		prefix = fmt.Sprintf("%s/%s", blob.Path, prefix)
	}

	params := storage.ListBlobsParameters{Prefix: prefix}

	resp, err := svc.ListBlobs(blob.Container, params)

	if err != nil {
		return repoKeys, fmt.Errorf("getting bucket contents at prefix '%s': %s", prefix, err)
	}

	for _, b := range resp.Blobs {

		plainKey := strings.TrimPrefix(b.Name, "/")

		if strings.HasSuffix(plainKey, ".sum") {
			plainKey = strings.TrimSuffix(plainKey, ".sum")
			repoKeys.Get(plainKey, remote).sumKey = b.Name

		} else {
			repoKeys.Get(plainKey, remote).remotePath = b.Name
		}
	}

	return repoKeys, nil
}

// get files from the azure blob to a local path, relative to rootKey
// eg
//
// dst: "/tmp/rego/123"
// rootKey: "images/456"
// key: "images/456/json"
// downloads to: "/tmp/rego/123/456/json"
func (remote *AzureRemote) getFiles(dst, rootKey string, imageKeys azKeys) error {
	blob := remote.config.Azure.Blob

	if blob.PathPresent {
		dst = fmt.Sprintf("%s/%s", blob.Path, dst)
	}

	errMap := make(map[string]error)

	for _, key := range imageKeys {
		relKey := strings.TrimPrefix(key.key, rootKey)
		relKey = strings.TrimPrefix(relKey, "/")

		err := remote.getFile(filepath.Join(dst, relKey), key)
		if err != nil {
			errMap[key.key] = err
		}
	}

	if len(errMap) > 0 {
		log.Printf("Errors during getFiles: %v", errMap)
		return fmt.Errorf("error downloading files from S3")
	}

	return nil
}

func (remote *AzureRemote) getFile(dst string, key *azKeyDef) error {
	log.Printf("Pulling key %s\n", key.key)

	svc, err := remote.azureBlobClient()
	if err != nil {
		return err
	}

	rdr, err := svc.GetBlob(key.remote.config.Azure.Blob.Container, key.remotePath)
	if err != nil {
		return err
	}
	defer rdr.Close()

	if err := os.MkdirAll(filepath.Dir(dst), 0700); err != nil {
		return err
	}

	to, err := os.Create(dst)
	if err != nil {
		return err
	}

	_, err = io.Copy(to, rdr)
	if err != nil {
		return err
	}

	return nil
}

func (remote *AzureRemote) tagFilePath(repo, tag string) string {
	if remote.config.Azure.Blob.PathPresent {
		return filepath.Join(remote.config.Azure.Blob.Path, "repositories", repo, tag)
	}

	return filepath.Join("repositories", repo, tag)
}
