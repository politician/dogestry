package remote

import (
	"io/ioutil"
	"os"
	"path/filepath"

	"code.google.com/p/go-uuid/uuid"
	"github.com/MSOpenTech/azure-sdk-for-go/storage"
	"github.com/dogestry/dogestry/config"
	. "gopkg.in/check.v1"
)

type SAz struct {
	remote  *AzureRemote
	TempDir string
}

var _ = Suite(&SAz{})

func (s *SAz) SetUpSuite(c *C) {
	tempDir, err := ioutil.TempDir("", "dogestry-test")
	if err != nil {
		c.Skip("Azure enviornment variables not set - skipping azure tests")
	}

	s.TempDir = tempDir

	cfg, err := config.NewAzureConfig()
	if err != nil {
		c.Fatalf("couldn't initialize config. Error: %s", err)
	}

	s.remote = &AzureRemote{
		config: cfg,
	}
}

func (s *SAz) TearDownSuite(c *C) {
	defer os.RemoveAll(s.TempDir)
}

func (s *SAz) TestService(c *C) {
	_, err := s.remote.azureBlobClient()
	c.Assert(err, IsNil)
}

func (s *SAz) TestRepoKeys(c *C) {
	svc, err := s.remote.azureBlobClient()

	container := uuid.New()
	s.remote.config.SetBlobSpec(container)
	c.Log(container)

	// Set up a container with some files
	err = svc.CreateContainer(container, storage.ContainerAccessTypePrivate)
	if err != nil {
		c.Fatalf("couldn't create test container.  Error: %s", err)
	}
	defer svc.DeleteContainer(container)
	svc.PutBlock(container, "Nelson", "AAAA", []byte{54, 55, 56})
	svc.PutBlockList(container, "Nelson", []storage.Block{storage.Block{"AAAA", storage.BlockStatusUncommitted}})
	svc.PutBlock(container, "Neo", "AAAB", []byte{57, 58, 59})
	svc.PutBlockList(container, "Neo", []storage.Block{storage.Block{"AAAB", storage.BlockStatusUncommitted}})

	keys, err := s.remote.repoKeys("")
	c.Assert(err, IsNil)

	c.Log(keys["Nelson"])

	c.Assert(keys["Nelson"].key, Equals, "Nelson")
	c.Assert(keys["Neo"].key, Equals, "Neo")
}

func (s *SAz) TestLocalKeys(c *C) {
	dumpFile(s.TempDir, "file1", "hello world")
	dumpFile(s.TempDir, "dir/file2", "hello mars")

	keys, err := s.remote.localKeys(s.TempDir)
	c.Assert(err, IsNil)

	c.Assert(keys["file1"].key, Equals, "file1")
	c.Assert(keys["file1"].fullPath, Equals, filepath.Join(s.TempDir, "file1"))
	c.Assert(keys["file1"].sum, Equals, "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed")

	c.Assert(keys["dir/file2"].key, Equals, "dir/file2")
	c.Assert(keys["dir/file2"].fullPath, Equals, filepath.Join(s.TempDir, "dir/file2"))
	c.Assert(keys["dir/file2"].sum, Equals, "dd6944c43fabd03cf643fe0daf625759dbdea808")
}

func (s *SAz) TestResolveImageNameToId(c *C) {
	rubyId := "123"

	svc, err := s.remote.azureBlobClient()

	container := uuid.New()
	s.remote.config.SetBlobSpec(container)
	c.Log(container)

	// Set up a container with some files
	err = svc.CreateContainer(container, storage.ContainerAccessTypePrivate)
	if err != nil {
		c.Fatalf("couldn't create test container.  Error: %s", err)
	}
	defer svc.DeleteContainer(container)
	err = svc.PutBlock(container, "repositories/ruby/latest", "AAAA", []byte{49, 50, 51})
	if err != nil {
		c.Fatalf("couldn't send block.  Error: %s", err)
	}
	err = svc.PutBlockList(container, "repositories/ruby/latest", []storage.Block{storage.Block{"AAAA", storage.BlockStatusUncommitted}})
	if err != nil {
		c.Fatalf("couldn't commit block.  Error: %s", err)
	}

	r, err := svc.GetBlob(container, "repositories/ruby/latest")
	azId := make([]byte, 3)
	r.Read(azId)

	c.Log(azId)

	id, err := s.remote.ResolveImageNameToId("ruby")
	c.Assert(err, IsNil)

	c.Assert(string(id), Equals, rubyId)

	id, err = s.remote.ResolveImageNameToId("rubyx")
	c.Assert(err, Not(IsNil))
}
