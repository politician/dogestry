package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
)

func NewConfig(useMetaService bool) (Config, error) {
	c := Config{}
	c.AWS.AccessKeyID = os.Getenv("AWS_ACCESS_KEY_ID")
	if c.AWS.AccessKeyID == "" {
		c.AWS.AccessKeyID = os.Getenv("AWS_ACCESS_KEY")
	}

	c.AWS.SecretAccessKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
	if c.AWS.SecretAccessKey == "" {
		c.AWS.SecretAccessKey = os.Getenv("AWS_SECRET_KEY")
	}

	c.Docker.Connection = os.Getenv("DOCKER_HOST")

	if c.Docker.Connection == "" {
		c.Docker.Connection = "unix:///var/run/docker.sock"
	}

	c.AWS.UseMetaService = useMetaService

	if !useMetaService && (c.AWS.AccessKeyID == "" || c.AWS.SecretAccessKey == "") {
		return c, errors.New("AWS_ACCESS_KEY_ID/AWS_ACCESS_KEY or AWS_SECRET_ACCESS_KEY/AWS_SECRET_KEY are missing.")
	}

	return c, nil
}

func NewAzureConfig() (Config, error) {
	c := Config{}

	c.Azure.AccountName = os.Getenv("AZ_ACCOUNT_NAME")
	c.Azure.AccountKey = os.Getenv("AZ_ACCOUNT_KEY")

	c.Docker.Connection = os.Getenv("DOCKER_HOST")

	if c.Docker.Connection == "" {
		c.Docker.Connection = "unix:///var/run/docker.sock"
	}

	if c.Azure.AccountName == "" || c.Azure.AccountKey == "" {
		return c, errors.New("AZ_ACCOUNT_NAME or AZ_ACCOUNT_KEY is missing.")
	}

	c.Azure.Active = true

	return c, nil
}

type Config struct {
	AWS struct {
		S3URL           *url.URL
		AccessKeyID     string
		SecretAccessKey string
		UseMetaService  bool
	}
	Azure struct {
		Active      bool
		AccountName string
		AccountKey  string
		Blob        *BlobSpec
	}
	Docker struct {
		Connection string
	}
}

type BlobSpec struct {
	Container   string
	Path        string
	PathPresent bool
}

func (c *Config) SetS3URL(rawurl string) error {
	urlStruct, err := url.Parse(rawurl)
	if err != nil {
		return err
	}

	c.AWS.S3URL = urlStruct

	return nil
}

func (c *Config) SetBlobSpec(s string) error {
	if s == "" {
		c.Azure.Blob = &BlobSpec{"", "", false}
		return nil
	}

	if i := strings.Index(s, "/"); i != -1 {
		z := strings.SplitN(s, "/", 2)
		c.Azure.Blob = &BlobSpec{z[0], strings.TrimSuffix(z[1], "/"), true}
		return nil
	}

	c.Azure.Blob = &BlobSpec{s, "", false}
	return nil
}

func (b *BlobSpec) String() string {
	if b.PathPresent {
		return fmt.Sprintf("%s/%s", b.Container, b.Path)
	}

	return b.Container
}
