package cli

import (
	"errors"
	"fmt"
)

const PullHelpMessage string = `  Pull IMAGE from REMOTE and load it into docker.

  Arguments:
    REMOTE       Name of REMOTE.
    IMAGE[:TAG]  Name of IMAGE. TAG is optional, and defaults to 'latest'.

  Examples:
    dogestry -pullhosts tcp://host-1:2375 pull s3://DockerBucket/Path/ ubuntu:14.04
    dogestry pull /path/to/images ubuntu`

func (cli *DogestryCli) CmdPull(args ...string) error {
	pullFlags := cli.Subcmd("pull", "REMOTE IMAGE[:TAG]", PullHelpMessage)

	// Don't return error here, this part is only relevant for CLI
	if err := pullFlags.Parse(args); err != nil {
		return nil
	}

	if len(pullFlags.Args()) < 2 {
		return errors.New("Error: REMOTE and IMAGE not specified")
	}

	r, err := cli.GetRemote(pullFlags.Arg(0))
	if err != nil {
		return err
	}

	image := pullFlags.Arg(1)
	imageRoot, err := cli.WorkDir(image)
	if err != nil {
		return err
	}

	fmt.Printf("Using docker endpoints for pull: %v\n", cli.PullHosts)
	fmt.Printf("Remote Connection: %v\n", r.Desc())

	fmt.Printf("Image tag: %v\n", image)

	id, err := r.ResolveImageNameToId(image)
	if err != nil {
		return err
	}

	fmt.Printf("Image '%s' resolved to ID '%s'\n", image, id.Short())

	fmt.Println("Determining which images need to be downloaded from remote...")
	downloadMap, err := cli.makeDownloadMap(r, id, imageRoot)
	if err != nil {
		return err
	}

	fmt.Println("Downloading images from remote...")
	if err := cli.downloadImages(r, downloadMap, imageRoot); err != nil {
		return err
	}

	fmt.Println("Generating repositories JSON file...")
	if err := cli.createRepositoriesJsonFile(image, imageRoot, r); err != nil {
		return err
	}

	fmt.Printf("Importing image(%s) TAR file to docker hosts: %v\n", id.Short(), cli.PullHosts)
	if err := cli.sendTar(imageRoot); err != nil {
		return err
	}

	return nil
}
