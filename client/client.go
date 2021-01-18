package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/sirupsen/logrus"
	"io"
	"io/ioutil"
	"net/http"
	"stpCommon/model"
	"time"
)

type StpManagerClient struct {
	managerHost string
	managerPort string
	httpClient  *http.Client
}

func NewStpManagerClient(managerHost string, managerPort string) *StpManagerClient {
	return &StpManagerClient{
		managerHost: managerHost,
		managerPort: managerPort,
		httpClient:  &http.Client{Timeout: time.Second * 5},
	}
}

func (c *StpManagerClient) httpRequest(method, uri string, payload io.Reader) ([]byte, error) {

	url := fmt.Sprintf("http://%s:%s/%s", c.managerHost, c.managerPort, uri)
	logrus.Debug("request url is: ", url)
	req, err := http.NewRequest(method, url, payload)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	if resp != nil {
		defer resp.Body.Close()
	}
	return ioutil.ReadAll(resp.Body)
}

func (c *StpManagerClient) GetExport() (*model.Export, error) {
	resp, err := c.httpRequest("GET", "get-export", nil)
	if err != nil {
		logrus.Error("error getting export: ", err.Error())
		return nil, err
	}
	exp := &model.Export{}
	if err := json.Unmarshal(resp, exp); err != nil {
		logrus.Error("error unmarshal json: ", err.Error())
		return nil, err
	}
	return exp, nil
}

func (c *StpManagerClient) SetExportStatus(status *model.ExportStatus) error {
	statusBytes, err := json.Marshal(status)
	if err != nil {
		return nil
	}
	payload := bytes.NewBuffer(statusBytes)
	_, err = c.httpRequest("POST", "set-status", payload)
	return err
}
