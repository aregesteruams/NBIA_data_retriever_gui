package downloader

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

// Token is used to handle the NBIA official token request
/*
Official example be like:
curl -X -v -d "username=nbia_guest&password=&client_id=NBIA&grant_type=password" -X POST -k https://services.cancerimagingarchive.net/nbia-api/oauth/token
*/
type Token struct {
	AccessToken      string    `json:"access_token"`
	SessionState     string    `json:"session_state"`
	ExpiresIn        int       `json:"expires_in"`
	NotBeforePolicy  int       `json:"not-before-policy"`
	RefreshExpiresIn int       `json:"refresh_expires_in"`
	Scope            string    `json:"scope"`
	IdToken          string    `json:"id_token"`
	RefreshToken     string    `json:"refresh_token"`
	TokenType        string    `json:"token_type"`
	ExpiredTime      time.Time `json:"expires_time"`

	// Thread safety
	mu       sync.RWMutex
	username string
	password string
	path     string
	client   *http.Client
}

// GetAccessToken returns the access token, refreshing if necessary
func (token *Token) GetAccessToken() (string, error) {
	token.mu.RLock()
	if time.Now().Before(token.ExpiredTime) {
		accessToken := token.AccessToken
		token.mu.RUnlock()
		return accessToken, nil
	}
	token.mu.RUnlock()

	// Token expired, refresh it
	token.mu.Lock()
	defer token.mu.Unlock()

	// Double-check after acquiring write lock
	if time.Now().Before(token.ExpiredTime) {
		return token.AccessToken, nil
	}

	Logger.Infof("Token expired, refreshing...")
	newToken, err := createNewToken(token.client, token.username, token.password, token.path)
	if err != nil {
		return "", fmt.Errorf("failed to refresh token: %v", err)
	}

	// Copy new token data
	token.AccessToken = newToken.AccessToken
	token.SessionState = newToken.SessionState
	token.ExpiresIn = newToken.ExpiresIn
	token.NotBeforePolicy = newToken.NotBeforePolicy
	token.RefreshExpiresIn = newToken.RefreshExpiresIn
	token.Scope = newToken.Scope
	token.IdToken = newToken.IdToken
	token.RefreshToken = newToken.RefreshToken
	token.TokenType = newToken.TokenType
	token.ExpiredTime = newToken.ExpiredTime

	// Save updated token
	if err := token.dumpInternal(); err != nil {
		Logger.Warnf("Failed to save refreshed token: %v", err)
	}

	return token.AccessToken, nil
}

func makeURL(url_ string, values map[string]interface{}) (string, error) {
	u, err := url.Parse(url_)
	if err != nil {
		return url_, fmt.Errorf("failed to parse url: %v", err)
	}

	queries := u.Query()

	for k, v := range values {
		queries.Set(k, fmt.Sprintf("%v", v))
	}

	u.RawQuery = queries.Encode()
	return u.String(), nil
}

// NewToken create token from official NBIA API
func NewToken(client *http.Client, username, passwd, path string) (*Token, error) {
	Logger.Debugf("creating token")
	token := &Token{
		username: username,
		password: passwd,
		path:     path,
		client:   client,
	}

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		Logger.Infof("restore token from %v", path)
		err = token.Load(path)
		if err != nil {
			Logger.Error(err)
			Logger.Infof("create new token")
		} else if token.ExpiredTime.Compare(time.Now()) > 0 {
			// Token is still valid
			token.username = username
			token.password = passwd
			token.path = path
			return token, nil
		} else {
			Logger.Warn("token expired, create new token")
		}
	}

	// Create new token
	newToken, err := createNewToken(client, username, passwd, path)
	if err != nil {
		return nil, err
	}

	// Set credentials on the new token instead of copying
	newToken.username = username
	newToken.password = passwd
	newToken.path = path

	return newToken, nil
}

// createNewToken creates a new token from the API
func createNewToken(client *http.Client, username, passwd, path string) (*Token, error) {
	// Create form data
	formData := url.Values{}
	formData.Set("username", username)
	formData.Set("password", passwd)
	formData.Set("client_id", "NBIA")
	formData.Set("grant_type", "password")

	req, err := http.NewRequest("POST", TokenUrl, strings.NewReader(formData.Encode()))
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	resp, err := DoRequest(client, req)
	if err != nil {
		return nil, fmt.Errorf("failed to do request: %v", err)
	}
	defer resp.Body.Close()

	content, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response data: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("token request failed with status %d: %s", resp.StatusCode, string(content))
	}

	token := new(Token)
	err = json.Unmarshal(content, token)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal token: %v", err)
	}

	token.ExpiredTime = time.Now().Local().Add(time.Second * time.Duration(token.ExpiresIn))

	// Save token
	if path != "" {
		if err := token.Dump(path); err != nil {
			Logger.Warnf("Failed to save token: %v", err)
		}
	}

	return token, nil
}

// Dump is used to save token information (thread-safe)
func (token *Token) Dump(path string) error {
	token.mu.RLock()
	defer token.mu.RUnlock()
	return token.dumpInternal()
}

// dumpInternal saves token without locking (caller must hold lock)
func (token *Token) dumpInternal() error {
	if token.path == "" {
		return nil
	}

	Logger.Debugf("saving token to %s", token.path)

	// Create temp file first
	tempPath := token.path + ".tmp"
	f, err := os.OpenFile(tempPath, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("failed to open token json: %v", err)
	}

	// Create a copy without internal fields
	tokenCopy := struct {
		AccessToken      string    `json:"access_token"`
		SessionState     string    `json:"session_state"`
		ExpiresIn        int       `json:"expires_in"`
		NotBeforePolicy  int       `json:"not-before-policy"`
		RefreshExpiresIn int       `json:"refresh_expires_in"`
		Scope            string    `json:"scope"`
		IdToken          string    `json:"id_token"`
		RefreshToken     string    `json:"refresh_token"`
		TokenType        string    `json:"token_type"`
		ExpiredTime      time.Time `json:"expires_time"`
	}{
		AccessToken:      token.AccessToken,
		SessionState:     token.SessionState,
		ExpiresIn:        token.ExpiresIn,
		NotBeforePolicy:  token.NotBeforePolicy,
		RefreshExpiresIn: token.RefreshExpiresIn,
		Scope:            token.Scope,
		IdToken:          token.IdToken,
		RefreshToken:     token.RefreshToken,
		TokenType:        token.TokenType,
		ExpiredTime:      token.ExpiredTime,
	}

	content, err := json.MarshalIndent(tokenCopy, "", "    ")
	if err != nil {
		f.Close()
		os.Remove(tempPath)
		return fmt.Errorf("failed to marshal token: %v", err)
	}

	_, err = f.Write(content)
	if err != nil {
		f.Close()
		os.Remove(tempPath)
		return fmt.Errorf("failed to dump token: %v", err)
	}

	if err := f.Close(); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to close token file: %v", err)
	}

	// Atomic rename
	if err := os.Rename(tempPath, token.path); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to rename token file: %v", err)
	}

	return nil
}

// Load restore token from json
func (token *Token) Load(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open token json: %v", err)
	}

	content, err := io.ReadAll(f)
	if err != nil {
		return fmt.Errorf("failed to read token: %v", err)
	}
	err = json.Unmarshal(content, token)
	if err != nil {
		return fmt.Errorf("failed to unmarshal token: %v", err)
	}

	return f.Close()
}
