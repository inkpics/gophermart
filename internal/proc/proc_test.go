package proc

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
)

func TestProc_Register(t *testing.T) {
	p, err := New("localhost:8080", "host=localhost port=54320 user=postgres password=postgres dbname=postgres sslmode=disable", "")
	if err != nil {
		t.Fatalf("could not init test: %v", err)
	}

	e := echo.New()
	id := uuid.New()
	str := "{\"login\":\"" + id.String() + "\",\"password\":\"test\"}"
	request := httptest.NewRequest(http.MethodPost, "http://localhost:8080/api/user/register", strings.NewReader(str))

	recorder := httptest.NewRecorder()
	c := e.NewContext(request, recorder)
	p.Register(c)

	result := recorder.Result()
	defer result.Body.Close()

	if result.StatusCode != http.StatusOK {
		t.Errorf("expected status %v; got %v", http.StatusOK, result.StatusCode)
	}

	body, err := io.ReadAll(result.Body)
	if err != nil {
		t.Fatalf("could not read response: %v", err)
	}

	got := string(body)
	want := "user registered and authenticated successfully"
	if got != want {
		t.Fatalf("expected answer to be %v; got %v", want, got)
	}
}

func TestProc_Login(t *testing.T) {
	p, err := New("localhost:8080", "host=localhost port=54320 user=postgres password=postgres dbname=postgres sslmode=disable", "")
	if err != nil {
		t.Fatalf("could not init test: %v", err)
	}

	e := echo.New()
	str := "{\"login\":\"test\",\"password\":\"test\"}"
	request := httptest.NewRequest(http.MethodPost, "http://localhost:8080/api/user/register", strings.NewReader(str))

	recorder := httptest.NewRecorder()
	c := e.NewContext(request, recorder)
	p.Register(c)

	str = "{\"login\":\"test\",\"password\":\"test\"}"
	request = httptest.NewRequest(http.MethodPost, "http://localhost:8080/api/user/login", strings.NewReader(str))

	recorder = httptest.NewRecorder()
	c = e.NewContext(request, recorder)
	p.Login(c)

	result := recorder.Result()
	defer result.Body.Close()

	if result.StatusCode != http.StatusOK {
		t.Errorf("expected status %v; got %v", http.StatusOK, result.StatusCode)
	}

	body, err := io.ReadAll(result.Body)
	if err != nil {
		t.Errorf("could not read response: %v", err)
	}

	got := string(body)
	want := "user authenticated successfully"
	if got != want {
		t.Errorf("expected answer to be %v; got %v", want, got)
	}

	str = "{\"login\":\"test\",\"password\":\"wrong\"}"
	request = httptest.NewRequest(http.MethodPost, "http://localhost:8080/api/user/login", strings.NewReader(str))

	recorder = httptest.NewRecorder()
	c = e.NewContext(request, recorder)
	p.Login(c)

	result = recorder.Result()
	defer result.Body.Close()

	if result.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected status %v; got %v", http.StatusUnauthorized, result.StatusCode)
	}

	body, err = io.ReadAll(result.Body)
	if err != nil {
		t.Errorf("could not read response: %v", err)
	}

	got = string(body)
	want = "wrong credentials"
	if got != want {
		t.Errorf("expected answer to be %v; got %v", want, got)
	}
}

func Test_validateLuhn(t *testing.T) {
	tests := []struct {
		number int
		want   bool
	}{
		{number: 125764357, want: true},
		{number: 5347754565, want: true},
		{number: 87643, want: true},
		{number: 45678976, want: true},
		{number: 6432964973280, want: false},
		{number: 1791238908321, want: false},
	}
	for _, tt := range tests {
		if got := validateLuhn(tt.number); got != tt.want {
			t.Errorf("validateLuhn() = %v, want %v", got, tt.want)
		}
	}
}

func Test_checksumLuhn(t *testing.T) {
	tests := []struct {
		number int
		want   int
	}{
		{number: 6478234230, want: 7},
		{number: 8907654355, want: 5},
		{number: 1209374734, want: 2},
		{number: 7453126346, want: 0},
		{number: 9740174550, want: 4},
	}
	for _, tt := range tests {
		if got := checksumLuhn(tt.number); got != tt.want {
			t.Errorf("checksumLuhn() = %v, want %v", got, tt.want)
		}
	}
}
